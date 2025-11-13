import hashlib
import hmac
import json
import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from google.auth.transport.requests import Request as GoogleRequest
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from itsdangerous import BadSignature, URLSafeSerializer
from sqlalchemy.orm import Session

from app.db import SessionLocal, get_db
from app.models import (
    ContentIndex,
    DriveSession,
    IngestionJob,
    SourceState,
    User,
    UserSession,
)
from .google_clients import build_flow
from app.rag import vector

from cryptography.fernet import Fernet, InvalidToken

router = APIRouter(prefix="/auth", tags=["auth"])

APP_ENV = os.getenv("APP_ENV", "development").lower()

_raw_session_secret = os.getenv("SESSION_SECRET")
if not _raw_session_secret or len(_raw_session_secret) < 32:
    if APP_ENV != "development":
        raise RuntimeError("SESSION_SECRET must be at least 32 chars outside development.")
    _raw_session_secret = _raw_session_secret or "dev-secret"
SESSION_SECRET = _raw_session_secret

SESSION_COOKIE_NAME = os.getenv("SESSION_COOKIE_NAME", "lc_session")
SESSION_TTL_DAYS = int(os.getenv("SESSION_TTL_DAYS", "30"))
SESSION_COOKIE_SECURE = os.getenv(
    "SESSION_COOKIE_SECURE",
    "0" if APP_ENV == "development" else "1",
) == "1"
SESSION_COOKIE_SAMESITE = os.getenv(
    "SESSION_COOKIE_SAMESITE",
    "lax" if APP_ENV == "development" else "strict",
)
CSRF_COOKIE_NAME = os.getenv("CSRF_COOKIE_NAME", "lc_csrf")
CSRF_HEADER_NAME = os.getenv("CSRF_HEADER_NAME", "X-CSRF-Token")
STATE_SIGNER = URLSafeSerializer(SESSION_SECRET, salt="oauth-state")

DRIVE_CREDENTIALS_KEY = os.getenv("DRIVE_CREDENTIALS_KEY")
_fernet: Optional[Fernet] = None
if DRIVE_CREDENTIALS_KEY:
    try:
        _fernet = Fernet(DRIVE_CREDENTIALS_KEY)
    except Exception as exc:
        raise RuntimeError("DRIVE_CREDENTIALS_KEY must be a valid Fernet key.") from exc
elif APP_ENV != "development":
    raise RuntimeError("DRIVE_CREDENTIALS_KEY is required outside development.")

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_aware(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _random_token() -> str:
    return secrets.token_urlsafe(48)


def _serialize_credentials(creds: Credentials) -> Dict[str, Any]:
    payload = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": list(creds.scopes or []),
    }
    if not _fernet:
        return payload
    blob = json.dumps(payload).encode("utf-8")
    ciphertext = _fernet.encrypt(blob).decode("utf-8")
    return {"ciphertext": ciphertext}


def _deserialize_credentials(data: Dict[str, Any]) -> Dict[str, Any]:
    if "ciphertext" not in data:
        return data
    if not _fernet:
        raise RuntimeError("Encrypted Google credentials present but DRIVE_CREDENTIALS_KEY is not configured.")
    try:
        decrypted = _fernet.decrypt(data["ciphertext"].encode("utf-8"))
    except InvalidToken as exc:
        raise RuntimeError("Stored Google credentials could not be decrypted; reconnect your Google account.") from exc
    return json.loads(decrypted.decode("utf-8"))


def _set_session_cookie(response: RedirectResponse, token: str) -> None:
    max_age = SESSION_TTL_DAYS * 24 * 60 * 60
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=token,
        max_age=max_age,
        httponly=True,
        secure=SESSION_COOKIE_SECURE,
        samesite=SESSION_COOKIE_SAMESITE,
    )


def _new_csrf_token() -> str:
    return secrets.token_urlsafe(32)


def _set_csrf_cookie(response: Response, token: str) -> None:
    max_age = SESSION_TTL_DAYS * 24 * 60 * 60
    response.set_cookie(
        key=CSRF_COOKIE_NAME,
        value=token,
        max_age=max_age,
        httponly=False,
        secure=SESSION_COOKIE_SECURE,
        samesite=SESSION_COOKIE_SAMESITE,
    )


def ensure_csrf_cookie(request: Request, response: Response) -> str:
    token = request.cookies.get(CSRF_COOKIE_NAME)
    if token:
        return token
    token = _new_csrf_token()
    _set_csrf_cookie(response, token)
    return token


def _fetch_profile(creds: Credentials) -> Dict[str, Any]:
    svc = build("oauth2", "v2", credentials=creds)
    return svc.userinfo().get().execute()


def _upsert_user(db: Session, profile: Dict[str, Any]) -> User:
    sub = profile.get("id") or profile.get("sub")
    email = profile.get("email")
    if not sub or not email:
        raise HTTPException(status_code=400, detail="Google profile missing id/email")

    user = db.query(User).filter(User.google_sub == sub).one_or_none()
    now = _utcnow()
    if user is None:
        user = User(
            google_sub=sub,
            email=email,
            full_name=profile.get("name"),
            picture=profile.get("picture"),
            created_at=now,
            updated_at=now,
        )
        db.add(user)
    else:
        user.email = email
        user.full_name = profile.get("name") or user.full_name
        user.picture = profile.get("picture") or user.picture
        user.updated_at = now
    db.commit()
    db.refresh(user)
    return user


def _persist_google_credentials(db: Session, user_id: str, creds: Credentials) -> None:
    record = db.get(DriveSession, user_id)
    if record is None:
        record = DriveSession(user_id=user_id)
        db.add(record)
    record.credentials = _serialize_credentials(creds)
    record.updated_at = _utcnow()
    if not record.created_at:
        record.created_at = _utcnow()
    db.commit()


def _issue_session(db: Session, user: User) -> str:
    raw = _random_token()
    token_hash = _hash_token(raw)
    expires_at = _utcnow() + timedelta(days=SESSION_TTL_DAYS)
    session_row = UserSession(user_id=user.id, token_hash=token_hash, expires_at=expires_at)
    db.add(session_row)
    db.commit()
    return raw


def _extract_session_token(request: Request) -> Optional[str]:
    cookie = request.cookies.get(SESSION_COOKIE_NAME)
    authz = request.headers.get("Authorization") or ""
    header_token = None
    if authz.lower().startswith("bearer "):
        header_token = authz.split(" ", 1)[1].strip()
    return cookie or header_token


def csrf_protect(request: Request) -> None:
    """
    Double-submit protection: compare readable CSRF cookie to supplied header.
    Only enforced when the session is supplied via cookie (Bearer flows are exempt).
    """
    if SESSION_COOKIE_NAME not in request.cookies:
        return
    cookie_token = request.cookies.get(CSRF_COOKIE_NAME)
    header_token = (
        request.headers.get(CSRF_HEADER_NAME)
        or request.headers.get("X-CSRFToken")
        or request.headers.get("X-Csrf-Token")
    )
    if not cookie_token or not header_token:
        raise HTTPException(status_code=403, detail="Missing CSRF token")
    if not hmac.compare_digest(cookie_token, header_token):
        raise HTTPException(status_code=403, detail="Invalid CSRF token")


def _load_session(db: Session, token: str) -> UserSession:
    token_hash = _hash_token(token)
    session = db.query(UserSession).filter(UserSession.token_hash == token_hash).one_or_none()
    if not session:
        raise HTTPException(status_code=401, detail="Invalid session token")
    expires_at = _ensure_aware(session.expires_at)
    if expires_at and expires_at < _utcnow():
        raise HTTPException(status_code=401, detail="Session expired")
    session.last_used_at = _utcnow()
    db.commit()
    return session


def get_current_user(request: Request, db: Session = Depends(get_db)) -> User:
    token = _extract_session_token(request)
    if not token:
        raise HTTPException(status_code=401, detail="Missing session token")
    session = _load_session(db, token)
    user = db.get(User, session.user_id)
    if not user:
        raise HTTPException(status_code=401, detail="User for session not found")
    return user


class _MissingCredentials(RuntimeError):
    pass


def _delete_user_data(db: Session, user_id: str) -> Dict[str, int]:
    summary = {
        "content_index": 0,
        "source_state": 0,
        "drive_sessions": 0,
        "ingestion_jobs": 0,
        "user_sessions": 0,
    }
    summary["content_index"] = (
        db.query(ContentIndex).filter(ContentIndex.user_id == user_id).delete(synchronize_session=False)
    )
    summary["source_state"] = (
        db.query(SourceState).filter(SourceState.user_id == user_id).delete(synchronize_session=False)
    )
    summary["drive_sessions"] = (
        db.query(DriveSession).filter(DriveSession.user_id == user_id).delete(synchronize_session=False)
    )
    summary["ingestion_jobs"] = (
        db.query(IngestionJob).filter(IngestionJob.user_id == user_id).delete(synchronize_session=False)
    )
    summary["user_sessions"] = (
        db.query(UserSession).filter(UserSession.user_id == user_id).delete(synchronize_session=False)
    )
    db.commit()
    try:
        vector.reset_collection(user_id=user_id)
    except Exception:
        pass
    return summary


def _clear_session_state(response: Response) -> None:
    response.delete_cookie(
        key=SESSION_COOKIE_NAME,
        httponly=True,
        secure=SESSION_COOKIE_SECURE,
        samesite=SESSION_COOKIE_SAMESITE,
    )
    response.delete_cookie(
        key=CSRF_COOKIE_NAME,
        httponly=False,
        secure=SESSION_COOKIE_SECURE,
        samesite=SESSION_COOKIE_SAMESITE,
    )


def _build_credentials(data: Dict[str, Any]) -> Credentials:
    decoded = _deserialize_credentials(data or {})
    return Credentials(
        token=decoded.get("token"),
        refresh_token=decoded.get("refresh_token"),
        token_uri=decoded.get("token_uri"),
        client_id=decoded.get("client_id"),
        client_secret=decoded.get("client_secret"),
        scopes=decoded.get("scopes"),
    )


def _load_credentials_row(db: Session, user_id: str) -> DriveSession:
    record = db.get(DriveSession, user_id)
    if not record or not record.credentials:
        raise _MissingCredentials(f"Google account not connected for user {user_id}")
    return record


def _refresh_if_needed(db: Session, record: DriveSession, creds: Credentials) -> None:
    if creds.expired and creds.refresh_token:
        creds.refresh(GoogleRequest())
        record.credentials = _serialize_credentials(creds)
        record.updated_at = _utcnow()
        db.commit()


def get_google_credentials_for_user(db: Session, user_id: str) -> Credentials:
    try:
        record = _load_credentials_row(db, user_id)
    except _MissingCredentials as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    creds = _build_credentials(record.credentials or {})
    try:
        _refresh_if_needed(db, record, creds)
    except Exception as exc:
        raise HTTPException(status_code=401, detail=f"Failed to refresh Google credentials: {exc}") from exc
    return creds


def get_google_credentials_for_user_unmanaged(user_id: str) -> Credentials:
    db = SessionLocal()
    try:
        record = _load_credentials_row(db, user_id)
        creds = _build_credentials(record.credentials or {})
        _refresh_if_needed(db, record, creds)
        return creds
    finally:
        db.close()


@router.get("/google")
def start_google_auth():
    flow = build_flow()
    flow.redirect_uri = os.getenv("OAUTH_REDIRECT_URI")
    state_payload = {"nonce": secrets.token_urlsafe(16)}
    state = STATE_SIGNER.dumps(state_payload)
    auth_url, _ = flow.authorization_url(
        access_type="offline",
        prompt="consent",
        state=state,
        include_granted_scopes="true",
    )
    return {"authorization_url": auth_url}


@router.get("/google/callback")
def google_callback(code: str, state: str, db: Session = Depends(get_db)):
    try:
        STATE_SIGNER.loads(state)
    except BadSignature as exc:
        raise HTTPException(status_code=400, detail="Invalid OAuth state") from exc

    flow = build_flow()
    flow.redirect_uri = os.getenv("OAUTH_REDIRECT_URI")
    flow.fetch_token(code=code)
    creds = flow.credentials
    profile = _fetch_profile(creds)
    user = _upsert_user(db, profile)
    _persist_google_credentials(db, user.id, creds)
    session_token = _issue_session(db, user)

    response = RedirectResponse(url=f"/auth/me", status_code=303)
    _set_session_cookie(response, session_token)
    _set_csrf_cookie(response, _new_csrf_token())
    return response


@router.get("/me")
def me(
    request: Request,
    response: Response,
    user: User = Depends(get_current_user),
):
    csrf_token = ensure_csrf_cookie(request, response)
    return {
        "user": {
            "id": user.id,
            "email": user.email,
            "full_name": user.full_name,
            "picture": user.picture,
        },
        "csrf_token": csrf_token,
    }


@router.get("/csrf")
def csrf_token(
    request: Request,
    response: Response,
    user: User = Depends(get_current_user),
):
    token = ensure_csrf_cookie(request, response)
    return {"csrf_token": token}


@router.post("/disconnect")
def disconnect(
    response: Response,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    _csrf=Depends(csrf_protect),
):
    summary = _delete_user_data(db, user.id)
    _clear_session_state(response)
    return {"status": "ok", "deleted": summary}
