import hashlib
import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from google.auth.transport.requests import Request as GoogleRequest
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from itsdangerous import BadSignature, URLSafeSerializer
from sqlalchemy.orm import Session

from app.db import SessionLocal, get_db
from app.models import DriveSession, User, UserSession
from .google_clients import build_flow

router = APIRouter(prefix="/auth", tags=["auth"])

SESSION_SECRET = os.getenv("SESSION_SECRET", "dev-secret")
SESSION_COOKIE_NAME = os.getenv("SESSION_COOKIE_NAME", "lc_session")
SESSION_TTL_DAYS = int(os.getenv("SESSION_TTL_DAYS", "30"))
SESSION_COOKIE_SECURE = os.getenv("SESSION_COOKIE_SECURE", "0") == "1"
STATE_SIGNER = URLSafeSerializer(SESSION_SECRET, salt="oauth-state")


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
    return {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": list(creds.scopes or []),
    }


def _set_session_cookie(response: RedirectResponse, token: str) -> None:
    max_age = SESSION_TTL_DAYS * 24 * 60 * 60
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=token,
        max_age=max_age,
        httponly=True,
        secure=SESSION_COOKIE_SECURE,
        samesite="lax",
    )


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
    alt_header = request.headers.get("X-Session")
    param_token = request.query_params.get("session")

    return cookie or header_token or alt_header or param_token


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


def _build_credentials(data: Dict[str, Any]) -> Credentials:
    return Credentials(
        token=data.get("token"),
        refresh_token=data.get("refresh_token"),
        token_uri=data.get("token_uri"),
        client_id=data.get("client_id"),
        client_secret=data.get("client_secret"),
        scopes=data.get("scopes"),
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

    response = RedirectResponse(url=f"/auth/debug/authed?token={session_token}", status_code=303)
    _set_session_cookie(response, session_token)
    return response


@router.get("/me")
def me(user: User = Depends(get_current_user)):
    return {
        "user": {
            "id": user.id,
            "email": user.email,
            "full_name": user.full_name,
            "picture": user.picture,
        }
    }


@router.get("/debug/authed", response_class=HTMLResponse)
def authed(token: Optional[str] = None):
    body_token = token or "(check your cookies)"
    return f"""
    <html>
      <body style=\"font-family: ui-monospace, Menlo, monospace; padding: 24px;\">
        <h2>Authenticated ✔</h2>
        <p>Your session token is stored as an HttpOnly cookie named <code>{SESSION_COOKIE_NAME}</code>.</p>
        <p>For API testing you can also copy the token below and send it via <code>Authorization: Bearer &lt;token&gt;</code>.</p>
        <pre style=\"white-space: pre-wrap; word-break: break-all; background:#f5f5f5; padding:12px; border-radius:8px;\">
{body_token}
        </pre>
      </body>
    </html>
    """
