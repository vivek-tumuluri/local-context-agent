import io
import os
import random
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from fastapi import APIRouter, Depends, Query
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from sqlalchemy.orm import Session

from app.db import SessionLocal, get_db
from app.models import DriveSession
from ..auth import creds_from_session
from ..rag.chunk import chunk_text
from ..rag.vector import upsert as upsert_chunks
from .parser import to_text

router = APIRouter(prefix="/ingest/drive", tags=["ingest"])


EXPORT_MIME = {
    "application/vnd.google-apps.document": "text/plain",
    "application/vnd.google-apps.spreadsheet": "text/csv",
    "application/vnd.google-apps.presentation": "text/plain",
}

DEFAULT_JOB_MAX = int(os.getenv("INGEST_DRIVE_DEFAULT_MAX", "50"))
MAX_PAGE_SIZE = int(os.getenv("INGEST_DRIVE_PAGE_SIZE", "200"))
MAX_LIST_RETRIES = int(os.getenv("INGEST_DRIVE_LIST_RETRIES", "4"))
LIST_BACKOFF_BASE = float(os.getenv("INGEST_DRIVE_BACKOFF_BASE", "0.8"))

_SESSION_CACHE: Dict[str, str] = {}
_SESSION_LOCK = threading.Lock()


def fake_user():
    class U:
        user_id = "demo_user"
    return U()


def _persist_session_token(db: Session, user_id: str, session_token: str) -> None:
    with _SESSION_LOCK:
        _SESSION_CACHE[user_id] = session_token

    row = db.get(DriveSession, user_id)
    if row:
        row.session_token = session_token
    else:
        row = DriveSession(user_id=user_id, session_token=session_token)
        db.add(row)
    db.commit()


def _persist_session_token_unmanaged(user_id: str, session_token: str) -> None:
    """
    Persist a token using a short-lived DB session (used when we only have the
    SessionLocal factory, e.g., inside background jobs).
    """
    if SessionLocal is None:
        return
    db = SessionLocal()
    try:
        _persist_session_token(db, user_id, session_token)
    finally:
        db.close()


def _session_for_user(user_id: str) -> str:
    with _SESSION_LOCK:
        cached = _SESSION_CACHE.get(user_id)
    if cached:
        return cached

    db = SessionLocal()
    try:
        row = db.get(DriveSession, user_id)
        if row:
            with _SESSION_LOCK:
                _SESSION_CACHE[user_id] = row.session_token
            return row.session_token
    finally:
        db.close()

    env_key = f"DRIVE_SESSION_{user_id.upper()}"
    token = os.getenv(env_key) or os.getenv("DRIVE_SESSION_TOKEN")
    if token:
        _persist_session_token_unmanaged(user_id, token)
        return token
    raise RuntimeError(
        f"No Drive session token available for user '{user_id}'. "
        "Register one via /ingest/drive or set DRIVE_SESSION_TOKEN."
    )


def _drive_service(session_token: str):
    creds = creds_from_session(session_token)
    if getattr(creds, "expired", False) and getattr(creds, "refresh_token", None):
        try:
            creds.refresh(Request())
        except Exception as exc:  # pragma: no cover - network dependent
            raise RuntimeError(f"Unable to refresh Drive credentials: {exc}") from exc
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _ingest_with_service(
    svc,
    *,
    user_id: str,
    name_filter: Optional[str],
    max_files: Optional[int],
    reembed_all: bool,
    on_progress: Optional[Callable[[int, int, str], None]],
) -> Dict[str, int]:
    limit = max_files or DEFAULT_JOB_MAX
    limit = max(1, limit)
    q = "trashed=false"
    if name_filter:
        q += f" and name contains '{name_filter}'"

    files = _list_drive_files(svc, q, limit)
    total = len(files)

    if on_progress:
        on_progress(0, total, "starting drive ingest")

    ingested = 0
    errors = 0
    for idx, f in enumerate(files, start=1):
        fid = f.get("id")
        name = f.get("name", "(untitled)")
        mime = f.get("mimeType", "")
        try:
            data = _download(svc, fid, mime)
            text = to_text(data, filename=name, mime=mime)
            if not text.strip():
                if on_progress:
                    on_progress(idx, total, f"skipped {name}: empty content")
                continue

            meta = {"source": "drive", "title": name, "id": fid, "mime": mime, "user_id": user_id}
            chunks = chunk_text(text, meta=meta)
            upsert_chunks(chunks, user_id=user_id)
            ingested += 1
            if on_progress:
                on_progress(idx, total, f"ingested {name}")
        except Exception as exc:
            errors += 1
            if on_progress:
                on_progress(idx, total, f"error {name}: {exc}")
            continue

    return {"found": total, "ingested": ingested, "errors": errors}


@router.post("")
def ingest_drive_endpoint(
    session: str,
    limit: int = Query(20, ge=1, le=500),
    name_contains: str | None = Query(None),
    user=Depends(fake_user),
    db: Session = Depends(get_db),
):
    _persist_session_token(db, user.user_id, session)
    svc = _drive_service(session)
    stats = _ingest_with_service(
        svc,
        user_id=user.user_id,
        name_filter=name_contains,
        max_files=limit,
        reembed_all=False,
        on_progress=None,
    )
    return stats

def _download(svc, file_id: str, mime: str | None):
    buf = io.BytesIO()
    if mime in EXPORT_MIME:
        req = svc.files().export_media(fileId=file_id, mimeType=EXPORT_MIME[mime])
    else:
        req = svc.files().get_media(fileId=file_id)
    downloader = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue()


def ingest_drive(
    user_id: str,
    name_filter: Optional[str] = None,
    max_files: Optional[int] = None,
    reembed_all: bool = False,
    on_progress: Optional[Callable[[int, int, str], None]] = None,
) -> Dict[str, int]:
    """
    Callable entry point for the background job system.
    """
    session_token = _session_for_user(user_id)
    svc = _drive_service(session_token)
    return _ingest_with_service(
        svc,
        user_id=user_id,
        name_filter=name_filter,
        max_files=max_files,
        reembed_all=reembed_all,
        on_progress=on_progress,
    )


def ensure_drive_session(user_id: str) -> None:
    """
    Raise a RuntimeError if we cannot locate a session token for this user.
    Useful for validating before starting jobs.
    """
    _session_for_user(user_id)


def _list_drive_files(svc, query: str, limit: int) -> List[Dict[str, Any]]:
    files: List[Dict[str, Any]] = []
    page_token: Optional[str] = None
    retries = 0

    while len(files) < limit:
        page_size = min(MAX_PAGE_SIZE, max(1, limit - len(files)))
        try:
            resp = (
                svc.files()
                .list(
                    q=query,
                    pageToken=page_token,
                    pageSize=page_size,
                    fields="nextPageToken, files(id,name,mimeType)",
                )
                .execute()
            )
        except HttpError as err:
            if _should_retry(err, retries):
                _sleep_with_backoff(err, retries)
                retries += 1
                continue
            raise

        retries = 0
        new_files = resp.get("files", []) or []
        files.extend(new_files)
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    return files[:limit]


def _should_retry(err: HttpError, attempt: int) -> bool:
    if attempt >= MAX_LIST_RETRIES:
        return False
    status = getattr(getattr(err, "resp", None), "status", None)
    try:
        code = int(status)
    except (TypeError, ValueError):
        code = None
    return code in {429, 500, 502, 503, 504}


def _sleep_with_backoff(err: HttpError, attempt: int) -> None:
    retry_after = None
    resp = getattr(err, "resp", None)
    if resp:
        retry_after = resp.get("retry-after") if hasattr(resp, "get") else None
        if retry_after is None:
            retry_after = getattr(resp, "retry_after", None)
    try:
        retry_after = float(retry_after) if retry_after is not None else None
    except (TypeError, ValueError):
        retry_after = None

    if retry_after is not None and retry_after > 0:
        delay = retry_after
    else:
        delay = LIST_BACKOFF_BASE * (2 ** attempt)
        delay += random.random() * 0.3
    time.sleep(delay)
