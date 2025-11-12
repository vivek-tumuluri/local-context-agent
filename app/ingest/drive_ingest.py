import io
import os
import random
import time
from typing import Any, Callable, Dict, List, Optional

from fastapi import APIRouter, Depends, Query
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from sqlalchemy.orm import Session

from app.db import get_db, SessionLocal
from ..auth import (
    get_current_user,
    get_google_credentials_for_user,
    get_google_credentials_for_user_unmanaged,
)
from .drive_pipeline import (
    run_drive_ingest_once,
    load_drive_cursor,
    save_drive_cursor,
)
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


def _drive_service(creds):
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _list_page_factory(svc, name_filter: Optional[str]):
    def _list_page(user_id: str, page_token: Optional[str], page_size: int) -> Dict[str, Any]:
        q = "trashed=false"
        if name_filter:
            q += f" and name contains '{name_filter}'"
        req = (
            svc.files()
            .list(
                q=q,
                pageToken=page_token,
                pageSize=page_size,
                fields=(
                    "nextPageToken, files(id,name,mimeType,md5Checksum,size,"
                    "modifiedTime,trashed,version)"
                ),
            )
        )
        return req.execute()

    return _list_page


def _fetch_file_factory(svc):
    def _fetch_file(user_id: str, file_id: str, mime_type: Optional[str]) -> bytes:
        return _download(svc, file_id, mime_type)

    return _fetch_file


def _parse_bytes(content: bytes, mime: Optional[str]) -> str:
    return to_text(content, filename="", mime=mime)


@router.post("")
def ingest_drive_endpoint(
    limit: int = Query(20, ge=1, le=500),
    name_contains: str | None = Query(None),
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    creds = get_google_credentials_for_user(db, user.user_id)
    svc = _drive_service(creds)
    list_page = _list_page_factory(svc, name_contains)
    fetch_file = _fetch_file_factory(svc)

    processed = embedded = errors = 0
    use_cursor = name_contains is None
    next_page: Optional[str] = load_drive_cursor(db, user.user_id) if use_cursor else None
    remaining = limit

    while remaining > 0:
        page_size = min(MAX_PAGE_SIZE, remaining)
        summary = run_drive_ingest_once(
            db=db,
            user_id=user.user_id,
            list_page=list_page,
            fetch_file_bytes=fetch_file,
            parse_bytes=_parse_bytes,
            job=None,
            page_token=next_page,
            page_size=page_size,
        )
        processed += summary.get("processed", 0)
        embedded += summary.get("embedded", 0)
        errors += summary.get("errors", 0)
        remaining -= summary.get("processed", 0)
        next_page = summary.get("nextPageToken")
        if use_cursor and not summary.get("listing_failed"):
            save_drive_cursor(db, user.user_id, next_page)
        if not next_page or summary.get("processed", 0) == 0:
            break

    return {"found": processed, "ingested": embedded, "errors": errors}

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
    limit = max_files or DEFAULT_JOB_MAX
    limit = max(1, limit)

    creds = get_google_credentials_for_user_unmanaged(user_id)
    svc = _drive_service(creds)
    list_page = _list_page_factory(svc, name_filter)
    fetch_file = _fetch_file_factory(svc)

    processed = embedded = errors = 0
    use_cursor = not reembed_all and name_filter is None
    page_token: Optional[str] = None
    remaining = limit

    db = SessionLocal()
    try:
        if use_cursor:
            page_token = load_drive_cursor(db, user_id)
        while remaining > 0:
            page_size = min(MAX_PAGE_SIZE, remaining)
            summary = run_drive_ingest_once(
                db=db,
                user_id=user_id,
                list_page=list_page,
                fetch_file_bytes=fetch_file,
                parse_bytes=_parse_bytes,
                job=None,
                page_token=page_token,
                page_size=page_size,
                force_reembed=reembed_all,
            )
            processed += summary.get("processed", 0)
            embedded += summary.get("embedded", 0)
            errors += summary.get("errors", 0)
            remaining -= summary.get("processed", 0)
            page_token = summary.get("nextPageToken")
            if use_cursor and not summary.get("listing_failed"):
                save_drive_cursor(db, user_id, page_token)

            if on_progress:
                total_hint = processed + max(remaining, 0)
                on_progress(processed, total_hint or processed, f"embedded chunks: {embedded}")

            if not page_token or summary.get("processed", 0) == 0:
                break

        return {"found": processed, "ingested": embedded, "errors": errors}
    finally:
        db.close()


def ensure_drive_session(user_id: str) -> None:
    """
    Raise a RuntimeError if we cannot locate a session token for this user.
    Useful for validating before starting jobs.
    """
    get_google_credentials_for_user_unmanaged(user_id)


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
