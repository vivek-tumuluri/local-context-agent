from __future__ import annotations

import io
from uuid import uuid4
from typing import Optional, Dict, Any

from fastapi import APIRouter, Depends, Query, Header, HTTPException, Request
from sqlalchemy.orm import Session

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from app.db import get_db
from app.models import IngestionJob
from app.ingest.drive_pipeline import run_drive_ingest_once


try:
    from app.google_clients import creds_from_session
except ImportError:  # pragma: no cover
    from app.auth import creds_from_session  # type: ignore

router = APIRouter(prefix="/ingest/drive", tags=["ingest:drive"])

def fake_user():
    class U: user_id = "demo_user"
    return U()

def _require_session_token(
    request: Request,
    x_session: Optional[str] = Header(default=None, alias="X-Session"),
    session_qs: Optional[str] = Query(default=None, alias="session"),
) -> str:
    token = x_session or session_qs or request.cookies.get("session")
    if not token:
        raise HTTPException(status_code=401, detail="Missing session token (X-Session header or ?session=...)")
    return token

def _drive_service(session_token: str):
    creds = creds_from_session(session_token)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

_GOOGLE_MIME_PREFIX = "application/vnd.google-apps/"
_EXPORT_MIME = {
    "application/vnd.google-apps.document": "text/plain",
    "application/vnd.google-apps.presentation": "text/plain",
    "application/vnd.google-apps.spreadsheet": "text/csv",
    "application/vnd.google-apps.drawing": "text/plain",
}

@router.post("/run")
def run_drive(
    max_files: int = Query(50, ge=1, le=500),
    page_token: Optional[str] = Query(None),
    user=Depends(fake_user),
    session_token: str = Depends(_require_session_token),
    db: Session = Depends(get_db),
):
    svc = _drive_service(session_token)

    def list_page(user_id: str, page_token: Optional[str], page_size: int) -> Dict[str, Any]:
        req = svc.files().list(
            q="trashed = false",
            pageToken=page_token,
            pageSize=page_size,
            fields="nextPageToken, files(id,name,mimeType,md5Checksum,size,modifiedTime,trashed,version)",
            corpora="user",
            includeItemsFromAllDrives=False,
            supportsAllDrives=False,
        )
        return req.execute()

    def fetch_file_bytes(user_id: str, file_id: str, mime_type: Optional[str]) -> bytes:
        buf = io.BytesIO()
        if mime_type and mime_type.startswith(_GOOGLE_MIME_PREFIX):
            export_mime = _EXPORT_MIME.get(mime_type, "text/plain")
            request = svc.files().export_media(fileId=file_id, mimeType=export_mime)
        else:
            request = svc.files().get_media(fileId=file_id)
        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return buf.getvalue()

    def parse_bytes(content: bytes, mime_type: Optional[str]) -> str:
        try:
            return content.decode("utf-8", errors="ignore")
        except Exception:
            return ""

    job = IngestionJob(
        id=str(uuid4()),
        user_id=user.user_id,
        source="drive",
        status="running",
        total_files=0,
        processed_files=0,
    )
    db.add(job)
    db.commit()

    out = run_drive_ingest_once(
        db=db,
        user_id=user.user_id,
        list_page=list_page,
        fetch_file_bytes=fetch_file_bytes,
        parse_bytes=parse_bytes,
        job=job,
        page_token=page_token,
        page_size=max_files,
    )

    job.status = "succeeded" if out.get("errors", 0) == 0 else "partial"
    db.commit()
    return {"job_id": job.id, **out}