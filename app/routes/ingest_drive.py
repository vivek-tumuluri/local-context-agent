from __future__ import annotations

import io
from uuid import uuid4
from typing import Optional, Dict, Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from app.db import get_db
from app.models import IngestionJob
from app.ingest.drive_pipeline import run_drive_ingest_once
from app.auth import get_current_user, get_google_credentials_for_user

router = APIRouter(prefix="/ingest/drive", tags=["ingest:drive"])

def _drive_service(creds):
    return build("drive", "v3", credentials=creds, cache_discovery=False)

_GOOGLE_MIME_PREFIX = "application/vnd.google-apps/"
_EXPORT_MIME = {
    "application/vnd.google-apps.document": "text/plain",
    "application/vnd.google-apps.presentation": "text/plain",
    "application/vnd.google-apps.spreadsheet": "text/csv",
    "application/vnd.google-apps.drawing": "text/plain",
}

# NOTE: Legacy endpoint. This wrapper was kept for potential future scripting use,
# but it currently bubbles up errors poorly and needs further refactoring before
# relying on it in production.
@router.post("/run")
def run_drive(
    max_files: int = Query(50, ge=1, le=500),
    page_token: Optional[str] = Query(None),
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    creds = get_google_credentials_for_user(db, user.user_id)
    svc = _drive_service(creds)

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

    total_processed = total_embedded = total_errors = 0
    remaining = max_files
    next_page = page_token

    while remaining > 0:
        page_size = min(remaining, max_files)
        out = run_drive_ingest_once(
            db=db,
            user_id=user.user_id,
            list_page=list_page,
            fetch_file_bytes=fetch_file_bytes,
            parse_bytes=parse_bytes,
            job=job,
            page_token=next_page,
            page_size=page_size,
        )
        total_processed += out.get("processed", 0)
        total_embedded += out.get("embedded", 0)
        total_errors += out.get("errors", 0)
        next_page = out.get("nextPageToken")
        remaining -= out.get("processed", 0)
        if not next_page or out.get("processed", 0) == 0:
            break

    job.status = "succeeded" if total_errors == 0 else "partial"
    db.commit()
    return {
        "job_id": job.id,
        "processed": total_processed,
        "embedded": total_embedded,
        "errors": total_errors,
        "nextPageToken": next_page,
    }
