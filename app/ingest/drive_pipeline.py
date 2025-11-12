from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Callable
from sqlalchemy.orm import Session

from app.models import ContentIndex, IngestionJob, SourceState
from app.ingest.text_normalize import normalize_text, compute_content_hash
from app.ingest.should_ingest import should_reingest
from app.ingest.chunking import split_by_chars
from app.rag import vector

DRIVE_SOURCE = "drive"


def _load_source_state(db: Session, user_id: str) -> Optional[SourceState]:
    return (
        db.query(SourceState)
        .filter(SourceState.user_id == user_id, SourceState.source == DRIVE_SOURCE)
        .one_or_none()
    )


def load_drive_cursor(db: Session, user_id: str) -> Optional[str]:
    state = _load_source_state(db, user_id)
    return state.cursor_token if state else None


def save_drive_cursor(
    db: Session,
    user_id: str,
    cursor_token: Optional[str],
    *,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    state = _load_source_state(db, user_id)
    now = datetime.now(timezone.utc)
    if state is None:
        state = SourceState(user_id=user_id, source=DRIVE_SOURCE)
        db.add(state)
    state.cursor_token = cursor_token
    state.last_sync = now if cursor_token is None else state.last_sync or now
    if extra is not None:
        state.extra = extra
    state.updated_at = now
    db.commit()


def _to_dt(val) -> Optional[datetime]:
    if not val:
        return None
    if isinstance(val, datetime):
        return val
    s = str(val).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    else:
        if len(s) >= 5 and (s[-5] in "+-") and s[-3] != ":":
            s = s[:-2] + ":" + s[-2:]
    if "T" in s:
        date_part, time_part = s.split("T", 1)
        frac_idx = time_part.find(".")
        if frac_idx != -1:
            tz_idx = max(time_part.find("+", frac_idx), time_part.find("-", frac_idx))
            if tz_idx == -1:
                tz_idx = len(time_part)
            frac = time_part[frac_idx + 1 : tz_idx]
            if len(frac) > 6:
                time_part = time_part[: frac_idx + 1] + frac[:6] + time_part[tz_idx:]
        s = date_part + "T" + time_part
    try:
        if "T" not in s and len(s) == 10:
            return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _get_row(db: Session, user_id: str, source: str, obj_id: str) -> Optional[ContentIndex]:
    return (
        db.query(ContentIndex)
        .filter_by(user_id=user_id, source=source, id=obj_id)
        .one_or_none()
    )


def _upsert_row(
    db: Session,
    user_id: str,
    meta: Dict[str, Any],
    content_hash: Optional[str],
) -> ContentIndex:
    fid = meta["id"]
    row = _get_row(db, user_id, "drive", fid)
    now = datetime.now(timezone.utc)
    if row is None:
        row = ContentIndex(
            id=fid,
            user_id=user_id,
            source="drive",
            external_id=fid,
            name=meta.get("name"),
            path=None,
            mime_type=meta.get("mimeType"),
            md5=meta.get("md5Checksum") or meta.get("md5"),
            modified_time=_to_dt(meta.get("modifiedTime") or meta.get("modified_time")),
            size_bytes=int(meta["size"]) if str(meta.get("size", "")).isdigit() else None,
            version=meta.get("version"),
            is_trashed=bool(meta.get("trashed") or meta.get("is_trashed")),
            content_hash=content_hash,
            last_ingested_at=now if content_hash else None,
            extra={},
        )
        db.add(row)
    else:
        row.name = meta.get("name") or row.name
        row.mime_type = meta.get("mimeType") or row.mime_type
        row.md5 = meta.get("md5Checksum") or meta.get("md5") or row.md5
        row.modified_time = _to_dt(meta.get("modifiedTime") or meta.get("modified_time")) or row.modified_time
        if str(meta.get("size", "")).isdigit():
            row.size_bytes = int(meta["size"])
        row.version = meta.get("version") or row.version
        row.is_trashed = bool(meta.get("trashed") or meta.get("is_trashed"))
        if content_hash is not None:
            row.content_hash = content_hash
            row.last_ingested_at = now
    row.updated_at = now
    db.commit()
    return row


def _embed_text(user_id: str, doc_id: str, text: str, content_hash: str) -> Dict[str, int]:
    chunks = split_by_chars(text)
    to_upsert: List[Dict[str, Any]] = [
        {
            "id": f"{user_id}-{doc_id}-{i}",
            "text": ch,
            "meta": {"user_id": user_id, "doc_id": doc_id, "content_hash": content_hash},
        }
        for i, ch in enumerate(chunks)
    ]
    return vector.upsert(to_upsert, user_id=user_id)


def process_drive_file(
    db: Session,
    *,
    user_id: str,
    file_meta: Dict[str, Any],
    fetch_file_bytes: Callable[[str, str, Optional[str]], bytes],
    parse_bytes: Callable[[bytes, Optional[str]], str],
    force_reembed: bool = False,
) -> Dict[str, int]:
    """
    Normalize, dedupe, embed, and persist metadata for a single Drive file.
    Returns {"processed": 1, "embedded": N} when work was attempted.
    """
    fid = file_meta["id"]
    stored = _get_row(db, user_id, "drive", fid)
    result = {"processed": 0, "embedded": 0}

    if not force_reembed and not should_reingest(stored, file_meta):
        result["processed"] = 1
        return result

    raw = fetch_file_bytes(user_id=user_id, file_id=fid, mime_type=file_meta.get("mimeType"))
    if not raw:
        result["processed"] = 1
        return result

    parsed = parse_bytes(raw, file_meta.get("mimeType"))
    normalized = normalize_text(parsed)
    chash = compute_content_hash(normalized)

    if not force_reembed and stored and (stored.content_hash or "") == chash:
        _upsert_row(db, user_id, file_meta, stored.content_hash)
        result["processed"] = 1
        return result

    existing_ids = vector.list_doc_chunk_ids(fid, user_id=user_id)
    summary = _embed_text(user_id, fid, normalized, chash)
    new_ids = set(summary.get("ids", []))
    stale_ids = [cid for cid in existing_ids if cid not in new_ids]
    if stale_ids:
        vector.delete_ids(stale_ids, user_id=user_id)
    _upsert_row(db, user_id, file_meta, chash)

    result["processed"] = 1
    result["embedded"] = summary.get("added", 0)
    return result


def run_drive_ingest_once(
    db: Session,
    user_id: str,
    list_page: Callable[[str, Optional[str], int], Dict[str, Any]],
    fetch_file_bytes: Callable[[str, str, Optional[str]], bytes],
    parse_bytes: Callable[[bytes, Optional[str]], str],
    job: Optional[IngestionJob] = None,
    page_token: Optional[str] = None,
    page_size: int = 50,
    force_reembed: bool = False,
) -> Dict[str, Any]:
    processed = embedded = errors = 0
    next_token = None
    listing_failed = False

    try:
        listing = list_page(user_id=user_id, page_token=page_token, page_size=page_size)
        files: List[Dict[str, Any]] = list(listing.get("files", []) or [])
        next_token = listing.get("nextPageToken")
        if job:
            job.total_files = (job.total_files or 0) + len(files)
    except Exception as e:
        listing_failed = True
        if job:
            job.status = "failed"
            job.error_summary = f"list error: {e}"
            job.updated_at = datetime.now(timezone.utc)
            db.commit()
        return {"processed": 0, "embedded": 0, "errors": 1, "nextPageToken": None}

    for f in files:
        processed_delta = 0
        try:
            summary = process_drive_file(
                db,
                user_id=user_id,
                file_meta=f,
                fetch_file_bytes=fetch_file_bytes,
                parse_bytes=parse_bytes,
                force_reembed=force_reembed,
            )
            processed_delta = summary.get("processed", 0)
            processed += processed_delta
            embedded += summary.get("embedded", 0)
        except Exception:
            errors += 1

        if job:
            inc = processed_delta or 1
            job.processed_files = (job.processed_files or 0) + inc
            job.metrics = {**(job.metrics or {}), "embedded": embedded, "errors": errors}
            job.updated_at = datetime.now(timezone.utc)
            db.commit()

    return {
        "processed": processed,
        "embedded": embedded,
        "errors": errors,
        "nextPageToken": next_token,
        "listing_failed": listing_failed,
    }
