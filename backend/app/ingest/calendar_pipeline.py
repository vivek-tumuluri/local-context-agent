from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from app.core.logging_utils import log_event
from app.core.metrics import StageTimer
from app.core.models import ContentIndex, SourceState
from app.ingest.text_normalize import compute_content_hash, normalize_text
from app.rag import vector_store as vector
from app.rag.chunk import chunk_text

CALENDAR_SOURCE = "calendar"
DEFAULT_WINDOW_PAST_DAYS = 30
DEFAULT_WINDOW_FUTURE_DAYS = 180
CALENDAR_TARGET_TOKENS = 300
CALENDAR_OVERLAP_TOKENS = 40


def _load_source_state(db: Session, user_id: str) -> Optional[SourceState]:
    return (
        db.query(SourceState)
        .filter(SourceState.user_id == user_id, SourceState.source == CALENDAR_SOURCE)
        .one_or_none()
    )


def load_calendar_cursor(db: Session, user_id: str) -> Optional[str]:
    state = _load_source_state(db, user_id)
    return state.cursor_token if state else None


def save_calendar_cursor(
    db: Session,
    user_id: str,
    cursor_token: Optional[str],
    *,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    state = _load_source_state(db, user_id)
    now = datetime.now(timezone.utc)
    if state is None:
        state = SourceState(user_id=user_id, source=CALENDAR_SOURCE)
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
    try:
        if "T" not in s and len(s) == 10:
            return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _upsert_row(db: Session, user_id: str, meta: Dict[str, Any], content_hash: Optional[str]) -> ContentIndex:
    eid = meta["id"]
    row = (
        db.query(ContentIndex)
        .filter_by(user_id=user_id, source=CALENDAR_SOURCE, id=eid)
        .one_or_none()
    )
    now = datetime.now(timezone.utc)
    if row is None:
        row = ContentIndex(
            id=eid,
            user_id=user_id,
            source=CALENDAR_SOURCE,
            external_id=eid,
            name=meta.get("summary") or meta.get("title"),
            path=None,
            mime_type="text/calendar",
            md5=None,
            modified_time=_to_dt(meta.get("updated")),
            size_bytes=None,
            version=meta.get("etag"),
            is_trashed=bool(meta.get("status") == "cancelled"),
            content_hash=content_hash,
            last_ingested_at=now if content_hash else None,
            extra={
                "start": meta.get("start"),
                "end": meta.get("end"),
                "location": meta.get("location"),
                "status": meta.get("status"),
                "htmlLink": meta.get("htmlLink"),
                "calendar_id": meta.get("calendarId"),
            },
        )
        db.add(row)
    else:
        row.name = meta.get("summary") or meta.get("title") or row.name
        row.modified_time = _to_dt(meta.get("updated")) or row.modified_time
        row.version = meta.get("etag") or row.version
        row.is_trashed = bool(meta.get("status") == "cancelled")
        if content_hash is not None:
            row.content_hash = content_hash
            row.last_ingested_at = now
        extra = row.extra or {}
        extra.update(
            {
                "start": meta.get("start"),
                "end": meta.get("end"),
                "location": meta.get("location"),
                "status": meta.get("status"),
                "htmlLink": meta.get("htmlLink"),
                "calendar_id": meta.get("calendarId"),
            }
        )
        row.extra = extra
    row.updated_at = now
    return row


def _build_event_text(event: Dict[str, Any]) -> str:
    summary = event.get("summary") or event.get("title") or "(no title)"
    desc = event.get("description") or ""
    loc = event.get("location") or ""
    start = event.get("start") or ""
    end = event.get("end") or ""
    attendees = ", ".join(a.get("email") or "" for a in event.get("attendees", []) if isinstance(a, dict))
    parts = [
        f"Event: {summary}",
        f"Start: {start}",
        f"End: {end}",
    ]
    if loc:
        parts.append(f"Location: {loc}")
    if attendees:
        parts.append(f"Attendees: {attendees}")
    if desc:
        parts.append(f"Description: {desc}")
    return "\n".join(p for p in parts if p)


def _build_calendar_chunk_meta(event: Dict[str, Any]) -> Dict[str, Any]:
    doc_id = event.get("id")
    title = event.get("summary") or event.get("title") or "(untitled)"
    link = event.get("htmlLink")
    return {
        "id": doc_id,
        "doc_id": doc_id,
        "source": CALENDAR_SOURCE,
        "title": title,
        "link": link,
        "start": event.get("start"),
        "end": event.get("end"),
        "location": event.get("location"),
        "status": event.get("status"),
    }


def _build_chunk_rows(
    user_id: str,
    doc_id: str,
    text: str,
    content_hash: str,
    doc_meta: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    base_meta = {"user_id": user_id, "doc_id": doc_id, "content_hash": content_hash, "source": CALENDAR_SOURCE}
    if doc_meta:
        base_meta.update({k: v for k, v in doc_meta.items() if v is not None})

    chunks = chunk_text(
        text,
        meta=base_meta,
        target_tokens=CALENDAR_TARGET_TOKENS,
        overlap_tokens=CALENDAR_OVERLAP_TOKENS,
        sentence_level=True,
    )
    rows: List[Dict[str, Any]] = []
    for i, ch in enumerate(chunks):
        idx = ch.get("meta", {}).get("chunk_index")
        if idx is None:
            idx = i
        cid = f"{user_id}-{doc_id}-{idx}"
        rows.append(
            {
                "id": cid,
                "text": (ch.get("text") or "")[: vector.MAX_CHARS_PER_CHUNK],
                "meta": ch.get("meta", {}),
            }
        )
    return rows


def _normalize_event(raw: Dict[str, Any]) -> Dict[str, Any]:
    start = raw.get("start") or {}
    end = raw.get("end") or {}
    def _extract_time(node: Dict[str, Any]) -> Optional[str]:
        if not isinstance(node, dict):
            return None
        return node.get("dateTime") or node.get("date")

    summary = raw.get("summary") or raw.get("title") or "(no title)"
    return {
        "id": raw.get("id"),
        "summary": summary,
        "title": summary,
        "description": raw.get("description") or "",
        "location": raw.get("location") or "",
        "start": _extract_time(start),
        "end": _extract_time(end),
        "status": raw.get("status"),
        "htmlLink": raw.get("htmlLink"),
        "updated": raw.get("updated"),
        "etag": raw.get("etag"),
        "calendarId": raw.get("calendarId"),
        "attendees": raw.get("attendees") or [],
    }


def process_calendar_event(
    db: Session,
    *,
    user_id: str,
    event: Dict[str, Any],
    force_reembed: bool = False,
) -> Dict[str, int]:
    eid = event["id"]
    stored = (
        db.query(ContentIndex)
        .filter_by(user_id=user_id, source=CALENDAR_SOURCE, id=eid)
        .one_or_none()
    )
    result = {"processed": 0, "embedded": 0}

    # Cancelled events: mark trashed and delete chunks
    if event.get("status") == "cancelled":
        existing_ids = vector.list_doc_chunk_ids(eid, user_id=user_id)
        if existing_ids:
            vector.delete_ids(existing_ids, user_id=user_id)
        _upsert_row(db, user_id, event, stored.content_hash if stored else None)
        result["processed"] = 1
        return result

    text = _build_event_text(event)
    normalized = normalize_text(text)
    if not normalized:
        result["processed"] = 1
        return result
    chash = compute_content_hash(normalized)

    if not force_reembed and stored and (stored.content_hash or "") == chash:
        _upsert_row(db, user_id, event, stored.content_hash)
        result["processed"] = 1
        return result

    existing_ids = vector.list_doc_chunk_ids(eid, user_id=user_id)
    doc_meta = _build_calendar_chunk_meta(event)
    chunk_rows = _build_chunk_rows(user_id, eid, normalized, chash, doc_meta)
    if not chunk_rows:
        raise RuntimeError(f"Embedding returned no chunks for event {eid}; aborting update.")

    # Upsert new chunks and clean stale
    vector.upsert(chunk_rows, user_id=user_id)
    stale_ids = [cid for cid in existing_ids if cid not in [c["id"] for c in chunk_rows]]
    if stale_ids:
        vector.delete_ids(stale_ids, user_id=user_id)
    _upsert_row(db, user_id, event, chash)

    result["processed"] = 1
    result["embedded"] = len(chunk_rows)
    return result


def run_calendar_ingest_once(
    db: Session,
    user_id: str,
    list_events: Callable[[str, Optional[str], int, Dict[str, Any]], Dict[str, Any]],
    *,
    cursor_token: Optional[str] = None,
    page_size: int = 50,
    force_reembed: bool = False,
    window_start: Optional[datetime] = None,
    window_end: Optional[datetime] = None,
    job: Optional[Any] = None,
) -> Dict[str, Any]:
    processed = embedded = errors = 0
    next_token = None
    listing_failed = False
    pending_progress = 0
    metrics_dirty = False

    def flush_job_updates(force: bool = False) -> None:
        nonlocal pending_progress, metrics_dirty
        if not job:
            return
        if not force and pending_progress <= 0 and not metrics_dirty:
            return
        current = int(getattr(job, "processed_files", 0) or 0)
        if pending_progress:
            job.processed_files = current + pending_progress
        metrics = dict(job.metrics or {}) if job.metrics else {}
        metrics["embedded"] = embedded
        metrics["errors"] = errors
        job.metrics = metrics
        job.updated_at = datetime.now(timezone.utc)
        pending_progress = 0
        metrics_dirty = False

    window_params: Dict[str, Any] = {}
    if window_start:
        window_params["timeMin"] = window_start.isoformat()
    if window_end:
        window_params["timeMax"] = window_end.isoformat()

    try:
        with StageTimer("calendar_list_events", user_id=user_id):
            listing = list_events(user_id, cursor_token, page_size, window_params)
        events: List[Dict[str, Any]] = list(listing.get("items", []) or listing.get("events", []) or [])
        next_token = listing.get("nextPageToken") or listing.get("nextSyncToken")
        if job:
            job.total_files = (job.total_files or 0) + len(events)
    except Exception as e:
        listing_failed = True
        if job:
            job.status = "failed"
            job.error_summary = f"list error: {e}"
            job.updated_at = datetime.now(timezone.utc)
            db.commit()
        raise RuntimeError(f"Calendar listing failed: {e}") from e

    try:
        for raw in events:
            ev = _normalize_event(raw)
            processed_delta = 0
            try:
                with StageTimer("calendar_process_event", user_id=user_id, doc_id=ev.get("id")):
                    summary = process_calendar_event(
                        db,
                        user_id=user_id,
                        event=ev,
                        force_reembed=force_reembed,
                    )
                processed_delta = summary.get("processed", 0)
                processed += processed_delta
                embedded += summary.get("embedded", 0)
            except Exception as exc:
                errors += 1
                log_event(
                    "calendar_event_error",
                    user_id=user_id,
                    doc_id=ev.get("id"),
                    title=ev.get("summary"),
                    error=str(exc),
                )
                if job:
                    metrics = dict(job.metrics or {}) if job.metrics else {}
                    failed_docs = list(metrics.get("failed_docs") or [])
                    if len(failed_docs) < 25:
                        failed_docs.append({"doc_id": ev.get("id"), "name": ev.get("summary"), "error": str(exc)})
                    metrics["failed_docs"] = failed_docs
                    job.metrics = metrics
                    metrics_dirty = True

            if job:
                inc = processed_delta or 1
                pending_progress += inc
                metrics_dirty = True
                if pending_progress >= 10:
                    flush_job_updates()
    except Exception:
        db.rollback()
        raise

    flush_job_updates(force=True)
    try:
        db.commit()
    except Exception:
        db.rollback()
        raise

    return {
        "processed": processed,
        "embedded": embedded,
        "errors": errors,
        "nextPageToken": next_token,
        "listing_failed": listing_failed,
    }


def default_event_window() -> Tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=DEFAULT_WINDOW_PAST_DAYS)
    end = now + timedelta(days=DEFAULT_WINDOW_FUTURE_DAYS)
    return start, end
