from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Callable, Tuple
from sqlalchemy.orm import Session

from app.models import ContentIndex, IngestionJob, SourceState
from app.ingest.text_normalize import normalize_text, compute_content_hash
from app.ingest.should_ingest import should_reingest
from app.ingest.chunking import split_by_chars
from app.rag import vector
from app.logging_utils import log_event
from app.metrics import StageTimer

DRIVE_SOURCE = "drive"
PROGRESS_FLUSH_INTERVAL = max(1, int(os.getenv("INGEST_PROGRESS_FLUSH_INTERVAL", "10")))
EMBED_BATCH_SIZE = max(1, int(os.getenv("EMBED_BATCH_SIZE", "48")))
EMBED_TOKEN_LIMIT = max(2000, int(os.getenv("EMBED_TOKEN_LIMIT", "6000")))


class EmbeddingBatchError(RuntimeError):
    def __init__(self, message: str, docs: List["DocWork"]):
        super().__init__(message)
        self.docs = docs


@dataclass
class DocWork:
    doc_id: str
    user_id: str
    chunks: List[Dict[str, Any]]
    existing_chunk_ids: List[str]
    file_meta: Dict[str, Any]
    content_hash: str
    embedded_count: int
    new_chunk_ids: List[str] = field(default_factory=list)


class EmbeddingBatcher:
    def __init__(self, user_id: str, max_batch_size: int = EMBED_BATCH_SIZE, max_tokens: int = EMBED_TOKEN_LIMIT):
        self.user_id = user_id
        self.max_batch_size = max_batch_size
        self.max_tokens = max_tokens
        self._pending: List[Tuple[DocWork, Dict[str, Any]]] = []
        self._pending_tokens = 0
        self._doc_states: Dict[str, Dict[str, Any]] = {}
        self._collection = None

    def enqueue_doc(self, work: DocWork) -> List[DocWork]:
        if not work.chunks:
            raise RuntimeError(f"Embedding returned no chunks for document {work.doc_id}; aborting update.")
        if work.doc_id in self._doc_states:
            raise RuntimeError(f"Duplicate doc_id registered in batcher: {work.doc_id}")
        self._doc_states[work.doc_id] = {"work": work, "inserted": 0}
        for chunk in work.chunks:
            text = (chunk.get("text") or "").strip()
            if not text:
                continue
            chunk["text"] = text[: vector.MAX_CHARS_PER_CHUNK]
            self._pending.append((work, chunk))
            self._pending_tokens += self._estimate_tokens(chunk["text"])
        work.embedded_count = len(work.chunks)
        return self._maybe_flush()

    def flush(self, force: bool = False) -> List[DocWork]:
        if not force:
            return []
        return self._flush_pending()

    def _maybe_flush(self) -> List[DocWork]:
        if len(self._pending) >= self.max_batch_size or self._pending_tokens >= self.max_tokens:
            return self._flush_pending()
        return []

    def _estimate_tokens(self, text: str) -> int:
        return max(1, len(text) // 4 + 1)

    def _flush_pending(self) -> List[DocWork]:
        if not self._pending:
            return []
        items = self._pending
        tokens = self._pending_tokens
        self._pending = []
        self._pending_tokens = 0
        try:
            return self._execute_flush(items)
        except Exception as exc:
            # restore pending state so upstream retry/failure has the original queue
            self._pending = items + self._pending
            self._pending_tokens += tokens
            involved_docs = list({doc.doc_id: doc for doc, _ in items}.values())
            raise EmbeddingBatchError(str(exc), list(involved_docs)) from exc

    def _execute_flush(self, items: List[Tuple[DocWork, Dict[str, Any]]]) -> List[DocWork]:
        try:
            col = self._collection or vector._col(user_id=self.user_id)
            self._collection = col
        except Exception as exc:
            raise RuntimeError(f"Vector store unavailable: {exc}") from exc

        ids: List[str] = []
        docs: List[str] = []
        metas: List[Dict[str, Any]] = []
        doc_refs: List[DocWork] = []

        for work, chunk in items:
            ids.append(chunk["id"])
            docs.append(chunk["text"])
            metas.append(chunk.get("meta", {}))
            doc_refs.append(work)

        vectors = vector._embed_with_retry(docs)
        if len(vectors) != len(docs):
            raise RuntimeError("Embedding returned mismatched vector count.")

        col.upsert(ids=ids, documents=docs, metadatas=metas, embeddings=vectors)

        ready: List[DocWork] = []
        for work, chunk_id in zip(doc_refs, ids):
            state = self._doc_states.get(work.doc_id)
            if not state:
                continue
            state["inserted"] += 1
            work.new_chunk_ids.append(chunk_id)
            expected = len(work.chunks)
            if state["inserted"] >= expected:
                ready.append(work)
                self._doc_states.pop(work.doc_id, None)
        return ready


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
    return row


def _build_drive_chunk_meta(file_meta: Dict[str, Any]) -> Dict[str, Any]:
    doc_id = file_meta.get("id")
    title = file_meta.get("name") or file_meta.get("title") or "(untitled)"
    link = file_meta.get("webViewLink") or file_meta.get("webviewlink") or file_meta.get("webContentLink")
    if not link and doc_id:
        link = f"https://drive.google.com/file/d/{doc_id}/view"
    return {
        "id": doc_id,
        "doc_id": doc_id,
        "source": "drive",
        "title": title,
        "mime_type": file_meta.get("mimeType"),
        "link": link,
    }


def _build_chunk_rows(
    user_id: str,
    doc_id: str,
    text: str,
    content_hash: str,
    doc_meta: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    base_meta = {"user_id": user_id, "doc_id": doc_id, "content_hash": content_hash}
    if doc_meta:
        base_meta.update({k: v for k, v in doc_meta.items() if v is not None})

    rows: List[Dict[str, Any]] = []
    for i, ch in enumerate(split_by_chars(text)):
        snippet = (ch or "").strip()
        if not snippet:
            continue
        row_meta = dict(base_meta)
        row_meta["chunk_index"] = i
        rows.append(
            {
                "id": f"{user_id}-{doc_id}-{i}",
                "text": snippet[: vector.MAX_CHARS_PER_CHUNK],
                "meta": row_meta,
            }
        )
    return rows


def _finalize_ready_docs(db: Session, user_id: str, docs: List[DocWork]) -> int:
    if not docs:
        return 0
    total_embedded = 0
    for work in docs:
        new_ids = list(work.new_chunk_ids)
        if len(new_ids) != work.embedded_count:
            raise RuntimeError(f"Embedding incomplete for document {work.doc_id}; aborting update.")
        stale_ids = [cid for cid in work.existing_chunk_ids if cid not in new_ids]
        if stale_ids:
            vector.delete_ids(stale_ids, user_id=user_id)
        _upsert_row(db, user_id, work.file_meta, work.content_hash)
        total_embedded += work.embedded_count
    return total_embedded


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
    if not normalized:
        result["processed"] = 1
        return result
    chash = compute_content_hash(normalized)

    if not force_reembed and stored and (stored.content_hash or "") == chash:
        _upsert_row(db, user_id, file_meta, stored.content_hash)
        result["processed"] = 1
        return result

    existing_ids = vector.list_doc_chunk_ids(fid, user_id=user_id)
    doc_meta = _build_drive_chunk_meta(file_meta)
    chunk_rows = _build_chunk_rows(user_id, fid, normalized, chash, doc_meta)
    if not chunk_rows:
        raise RuntimeError(f"Embedding returned no chunks for document {fid}; aborting update.")

    work = DocWork(
        doc_id=fid,
        user_id=user_id,
        chunks=chunk_rows,
        existing_chunk_ids=list(existing_ids),
        file_meta=dict(file_meta),
        content_hash=chash,
        embedded_count=len(chunk_rows),
    )

    result["processed"] = 1
    result["embedded"] = 0
    result["doc_work"] = work
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
    pending_progress = 0
    metrics_dirty = False
    batcher = EmbeddingBatcher(user_id)

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

    try:
        with StageTimer("drive_list_page", user_id=user_id):
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
        raise RuntimeError(f"Drive listing failed: {e}") from e

    try:
        for f in files:
            processed_delta = 0
            try:
                with StageTimer("drive_process_file", user_id=user_id, doc_id=f.get("id")):
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
                doc_work = summary.get("doc_work")
                if doc_work:
                    try:
                        ready_docs = batcher.enqueue_doc(doc_work)
                    except EmbeddingBatchError as exc:
                        errors += len(exc.docs)
                        for failed in exc.docs:
                            log_event(
                                "drive_file_error",
                                user_id=user_id,
                                doc_id=failed.doc_id,
                                name=failed.file_meta.get("name"),
                                error=str(exc),
                            )
                        raise
                    embedded += _finalize_ready_docs(db, user_id, ready_docs)
                else:
                    embedded += summary.get("embedded", 0)
            except Exception as exc:
                errors += 1
                log_event(
                    "drive_file_error",
                    user_id=user_id,
                    doc_id=f.get("id"),
                    name=f.get("name"),
                    error=str(exc),
                )
                if job:
                    metrics = dict(job.metrics or {}) if job.metrics else {}
                    failed_docs = list(metrics.get("failed_docs") or [])
                    if len(failed_docs) < 25:
                        failed_docs.append({"doc_id": f.get("id"), "name": f.get("name"), "error": str(exc)})
                    metrics["failed_docs"] = failed_docs
                    job.metrics = metrics
                    metrics_dirty = True

            if job:
                inc = processed_delta or 1
                pending_progress += inc
                metrics_dirty = True
                if pending_progress >= PROGRESS_FLUSH_INTERVAL:
                    flush_job_updates()
    except Exception:
        db.rollback()
        raise

    try:
        ready_docs = batcher.flush(force=True)
    except EmbeddingBatchError as exc:
        errors += len(exc.docs)
        for failed in exc.docs:
            log_event(
                "drive_file_error",
                user_id=user_id,
                doc_id=failed.doc_id,
                name=failed.file_meta.get("name"),
                error=str(exc),
            )
        db.rollback()
        raise
    embedded += _finalize_ready_docs(db, user_id, ready_docs)

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
