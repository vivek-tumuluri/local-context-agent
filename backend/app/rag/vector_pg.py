from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.core.db import SessionLocal
from app.core.models import DocChunk
from app.rag import vector as vector_backend

log = logging.getLogger("vector_pg")

EMBED_MODEL = vector_backend.EMBED_MODEL
MAX_CHARS_PER_CHUNK = vector_backend.MAX_CHARS_PER_CHUNK
BATCH_SIZE = vector_backend.BATCH_SIZE

_embed_with_retry = vector_backend._embed_with_retry


def _normalize_user(user_id: Optional[str]) -> str:
    return user_id or "__public__"


def _get_session() -> Session:
    return SessionLocal()


class _PgCollection:
    def __init__(self, user_id: Optional[str]) -> None:
        self.user_id = _normalize_user(user_id)

    def upsert(
        self,
        *,
        ids: List[str],
        documents: List[str],
        metadatas: List[Dict[str, Any]],
        embeddings: List[List[float]],
    ) -> None:
        if not ids:
            return
        session = _get_session()
        try:
            session.execute(
                delete(DocChunk).where(
                    DocChunk.user_id == self.user_id,
                    DocChunk.id.in_(ids),
                )
            )
            rows = []
            for idx, cid in enumerate(ids):
                rows.append(
                    DocChunk(
                        id=cid,
                        user_id=self.user_id,
                        doc_id=metadatas[idx].get("doc_id") if idx < len(metadatas) else None,
                        source=(metadatas[idx].get("source") if idx < len(metadatas) else None) or "drive",
                        title=metadatas[idx].get("title") if idx < len(metadatas) else None,
                        text=documents[idx] if idx < len(documents) else "",
                        chunk_metadata=metadatas[idx] if idx < len(metadatas) else {},
                        embedding=embeddings[idx] if idx < len(embeddings) else [],
                    )
                )
            if rows:
                session.bulk_save_objects(rows)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


def _col(user_id: Optional[str] = None, name: Optional[str] = None) -> _PgCollection:
    return _PgCollection(user_id)


def _prepare_batch(batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    prepared: List[Dict[str, Any]] = []
    for chunk in batch:
        cid = chunk.get("id")
        meta = chunk.get("meta") or {}
        doc_id = meta.get("doc_id")
        txt = (chunk.get("text") or "").strip()
        if not cid or not doc_id or not txt:
            continue
        txt = txt[:MAX_CHARS_PER_CHUNK]
        prepared.append(
            {
                "id": cid,
                "doc_id": doc_id,
                "text": txt,
                "meta": meta,
            }
        )
    return prepared


def upsert(chunks: List[Dict[str, Any]], user_id: Optional[str] = None) -> Dict[str, Any]:
    summary = {"batches": 0, "added": 0, "errors": 0, "ids": []}
    if not chunks:
        return summary
    normalized_user = _normalize_user(user_id)
    for i in range(0, len(chunks), BATCH_SIZE):
        batch = _prepare_batch(chunks[i : i + BATCH_SIZE])
        if not batch:
            continue
        try:
            vecs = _embed_with_retry([b["text"] for b in batch])
        except Exception as exc:  # pragma: no cover
            log.error("[vector_pg] Embedding batch failed: %s", exc)
            summary["errors"] += 1
            continue

        prepared = batch[: len(vecs)]
        doc_ids = sorted({b["doc_id"] for b in prepared})
        rows = []
        for idx, item in enumerate(prepared):
            meta = item["meta"]
            rows.append(
                DocChunk(
                    id=item["id"],
                    user_id=normalized_user,
                    doc_id=item["doc_id"],
                    source=(meta.get("source") or meta.get("src") or "drive"),
                    title=meta.get("title") or meta.get("name"),
                    text=item["text"],
                    chunk_metadata=dict(meta),
                    embedding=vecs[idx],
                )
            )

        session = _get_session()
        try:
            if doc_ids:
                session.execute(
                    delete(DocChunk).where(
                        DocChunk.user_id == normalized_user,
                        DocChunk.doc_id.in_(doc_ids),
                    )
                )
            if rows:
                session.bulk_save_objects(rows)
            session.commit()
            summary["batches"] += 1
            summary["added"] += len(rows)
            summary["ids"].extend(item["id"] for item in prepared)
        except Exception as exc:  # pragma: no cover
            session.rollback()
            log.error("[vector_pg] Failed to upsert batch: %s", exc)
            summary["errors"] += 1
        finally:
            session.close()
    return summary


def query(q: str, k: int = 5, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
    vecs = _embed_with_retry([q])
    if not vecs:
        return []
    query_vec = vecs[0]
    session = _get_session()
    normalized_user = _normalize_user(user_id)
    try:
        distance = DocChunk.embedding.l2_distance(query_vec)
        stmt = (
            select(
                DocChunk.id,
                DocChunk.text,
                DocChunk.chunk_metadata,
                distance.label("distance"),
            )
            .where(DocChunk.user_id == normalized_user)
            .order_by(distance)
            .limit(k)
        )
        rows = session.execute(stmt).all()
    finally:
        session.close()

    out: List[Dict[str, Any]] = []
    for row in rows:
        dist = row.distance
        sim = (1.0 - dist) if isinstance(dist, (int, float)) else None
        meta = row.chunk_metadata or {}
        out.append(
            {
                "text": row.text,
                "meta": meta,
                "id": row.id,
                "distance": dist,
                "similarity": sim,
            }
        )
    return out


def delete_by_doc_id(doc_id: str, user_id: Optional[str] = None) -> Dict[str, int]:
    session = _get_session()
    normalized_user = _normalize_user(user_id)
    try:
        result = session.execute(
            delete(DocChunk).where(
                DocChunk.user_id == normalized_user,
                DocChunk.doc_id == doc_id,
            )
        )
        session.commit()
        return {"deleted": result.rowcount or 0}
    except Exception as exc:  # pragma: no cover
        session.rollback()
        log.error("[vector_pg] delete_by_doc_id failed: %s", exc)
        return {"deleted": 0}
    finally:
        session.close()


def list_doc_chunk_ids(doc_id: str, user_id: Optional[str] = None) -> List[str]:
    session = _get_session()
    normalized_user = _normalize_user(user_id)
    try:
        stmt = select(DocChunk.id).where(
            DocChunk.user_id == normalized_user,
            DocChunk.doc_id == doc_id,
        )
        return [row.id for row in session.execute(stmt)]
    finally:
        session.close()


def delete_ids(ids: List[str], user_id: Optional[str] = None) -> int:
    if not ids:
        return 0
    session = _get_session()
    normalized_user = _normalize_user(user_id)
    try:
        result = session.execute(
            delete(DocChunk).where(
                DocChunk.user_id == normalized_user,
                DocChunk.id.in_(ids),
            )
        )
        session.commit()
        return result.rowcount or 0
    except Exception as exc:  # pragma: no cover
        session.rollback()
        log.error("[vector_pg] delete_ids failed: %s", exc)
        return 0
    finally:
        session.close()


def reset_collection(user_id: Optional[str] = None, name: Optional[str] = None) -> None:
    clear_user(user_id)


def clear_user(user_id: Optional[str]) -> None:
    session = _get_session()
    normalized_user = _normalize_user(user_id)
    try:
        session.execute(delete(DocChunk).where(DocChunk.user_id == normalized_user))
        session.commit()
    finally:
        session.close()


def healthcheck(user_id: Optional[str] = None) -> Dict[str, str]:
    session = _get_session()
    try:
        session.execute(select(DocChunk.id).limit(1))
        return {"status": "ok"}
    except Exception as exc:  # pragma: no cover
        return {"status": "error", "detail": str(exc)}
    finally:
        session.close()


def shutdown() -> None:
    pass
