from __future__ import annotations

import json
import logging
import os
import random
import re
import time
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Sequence, Tuple

from openai import OpenAI
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.core.db import SessionLocal
from app.core.models import DocChunk
from app.rag.embedding_config import EMBED_DIM, EMBED_MODEL

log = logging.getLogger("vector_pg")
BACKEND_NAME = "pgvector"

DEFAULT_EMBED_BATCH_SIZE = 40
DEFAULT_MAX_CHARS_PER_CHUNK = 3000
DEFAULT_MAX_RETRIES = 6
DEFAULT_BASE_BACKOFF = 0.6

BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", str(DEFAULT_EMBED_BATCH_SIZE)))
MAX_CHARS_PER_CHUNK = int(os.getenv("MAX_CHARS_PER_CHUNK", str(DEFAULT_MAX_CHARS_PER_CHUNK)))
MAX_RETRIES = int(os.getenv("EMBED_MAX_RETRIES", str(DEFAULT_MAX_RETRIES)))
BASE_BACKOFF = float(os.getenv("EMBED_BASE_BACKOFF", str(DEFAULT_BASE_BACKOFF)))

_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
_retry_after_re = re.compile(r"try again in (\d+)\s*ms", re.IGNORECASE)


def _clean_texts(texts: Sequence[str]) -> List[str]:
    out: List[str] = []
    for text in texts:
        if not text:
            continue
        t = text.strip()
        if not t:
            continue
        out.append(t[:MAX_CHARS_PER_CHUNK])
    return out


def _embed_once(texts: Sequence[str]) -> List[List[float]]:
    cleaned = _clean_texts(list(texts))
    if not cleaned:
        return []
    resp = _client.embeddings.create(input=cleaned, model=EMBED_MODEL)
    return [d.embedding for d in resp.data]


def _is_rate_limit(err: Exception) -> bool:
    s = str(err).lower()
    return "rate limit" in s or "429" in s or "rate_limit_exceeded" in s


def _parse_retry_after_seconds(err: Exception) -> Optional[float]:
    try:
        resp = getattr(err, "response", None)
        if resp and hasattr(resp, "headers"):
            ra = resp.headers.get("retry-after")  # type: ignore[attr-defined]
            if ra:
                return float(ra)
    except Exception:
        pass

    m = _retry_after_re.search(str(err))
    if m:
        try:
            return float(m.group(1)) / 1000.0
        except Exception:
            return None
    return None


def _sleep_with_jitter(attempt: int, retry_after_s: Optional[float]) -> None:
    if retry_after_s and retry_after_s > 0:
        delay = retry_after_s
    else:
        delay = BASE_BACKOFF * (2**attempt)
        delay += random.random() * 0.25
    time.sleep(delay)


def _embed_with_retry(texts: Sequence[str]) -> List[List[float]]:
    for attempt in range(MAX_RETRIES):
        try:
            return _embed_once(texts)
        except Exception as exc:
            if _is_rate_limit(exc):
                ra = _parse_retry_after_seconds(exc)
                log.warning(
                    "[vector_pg] Rate limited (attempt %d/%d). %s",
                    attempt + 1,
                    MAX_RETRIES,
                    f"Retry-After={ra}s" if ra else "Exponential backoff",
                )
                _sleep_with_jitter(attempt, ra)
                continue
            log.error("[vector_pg] Embedding error (non-rate-limit): %s", exc)
            raise
    raise RuntimeError("Embedding repeatedly rate-limited; exceeded max retries.")


def _l2_distance(a: Sequence[float], b: Sequence[float]) -> float:
    n = min(len(a), len(b))
    return float(sum((a[i] - b[i]) ** 2 for i in range(n)))


def _is_postgres_session(session: Session) -> bool:
    try:
        bind = session.get_bind()
        return bool(bind and bind.dialect.name == "postgresql")
    except Exception:
        return False


def _coerce_embedding(val: Any) -> List[float]:
    if isinstance(val, memoryview):
        try:
            val = val.tobytes().decode()
        except Exception:
            val = val.tolist()
    if isinstance(val, (list, tuple)):
        try:
            return [float(x) for x in val]
        except Exception:
            return []
    if isinstance(val, (bytes, bytearray)):
        try:
            val = val.decode()
        except Exception:
            return []
    if isinstance(val, str):
        try:
            data = json.loads(val)
            if isinstance(data, list):
                return [float(x) for x in data]
        except Exception:
            return []
    return []


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
                        embedding=list(embeddings[idx]) if idx < len(embeddings) else [],
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
                    embedding=list(vecs[idx]),
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
        if _is_postgres_session(session):
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
        else:
            stmt = (
                select(
                    DocChunk.id,
                    DocChunk.text,
                    DocChunk.chunk_metadata,
                    DocChunk.embedding,
                )
                .where(DocChunk.user_id == normalized_user)
            )
            results = session.execute(stmt).all()
            scored: List[Tuple[float, Any]] = []
            for row in results:
                emb = _coerce_embedding(row.embedding)
                scored.append((_l2_distance(query_vec, emb), row))
            scored.sort(key=lambda tup: tup[0])
            rows = [
                SimpleNamespace(
                    id=row.id,
                    text=row.text,
                    chunk_metadata=row.chunk_metadata,
                    distance=dist,
                )
                for dist, row in scored[:k]
            ]
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
