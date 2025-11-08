from __future__ import annotations

import os
import re
import time
import random
import shutil
import logging
import threading
from typing import List, Dict, Optional, Any

import chromadb
from chromadb.api.types import GetResult
from openai import OpenAI


log = logging.getLogger("vector")

EMBED_MODEL = os.getenv("EMBED_MODEL", "text-embedding-3-small")


_default_dir = os.getenv("CHROMA_DIR")
if not _default_dir:
    _default_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".chroma"))
CHROMA_DIR = _default_dir


DEFAULT_COLLECTION_PREFIX = os.getenv("COLLECTION_PREFIX", "local-context")


RESET_ON_CORRUPTION = os.getenv("CHROMA_RESET_ON_CORRUPTION", "0") == "1"


BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", "40"))
MAX_CHARS_PER_CHUNK = int(os.getenv("MAX_CHARS_PER_CHUNK", "3000"))
MAX_RETRIES = int(os.getenv("EMBED_MAX_RETRIES", "6"))
BASE_BACKOFF = float(os.getenv("EMBED_BASE_BACKOFF", "0.6"))


_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
_chroma: Optional[chromadb.PersistentClient] = None
_collection_cache: dict[str, Any] = {}


_chroma_lock = threading.Lock()

_retry_after_re = re.compile(r"try again in (\d+)\s*ms", re.IGNORECASE)
_invalid_name_re = re.compile(r"[^a-zA-Z0-9._-]+")



class VectorStoreCorrupt(RuntimeError):
    """The on-disk Chroma metadata is incompatible with this version."""

class VectorStoreReset(RuntimeError):
    """Raised when a dev-only auto-reset was performed (reingest required)."""



def _db() -> chromadb.PersistentClient:
    """Create (once) and return a persistent Chroma client."""
    global _chroma
    if _chroma is None:
        os.makedirs(CHROMA_DIR, exist_ok=True)
        _chroma = chromadb.PersistentClient(path=CHROMA_DIR)
    return _chroma


def _dev_reset_chroma_store() -> None:
    """DEV ONLY: Drop the on-disk Chroma state so it can be recreated cleanly."""
    global _chroma
    _chroma = None
    shutil.rmtree(CHROMA_DIR, ignore_errors=True)
    log.warning("[vector] DEV reset: cleared Chroma store at %s", CHROMA_DIR)


def _sanitize_segment(segment: Optional[str], fallback: str) -> str:
    """Sanitize a collection name segment to match Chroma's allowed charset."""
    seg = segment or fallback
    seg = _invalid_name_re.sub("-", seg).strip("._-")
    if len(seg) < 1:
        seg = fallback
    return seg


def _collection_key(user_id: Optional[str] = None, name: Optional[str] = None) -> str:
    if name:
        return _sanitize_segment(name, DEFAULT_COLLECTION_PREFIX)

    prefix = _sanitize_segment(DEFAULT_COLLECTION_PREFIX, "local-context")
    suffix = _sanitize_segment(user_id, "public")
    key = f"{prefix}-{suffix}"
    if len(key) < 3:
        key = f"{key}-lc"
    return key[:512]


def _col(user_id: Optional[str] = None, name: Optional[str] = None):
    """Return a Chroma collection, namespaced per user by default."""
    key = _collection_key(user_id, name)
    with _chroma_lock:
        if key in _collection_cache:
            return _collection_cache[key]
        try:
            col = _db().get_or_create_collection(key)
            _collection_cache[key] = col
            return col
        except KeyError as err:

            if err.args and err.args[0] == "_type":
                if RESET_ON_CORRUPTION:
                    log.error("[vector] Chroma metadata incompatible (%r). DEV auto-reset enabled.", err)
                    _dev_reset_chroma_store()
                    col = _db().get_or_create_collection(key)
                    _collection_cache.clear()
                    _collection_cache[key] = col

                    raise VectorStoreReset(
                        "Chroma store was reset in DEV mode; reingest required."
                    )

                log.critical("[vector] Chroma metadata incompatible and auto-reset is disabled.")
                raise VectorStoreCorrupt(
                    f"Chroma metadata at {CHROMA_DIR} is incompatible. "
                    "Run a blue/green rebuild or manual migration; do not auto-delete in prod."
                ) from err
            raise


def shutdown() -> None:
    """Clear in-memory handles. Useful for tests or graceful app shutdown."""
    global _chroma
    with _chroma_lock:
        _collection_cache.clear()
        _chroma = None



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
        delay = BASE_BACKOFF * (2 ** attempt)
        delay += random.random() * 0.25
    time.sleep(delay)



def _clean_texts(texts: List[str]) -> List[str]:
    out: List[str] = []
    for t in texts:
        if not t:
            continue
        t = t.strip()
        if t:
            out.append(t[:MAX_CHARS_PER_CHUNK])
    return out


def _embed_once(texts: List[str]) -> List[List[float]]:
    texts = _clean_texts(texts)
    if not texts:
        return []
    resp = _client.embeddings.create(input=texts, model=EMBED_MODEL)
    return [d.embedding for d in resp.data]


def _embed_with_retry(texts: List[str]) -> List[List[float]]:
    for attempt in range(MAX_RETRIES):
        try:
            return _embed_once(texts)
        except Exception as e:
            if _is_rate_limit(e):
                ra = _parse_retry_after_seconds(e)
                log.warning(
                    "[vector] Rate limited (attempt %d/%d). %s",
                    attempt + 1,
                    MAX_RETRIES,
                    f"Retry-After={ra}s" if ra else "Exponential backoff",
                )
                _sleep_with_jitter(attempt, ra)
                continue
            log.error("[vector] Embedding error (non-rate-limit): %s", e)
            raise
    raise RuntimeError("Embedding repeatedly rate-limited; exceeded max retries.")



def upsert(chunks: List[Dict[str, Any]], user_id: Optional[str] = None) -> Dict[str, int]:
    """
    Upsert text chunks into Chroma in safe batches.
    Each chunk MUST be shaped like:
      {
        "id": str,                # deterministic id: f"{user_id}:{doc_id}:{chunk_idx}"
        "text": str,              # chunk text (trimmed to MAX_CHARS_PER_CHUNK)
        "meta": dict              # include at least {"user_id": ..., "doc_id": ..., "content_hash": ...}
      }
    Returns a summary: {"batches": X, "added": Y, "errors": Z}
    """
    summary = {"batches": 0, "added": 0, "errors": 0}
    if not chunks:
        return summary

    try:
        col = _col(user_id=user_id)
    except VectorStoreReset:

        return summary

    for i in range(0, len(chunks), BATCH_SIZE):
        batch = chunks[i : i + BATCH_SIZE]


        filtered: List[tuple[str, str, dict]] = []
        for c in batch:
            cid = c["id"]
            txt = (c.get("text") or "").strip()[:MAX_CHARS_PER_CHUNK]
            if not txt:
                continue
            meta = c.get("meta", {})
            filtered.append((cid, txt, meta))

        if not filtered:
            continue

        ids  = [cid for cid, _, _ in filtered]
        docs = [txt for _, txt, _ in filtered]
        metas= [m   for _, _, m in filtered]

        try:
            vecs = _embed_with_retry(docs)
            if not vecs:
                continue
            n = len(vecs)
            col.upsert(ids=ids[:n], documents=docs[:n], metadatas=metas[:n], embeddings=vecs)
            summary["batches"] += 1
            summary["added"] += n
        except Exception as e:
            summary["errors"] += 1
            log.error("[vector] Skipping batch %d-%d after error: %s", i, i + len(batch), e)


    try:
        _db().persist()
    except Exception:
        pass

    return summary


def query(q: str, k: int = 5, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Return top-k similar chunks for query q.
    Each result: {"text", "meta", "id", "distance", "similarity"}
    """
    try:
        vecs = _embed_with_retry([q])
        if not vecs:
            return []
        col = _col(user_id=user_id)
    except VectorStoreReset:

        return []
    res = col.query(
        query_embeddings=[vecs[0]],
        n_results=k,
        include=["documents", "metadatas", "distances"],
    )

    out: List[Dict[str, Any]] = []
    if not res or not res.get("documents"):
        return out

    docs = res["documents"][0]
    metas = res.get("metadatas", [[]])[0]
    ids = res.get("ids", [[]])[0]
    dists = res.get("distances", [[]])[0]

    for i, doc in enumerate(docs):
        dist = dists[i] if i < len(dists) else None
        sim = (1.0 - dist) if isinstance(dist, (int, float)) else None
        out.append({"text": doc, "meta": metas[i] if i < len(metas) else {},
                    "id": ids[i] if i < len(ids) else None, "distance": dist, "similarity": sim})
    return out


def delete_by_doc_id(doc_id: str, user_id: Optional[str] = None) -> Dict[str, int]:
    """
    Delete all chunks for a given document id (expects meta['doc_id']=doc_id).
    NOTE: Avoids full-scan fallbacks for large sets. Returns {"deleted": N}.
    """
    try:
        col = _col(user_id=user_id)
    except VectorStoreReset:
        return {"deleted": 0}

    try:
        res: GetResult = col.get(where={"doc_id": doc_id})
        ids = res.get("ids", [])
        if ids:
            col.delete(ids=ids)
            try:
                _db().persist()
            except Exception:
                pass
            return {"deleted": len(ids)}
        return {"deleted": 0}
    except Exception as e:
        log.error("[vector] delete_by_doc_id failed: %s", e)
        return {"deleted": 0}


def reset_collection(user_id: Optional[str] = None, name: Optional[str] = None) -> None:
    """
    DEV helper: drop and recreate a collection.
    In production prefer blue/green rebuilds instead of destructive resets.
    """
    key = _collection_key(user_id, name)
    with _chroma_lock:
        _db().delete_collection(key)
        _db().get_or_create_collection(key)
        try:
            _db().persist()
        except Exception:
            pass


def healthcheck(user_id: Optional[str] = None) -> Dict[str, str]:
    """Light probe to verify the vector store is healthy."""
    try:
        _col(user_id).count()
        return {"status": "ok"}
    except VectorStoreCorrupt as e:
        return {"status": "corrupt", "detail": str(e)}
    except VectorStoreReset as e:

        return {"status": "reset", "detail": str(e)}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
