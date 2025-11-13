import os
import socket
from typing import Any, Dict, Optional

from redis import Redis, from_url
from redis.exceptions import ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError
from rq import Queue, Retry

from app import db as app_db
from app.ingest import drive_ingest, job_helper


_redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
_redis_conn: Optional[Redis] = None
try:
    candidate = from_url(_redis_url)
    candidate.ping()
    _redis_conn = candidate
except Exception:
    _redis_conn = None

INGEST_QUEUE = Queue("ingest", connection=_redis_conn) if _redis_conn else None
RETRY_POLICY = Retry(max=3, interval=[10, 60])
TRANSIENT_STATUSES = {429, 500, 502, 503, 504}


def _run_ingest(job_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    db = app_db.SessionLocal()
    try:
        job_helper.mark_job_running(db, job_id, total_files=0)
        result = drive_ingest.ingest_drive(**payload)
        job_helper.finish_job(db, job_id, status="succeeded", metrics=result)
        return result
    except Exception as exc:  # pragma: no cover
        summary = _format_error(exc)
        if _is_transient_error(exc):
            job_helper.record_job_error(db, job_id, f"Transient error: {summary}")
            raise
        job_helper.finish_job(db, job_id, status="failed", error_summary=summary)
        raise
    finally:
        db.close()


def enqueue_drive_job(job_id: str, payload: Dict[str, Any]) -> str:
    if INGEST_QUEUE is None:
        raise RuntimeError("ingest queue is not available")
    job = INGEST_QUEUE.enqueue(
        _run_ingest,
        job_id,
        payload,
        retry=RETRY_POLICY,
        job_timeout=3600,
    )
    return job.id


def queue_enabled() -> bool:
    return INGEST_QUEUE is not None


def _format_error(exc: Exception) -> str:
    summary = str(exc).strip() or exc.__class__.__name__
    return summary[:200] + ("..." if len(summary) > 200 else "")


def _extract_status(exc: Exception) -> Optional[int]:
    for attr in ("status", "status_code", "code"):
        val = getattr(exc, attr, None)
        if isinstance(val, int):
            return val
    resp = getattr(exc, "resp", None)
    if resp is not None:
        for attr in ("status", "status_code"):
            val = getattr(resp, attr, None)
            if isinstance(val, int):
                return val
    response = getattr(exc, "response", None)
    if response is not None:
        val = getattr(response, "status_code", None)
        if isinstance(val, int):
            return val
    return None


def _is_transient_error(exc: Exception) -> bool:
    status = _extract_status(exc)
    if status in TRANSIENT_STATUSES:
        return True
    if isinstance(exc, (TimeoutError, ConnectionError, OSError, RedisTimeoutError, RedisConnectionError, socket.timeout)):
        return True
    text = str(exc).lower()
    return any(keyword in text for keyword in ("rate limit", "timed out", "temporarily", "retry"))
