import os

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from sqlalchemy import text
from redis import from_url
from redis.exceptions import RedisError

from app.core.db import SessionLocal
from app.core.settings import READ_ONLY_MODE
from app.core.logging_utils import log_event
from app.ingest import queue as ingest_queue

router = APIRouter(tags=["health"])

APP_VERSION = os.getenv("APP_VERSION") or os.getenv("GIT_SHA") or "dev"


@router.get("/healthz")
def healthz():
    status = {
        "db": "ok",
        "redis": "ok",
        "openai": "configured" if os.getenv("OPENAI_API_KEY") else "missing",
    }
    http_status = 200

    try:
        with SessionLocal() as session:
            session.execute(text("SELECT 1"))
    except Exception as exc:
        status["db"] = "error"
        http_status = 503
        log_event("healthz_db_error", error=str(exc), level="error")

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    try:
        client = from_url(redis_url)
        client.ping()
    except RedisError as exc:
        status["redis"] = "error"
        http_status = 503
        log_event("healthz_redis_error", error=str(exc), level="error")

    payload = {
        "status": "ok" if http_status == 200 else "degraded",
        "checks": status,
        "read_only_mode": READ_ONLY_MODE,
        "version": APP_VERSION,
    }

    return JSONResponse(status_code=http_status, content=payload)


@router.get("/health/worker")
def worker_health():
    """Lightweight check for queue connectivity (reachable Redis/RQ)."""
    queue = ingest_queue.INGEST_QUEUE
    try:
        if queue and queue.connection:
            queue.connection.ping()
            return JSONResponse(status_code=200, content={"status": "ok", "queue": "reachable"})
    except Exception as exc:
        log_event("health_worker_queue_error", error=str(exc), level="error")
    return JSONResponse(status_code=503, content={"status": "degraded", "queue": "unreachable"})


@router.get("/")
def root():
    return {"ok": True}
