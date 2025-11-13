import os
import time
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text
from redis import from_url
from redis.exceptions import RedisError
from dotenv import load_dotenv

from app.db import SessionLocal
from app.runtime import ensure_writes_enabled
from app.settings import READ_ONLY_MODE
from app.logging_utils import log_event

load_dotenv()

app = FastAPI(title="Local Context Agent (Minimal)")

from .auth import router as auth_router
from .ingest.drive_ingest import router as drive_router
from .ingest.calendar_ingest import router as cal_router
from .ingest.routes import router as ingest_router
from .rag.routes import router as rag_router
from app.routes import jobs

app.include_router(auth_router)
app.include_router(drive_router)
app.include_router(cal_router)
app.include_router(ingest_router)
app.include_router(rag_router)
app.include_router(jobs.router)
APP_VERSION = os.getenv("APP_VERSION") or os.getenv("GIT_SHA") or "dev"


@app.middleware("http")
async def request_logging(request: Request, call_next):
    request_id = str(uuid.uuid4())
    start = time.perf_counter()
    base_log_fields = {
        "request_id": request_id,
        "method": request.method,
        "path": request.url.path,
    }
    log_event(
        "request_start",
        **base_log_fields,
        user_id=getattr(request.state, "user_id", None),
    )
    try:
        response = await call_next(request)
    except Exception as exc:
        duration_ms = round((time.perf_counter() - start) * 1000, 3)
        log_event(
            "request_error",
            **base_log_fields,
            user_id=getattr(request.state, "user_id", None),
            status="error",
            duration_ms=duration_ms,
            error=str(exc),
            level="error",
        )
        raise
    duration_ms = round((time.perf_counter() - start) * 1000, 3)
    response.headers["X-Request-ID"] = request_id
    log_event(
        "request_end",
        **base_log_fields,
        user_id=getattr(request.state, "user_id", None),
        status=response.status_code,
        duration_ms=duration_ms,
    )
    return response


@app.get("/healthz")
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


@app.get("/")
def root():
    return {"ok": True}
