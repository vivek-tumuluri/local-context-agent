import time
import uuid

from fastapi import FastAPI, Request
from dotenv import load_dotenv

from app.core.logging_utils import log_event
from app.routes import (
    auth_router,
    ingest_router,
    rag_router,
    health_router,
    jobs_router,
)
from app.ingest.drive_ingest import router as drive_router
from app.ingest.calendar_ingest import router as calendar_router


load_dotenv()


def create_app() -> FastAPI:
    app = FastAPI(title="Local Context Agent")

    app.include_router(auth_router)
    app.include_router(drive_router)
    app.include_router(calendar_router)
    app.include_router(ingest_router)
    app.include_router(rag_router)
    app.include_router(health_router)
    app.include_router(jobs_router)

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

    return app


app = create_app()
