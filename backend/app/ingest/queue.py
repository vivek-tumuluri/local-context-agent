import os
import socket
import time
import datetime
from typing import Any, Dict, Optional

from redis import Redis, from_url
from redis.exceptions import ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError
from rq import Queue, Retry, get_current_job

from app.core import db as app_db
from app.core.logging_utils import log_event
from app.core.settings import settings
from app.ingest import drive_ingest, job_helper
from app.ingest import calendar_pipeline
from app.core.auth import get_google_credentials_for_user_unmanaged
from app.core.models import IngestionJob
from app.ingest.job_helper import utcnow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


_redis_url = settings.redis_url or "redis://localhost:6379/0"
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
PROGRESS_FLUSH_INTERVAL = max(1, int(os.getenv("INGEST_PROGRESS_FLUSH_INTERVAL", "10")))
CALENDAR_PAGE_SIZE = max(1, int(os.getenv("INGEST_CALENDAR_PAGE_SIZE", "100")))


def _ingest_attempt_context() -> Dict[str, Any]:
    ctx: Dict[str, Any] = {"attempt": 1, "max_attempts": (RETRY_POLICY.max + 1) if RETRY_POLICY else None}
    try:
        job = get_current_job()
    except Exception:  # pragma: no cover - defensive when not run via worker
        job = None
    if not job:
        return ctx
    ctx["rq_job_id"] = job.id
    attempt = int(job.meta.get("attempt", 0)) + 1
    job.meta["attempt"] = attempt
    job.save_meta()
    ctx["attempt"] = attempt
    retries_left = getattr(job, "retries_left", None)
    if retries_left is not None and RETRY_POLICY:
        ctx["max_attempts"] = RETRY_POLICY.max + 1
        ctx["retries_left"] = retries_left
    return ctx


def _run_ingest(job_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    db = app_db.SessionLocal()
    timing_start = time.perf_counter()
    user_id = payload.get("user_id")
    attempt_ctx = _ingest_attempt_context()
    log_event(
        "ingest_job_start",
        job_id=job_id,
        user_id=user_id,
        attempt=attempt_ctx.get("attempt"),
        max_attempts=attempt_ctx.get("max_attempts"),
        rq_job_id=attempt_ctx.get("rq_job_id"),
    )
    try:
        job_record = job_helper.get_job(db, job_id)
        if not job_record:
            log_event(
                "ingest_job_missing",
                job_id=job_id,
                user_id=user_id,
                attempt=attempt_ctx.get("attempt"),
                max_attempts=attempt_ctx.get("max_attempts"),
                rq_job_id=attempt_ctx.get("rq_job_id"),
                level="warning",
            )
            return {}

        job_helper.mark_job_running(db, job_id, total_files=0)
        if int(attempt_ctx.get("attempt") or 1) > 1:
            job_row = db.get(IngestionJob, job_id)
            if job_row:
                job_row.processed_files = 0
                job_row.updated_at = utcnow()
                db.commit()

        last_reported = 0
        latest_done = 0
        last_known_total: Optional[int] = None
        pending_logs: list[str] = []

        def flush_progress(force: bool = False) -> None:
            nonlocal last_reported
            if not pending_logs and latest_done - last_reported <= 0 and not force:
                return
            increment = max(0, latest_done - last_reported)
            message = "\n".join(pending_logs) if pending_logs else None
            if increment <= 0 and not message:
                return
            job_helper.bump_job_progress(db, job_id, inc=increment or 0, message=message)
            pending_logs.clear()
            if increment:
                last_reported = latest_done

        def on_progress(done: int, total: Optional[int], msg: str = "") -> None:
            nonlocal latest_done, last_known_total
            if total is not None:
                total_val = max(0, int(total or 0))
                if last_known_total != total_val:
                    job_helper.mark_job_running(db, job_id, total_files=total_val)
                    last_known_total = total_val
            done_val = max(0, int(done or 0))
            if done_val > latest_done:
                latest_done = done_val
            if msg:
                pending_logs.append(msg)
            if latest_done - last_reported >= PROGRESS_FLUSH_INTERVAL:
                flush_progress()

        result = drive_ingest.ingest_drive(on_progress=on_progress, **payload)
        flush_progress(force=True)
        errors = int(result.get("errors") or 0)
        token_budget_hit = bool(result.get("token_budget_hit"))

        final_processed = result.get("found", result.get("processed"))
        if final_processed is not None:
            job_row = db.get(IngestionJob, job_id)
            if job_row:
                job_row.processed_files = max(0, int(final_processed or 0))
                job_row.total_files = max(int(job_row.total_files or 0), int(final_processed or 0))
                job_row.updated_at = utcnow()

        if errors:
            summary = f"Ingest completed with {errors} error(s)."
            job_helper.finish_job(db, job_id, status="failed", error_summary=summary, metrics=result)
            duration_ms = round((time.perf_counter() - timing_start) * 1000, 3)
            log_event(
                "ingest_job_completed",
                job_id=job_id,
                user_id=user_id,
                status="failed",
                duration_ms=duration_ms,
                attempt=attempt_ctx.get("attempt"),
                max_attempts=attempt_ctx.get("max_attempts"),
                rq_job_id=attempt_ctx.get("rq_job_id"),
                metrics=result,
            )
            return result
        if token_budget_hit:
            summary = (
                f"Ingest stopped after reaching token budget "
                f"({result.get('estimated_tokens_embedded')} of {settings.azeryn_max_tokens_per_job})."
            )
            job_helper.finish_job(db, job_id, status="partial", error_summary=summary, metrics=result)
            duration_ms = round((time.perf_counter() - timing_start) * 1000, 3)
            log_event(
                "ingest_job_completed",
                job_id=job_id,
                user_id=user_id,
                status="partial",
                duration_ms=duration_ms,
                attempt=attempt_ctx.get("attempt"),
                max_attempts=attempt_ctx.get("max_attempts"),
                rq_job_id=attempt_ctx.get("rq_job_id"),
                metrics=result,
            )
            return result
        job_helper.finish_job(db, job_id, status="succeeded", metrics=result)
        duration_ms = round((time.perf_counter() - timing_start) * 1000, 3)
        log_event(
            "ingest_job_completed",
            job_id=job_id,
            user_id=user_id,
            status="succeeded",
            duration_ms=duration_ms,
            attempt=attempt_ctx.get("attempt"),
            max_attempts=attempt_ctx.get("max_attempts"),
            rq_job_id=attempt_ctx.get("rq_job_id"),
            metrics=result,
        )
        return result
    except Exception as exc:  # pragma: no cover
        try:
            flush_progress(force=True)
        except Exception:
            pass
        summary = _format_error(exc)
        duration_ms = round((time.perf_counter() - timing_start) * 1000, 3)
        db.rollback()
        if _is_transient_error(exc):
            job_helper.record_job_error(db, job_id, f"Transient error: {summary}")
            log_event(
                "ingest_job_retry",
                job_id=job_id,
                user_id=user_id,
                duration_ms=duration_ms,
                error=summary,
                attempt=attempt_ctx.get("attempt"),
                max_attempts=attempt_ctx.get("max_attempts"),
                rq_job_id=attempt_ctx.get("rq_job_id"),
                level="warning",
            )
            raise
        job_helper.finish_job(db, job_id, status="failed", error_summary=summary)
        log_event(
            "ingest_job_failed",
            job_id=job_id,
            user_id=user_id,
            status="failed",
            duration_ms=duration_ms,
            error=summary,
            attempt=attempt_ctx.get("attempt"),
            max_attempts=attempt_ctx.get("max_attempts"),
            rq_job_id=attempt_ctx.get("rq_job_id"),
            level="error",
        )
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


def _list_calendar_events_factory(user_id: str):
    creds = get_google_credentials_for_user_unmanaged(user_id)
    svc = build("calendar", "v3", credentials=creds)

    def _list_events(
        _: str,
        cursor_token: Optional[str],
        page_size: int,
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        request_params: Dict[str, Any] = {
            "calendarId": "primary",
            "singleEvents": True,
            "orderBy": "startTime",
            "maxResults": page_size,
            "showDeleted": False,
        }
        time_min = params.get("timeMin")
        time_max = params.get("timeMax")
        if time_min:
            request_params["timeMin"] = time_min
        if time_max:
            request_params["timeMax"] = time_max
        if cursor_token:
            request_params["pageToken"] = cursor_token
        try:
            resp = svc.events().list(**request_params).execute()
            return resp
        except HttpError as err:
            raise RuntimeError(f"Calendar list failed: {err}") from err

    return _list_events


def queue_enabled() -> bool:
    return INGEST_QUEUE is not None


def run_calendar_ingest_job(job_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    db = app_db.SessionLocal()
    timing_start = time.perf_counter()
    user_id = payload.get("user_id")
    attempt_ctx = _ingest_attempt_context()
    log_event(
        "ingest_job_start",
        job_id=job_id,
        user_id=user_id,
        attempt=attempt_ctx.get("attempt"),
        max_attempts=attempt_ctx.get("max_attempts"),
        rq_job_id=attempt_ctx.get("rq_job_id"),
        source="calendar",
    )
    try:
        job_record = job_helper.get_job(db, job_id)
        if not job_record:
            log_event(
                "ingest_job_missing",
                job_id=job_id,
                user_id=user_id,
                attempt=attempt_ctx.get("attempt"),
                max_attempts=attempt_ctx.get("max_attempts"),
                rq_job_id=attempt_ctx.get("rq_job_id"),
                level="warning",
                source="calendar",
            )
            return {}

        job_helper.mark_job_running(db, job_id, total_files=0)

        cursor = calendar_pipeline.load_calendar_cursor(db, user_id)
        list_events = _list_calendar_events_factory(user_id)
        window_start, window_end = calendar_pipeline.default_event_window()

        errors = 0
        embedded = 0
        processed = 0

        while True:
            result = calendar_pipeline.run_calendar_ingest_once(
                db=db,
                user_id=user_id,
                list_events=list_events,
                cursor_token=cursor,
                page_size=CALENDAR_PAGE_SIZE,
                window_start=window_start,
                window_end=window_end,
                force_reembed=bool(payload.get("force_reembed")),
                job=None,
            )
            processed += int(result.get("processed") or 0)
            embedded += int(result.get("embedded") or 0)
            errors += int(result.get("errors") or 0)
            cursor = result.get("nextPageToken") or result.get("nextSyncToken")
            if not cursor or result.get("listing_failed"):
                break

        calendar_pipeline.save_calendar_cursor(db, user_id, cursor)

        job_row = db.get(IngestionJob, job_id)
        if job_row:
            job_row.processed_files = processed
            job_row.total_files = max(getattr(job_row, "total_files", 0) or 0, processed)
            job_row.updated_at = utcnow()

        summary = {"processed": processed, "embedded": embedded, "errors": errors}
        if errors:
            job_helper.finish_job(db, job_id, status="failed", error_summary=f"Ingest completed with {errors} error(s).", metrics=summary)
        else:
            job_helper.finish_job(db, job_id, status="succeeded", metrics=summary)

        duration_ms = round((time.perf_counter() - timing_start) * 1000, 3)
        log_event(
            "ingest_job_completed",
            job_id=job_id,
            user_id=user_id,
            status="succeeded" if not errors else "failed",
            duration_ms=duration_ms,
            attempt=attempt_ctx.get("attempt"),
            max_attempts=attempt_ctx.get("max_attempts"),
            rq_job_id=attempt_ctx.get("rq_job_id"),
            metrics=summary,
            source="calendar",
        )
        return summary
    except Exception as exc:  # pragma: no cover
        summary = _format_error(exc)
        duration_ms = round((time.perf_counter() - timing_start) * 1000, 3)
        if _is_transient_error(exc):
            job_helper.record_job_error(db, job_id, f"Transient error: {summary}")
            log_event(
                "ingest_job_retry",
                job_id=job_id,
                user_id=user_id,
                duration_ms=duration_ms,
                error=summary,
                attempt=attempt_ctx.get("attempt"),
                max_attempts=attempt_ctx.get("max_attempts"),
                rq_job_id=attempt_ctx.get("rq_job_id"),
                level="warning",
                source="calendar",
            )
            raise
        job_helper.finish_job(db, job_id, status="failed", error_summary=summary)
        log_event(
            "ingest_job_failed",
            job_id=job_id,
            user_id=user_id,
            status="failed",
            duration_ms=duration_ms,
            error=summary,
            attempt=attempt_ctx.get("attempt"),
            max_attempts=attempt_ctx.get("max_attempts"),
            rq_job_id=attempt_ctx.get("rq_job_id"),
            level="error",
            source="calendar",
        )
        raise
    finally:
        db.close()


def enqueue_calendar_ingest_for_user(db: app_db.SessionLocal, user_id: str, force_reembed: bool = False) -> Dict[str, Any]:
    if INGEST_QUEUE is None:
        raise RuntimeError("ingest queue is not available")
    job_id = job_helper.create_job(
        db,
        user_id=user_id,
        kind="calendar_ingest",
        payload={"user_id": user_id, "force_reembed": force_reembed},
        total_files=0,
        status="queued",
    )
    job_row = db.get(IngestionJob, job_id)
    if job_row and hasattr(job_row, "source"):
        job_row.source = "calendar"
        job_row.updated_at = utcnow()
        db.commit()

    rq_job = INGEST_QUEUE.enqueue(
        run_calendar_ingest_job,
        job_id,
        {"user_id": user_id, "force_reembed": force_reembed},
        retry=RETRY_POLICY,
        job_timeout=3600,
    )
    return job_helper.get_job(db, job_id) | {"queue_job_id": rq_job.id}


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
