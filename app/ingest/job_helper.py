from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy.orm import Session
from app.models import IngestionJob




def utcnow() -> datetime:
    return datetime.now(timezone.utc)




ALLOWED_STATUSES: Sequence[str] = (
    "queued", "running", "succeeded", "failed", "partial"
)

def _validate_status(status: str) -> None:
    if status not in ALLOWED_STATUSES:
        raise ValueError(
            f"Invalid status '{status}'. Allowed: {', '.join(ALLOWED_STATUSES)}"
        )




def _set_if_attr(obj: Any, name: str, value: Any) -> None:
    if hasattr(obj, name):
        setattr(obj, name, value)

def _get_or_default(obj: Any, name: str, default: Any) -> Any:
    return getattr(obj, name) if hasattr(obj, name) else default

def _append_log_to_job(job: IngestionJob, message: str) -> None:
    """
    Append a timestamped message to a 'logs' JSON/list column if present.
    If no 'logs' field on the model, fall back to embedding logs under metrics['logs'].
    """
    ts = utcnow().isoformat()
    entry = {"ts": ts, "message": str(message)}

    if hasattr(job, "logs"):
        logs = _get_or_default(job, "logs", None)
        if logs is None:
            logs = []
        elif not isinstance(logs, list):

            logs = [logs]
        logs.append(entry)
        job.logs = logs  # type: ignore[attr-defined]
        return


    metrics = _get_or_default(job, "metrics", None)
    if metrics is None or not isinstance(metrics, dict):
        metrics = {}
    logs = metrics.get("logs")
    if logs is None or not isinstance(logs, list):
        logs = []
    logs.append(entry)
    metrics["logs"] = logs
    _set_if_attr(job, "metrics", metrics)

def _job_pk(job: IngestionJob) -> str:
    """
    Return the primary key for a job, handling legacy models that may expose
    either `id` or `job_id`.
    """
    job_id = getattr(job, "id", None)
    if job_id:
        return str(job_id)
    if hasattr(job, "job_id"):
        return str(getattr(job, "job_id"))
    raise AttributeError("IngestionJob is missing both 'id' and 'job_id' attributes")




def create_job(
    db: Session,
    *,
    user_id: str,
    kind: str = "drive_ingest",
    payload: Optional[Dict[str, Any]] = None,
    total_files: int = 0,
    status: str = "queued",
) -> str:
    """
    Create a new ingestion job row and return its primary key (job_id).
    Your IngestionJob model should have (ideally):
      id (pk), user_id, kind, payload (JSON), status, total_files, processed_files,
      error_summary, metrics (JSON), created_at, updated_at, started_at, finished_at
    Any missing columns are ignored.
    """
    _validate_status(status)

    job = IngestionJob()  # type: ignore[call-arg]


    _set_if_attr(job, "user_id", user_id)
    _set_if_attr(job, "kind", kind)
    _set_if_attr(job, "payload", payload or {})
    _set_if_attr(job, "status", status)
    _set_if_attr(job, "total_files", int(total_files or 0))
    _set_if_attr(job, "processed_files", 0)
    _set_if_attr(job, "error_summary", None)
    _set_if_attr(job, "metrics", {})
    _set_if_attr(job, "created_at", utcnow())
    _set_if_attr(job, "updated_at", utcnow())
    _set_if_attr(job, "started_at", None)
    _set_if_attr(job, "finished_at", None)

    db.add(job)
    db.commit()
    db.refresh(job)


    return _job_pk(job)

def get_job(db: Session, job_id: str) -> Optional[Dict[str, Any]]:
    job = db.get(IngestionJob, job_id)
    if not job:
        return None

    metrics_val = _get_or_default(job, "metrics", None)
    metrics_dict = metrics_val if isinstance(metrics_val, dict) else {}


    return {
        "job_id": _job_pk(job),
        "user_id": _get_or_default(job, "user_id", None),
        "kind": _get_or_default(job, "kind", None),
        "payload": _get_or_default(job, "payload", None),
        "status": _get_or_default(job, "status", None),
        "total_files": _get_or_default(job, "total_files", 0),
        "processed_files": _get_or_default(job, "processed_files", 0),
        "error_summary": _get_or_default(job, "error_summary", None),
        "metrics": metrics_dict or metrics_val,
        "logs": _get_or_default(job, "logs", metrics_dict.get("logs") if metrics_dict else None),
        "created_at": _get_or_default(job, "created_at", None),
        "updated_at": _get_or_default(job, "updated_at", None),
        "started_at": _get_or_default(job, "started_at", None),
        "finished_at": _get_or_default(job, "finished_at", None),
    }

def list_jobs(
    db: Session,
    *,
    user_id: Optional[str] = None,
    kind: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """
    Simple list with optional filters. Adjust to your ORM query pattern if needed.
    """
    q = db.query(IngestionJob)
    if user_id is not None and hasattr(IngestionJob, "user_id"):
        q = q.filter(IngestionJob.user_id == user_id)  # type: ignore[attr-defined]
    if kind is not None and hasattr(IngestionJob, "kind"):
        q = q.filter(IngestionJob.kind == kind)  # type: ignore[attr-defined]


    if hasattr(IngestionJob, "created_at"):
        q = q.order_by(IngestionJob.created_at.desc())  # type: ignore[attr-defined]

    rows = q.offset(offset).limit(limit).all()
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(get_job(db, _job_pk(r)) or {})
    return out




def mark_job_running(db: Session, job_id: str, total_files: int) -> None:
    job = db.get(IngestionJob, job_id)
    if not job:
        raise ValueError(f"IngestionJob {job_id} not found")
    _set_if_attr(job, "status", "running")
    _set_if_attr(job, "total_files", int(total_files or 0))
    if hasattr(job, "started_at") and _get_or_default(job, "started_at", None) is None:
        _set_if_attr(job, "started_at", utcnow())
    _set_if_attr(job, "updated_at", utcnow())
    db.commit()

def bump_job_progress(db: Session, job_id: str, inc: int = 1, message: Optional[str] = None) -> None:
    job = db.get(IngestionJob, job_id)
    if not job:
        raise ValueError(f"IngestionJob {job_id} not found")
    current = int(_get_or_default(job, "processed_files", 0) or 0)
    _set_if_attr(job, "processed_files", current + int(inc or 0))
    _set_if_attr(job, "updated_at", utcnow())
    if message:
        _append_log_to_job(job, message)
    db.commit()

def append_job_log(db: Session, job_id: str, message: str) -> None:
    job = db.get(IngestionJob, job_id)
    if not job:
        raise ValueError(f"IngestionJob {job_id} not found")
    _append_log_to_job(job, message)
    _set_if_attr(job, "updated_at", utcnow())
    db.commit()

def finish_job(
    db: Session,
    job_id: str,
    status: str = "succeeded",
    error_summary: Optional[str] = None,
    metrics: Optional[Dict[str, Any]] = None,
) -> None:
    _validate_status(status)
    job = db.get(IngestionJob, job_id)
    if not job:
        raise ValueError(f"IngestionJob {job_id} not found")

    _set_if_attr(job, "status", status)
    _set_if_attr(job, "error_summary", error_summary)

    if metrics is not None:
        existing = _get_or_default(job, "metrics", None)
        if not isinstance(existing, dict):
            existing = {}

        existing.update(metrics)
        _set_if_attr(job, "metrics", existing)

    _set_if_attr(job, "updated_at", utcnow())
    if hasattr(job, "finished_at"):
        _set_if_attr(job, "finished_at", utcnow())

    db.commit()
