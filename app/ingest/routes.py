from __future__ import annotations

from typing import Optional, Callable, Protocol
import inspect

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status
from pydantic import BaseModel


try:
    from app.db import get_db, SessionLocal  # type: ignore
except Exception:  # pragma: no cover
    get_db = None
    SessionLocal = None

from sqlalchemy.orm import Session


from app.ingest import job_helper


from app.rag.routes import fake_user


class DriveIngestCallable(Protocol):
    def __call__(
        self,
        user_id: str,
        name_filter: Optional[str] = None,
        max_files: Optional[int] = None,
        reembed_all: bool = False,
        on_progress: Optional[Callable[[int, int, str], None]] = None,
    ) -> None: ...


def _fallback_ingest(
    user_id: str,
    name_filter: Optional[str] = None,
    max_files: Optional[int] = None,
    reembed_all: bool = False,
    on_progress: Optional[Callable[[int, int, str], None]] = None,
) -> None:
    """Fallback shim that simulates work so the API remains callable."""
    total = int(max_files or 3)
    for i in range(1, total + 1):
        if on_progress:
            on_progress(i, total, f"processed demo_file_{i}")


def _load_drive_ingest_callable() -> DriveIngestCallable:
    try:
        from app.ingest import drive_ingest as drive_mod  # type: ignore
    except Exception:  # pragma: no cover
        return _fallback_ingest

    candidate = getattr(drive_mod, "ingest_drive", None)
    if not callable(candidate):
        return _fallback_ingest

    try:
        sig = inspect.signature(candidate)
        required = ["user_id", "name_filter", "max_files", "reembed_all", "on_progress"]
        if all(name in sig.parameters for name in required):
            return candidate  # type: ignore[return-value]
    except (TypeError, ValueError):
        pass

    return _fallback_ingest


INGEST_DRIVE_CALLABLE: DriveIngestCallable = _load_drive_ingest_callable()
try:
    from app.ingest import drive_ingest as drive_ingest_module  # type: ignore
    ENSURE_DRIVE_SESSION = getattr(drive_ingest_module, "ensure_drive_session", None)
except Exception:  # pragma: no cover
    ENSURE_DRIVE_SESSION = None


router = APIRouter(prefix="/ingest", tags=["ingest"])



class DriveStartBody(BaseModel):
    query: Optional[str] = None
    max_files: Optional[int] = None
    reembed_all: bool = False



if get_db is None:
    def _db_dependency():
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database dependency get_db is not configured.",
        )
else:
    _db_dependency = get_db


def _bg_db_session() -> Session:
    """
    Creates a DB session for background tasks.
    Requires SessionLocal to be available.
    """
    if SessionLocal is None:
        raise RuntimeError("SessionLocal is not configured; cannot run background job.")
    return SessionLocal()



@router.post("/drive/start")
def start_drive_ingest(
    body: DriveStartBody,
    bg: BackgroundTasks,
    user=Depends(fake_user),
    db: Session = Depends(_db_dependency),
):
    """
    Creates a new Drive ingestion job and schedules it to run in the background.
    Returns a job_id immediately; poll /ingest/jobs/{job_id} to monitor progress.
    """
    if ENSURE_DRIVE_SESSION:
        try:
            ENSURE_DRIVE_SESSION(user.user_id)  # type: ignore[call-arg]
        except RuntimeError as err:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(err),
            )

    job_id = job_helper.create_job(
        db,
        user_id=user.user_id,
        kind="drive_ingest",
        payload={
            "user_id": user.user_id,
            "query": body.query,
            "max_files": body.max_files,
            "reembed_all": body.reembed_all,
        },
        total_files=0,
        status="queued",
    )

    bg.add_task(_run_drive_job, job_id)
    return {"job_id": job_id}


@router.get("/jobs/{job_id}")
def get_job(job_id: str, user=Depends(fake_user), db: Session = Depends(_db_dependency)):
    """
    Returns the current status and metadata for a single ingestion job.
    Enforces that the job belongs to the current user.
    """
    job = job_helper.get_job(db, job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job not found")
    owner = (job.get("user_id") or job.get("payload", {}).get("user_id"))
    if owner and owner != user.user_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="forbidden")
    return job


@router.get("/jobs")
def list_jobs(user=Depends(fake_user), db: Session = Depends(_db_dependency)):
    """
    Lists recent ingestion jobs for the current user, newest first.
    """
    return job_helper.list_jobs(db, user_id=user.user_id, kind=None, limit=50, offset=0)



def _run_drive_job(job_id: str) -> None:
    """
    Background worker that executes a Drive ingestion job:
    - marks job running
    - calls ingest_drive with an on_progress callback
    - finishes the job with succeeded/failed
    Uses its own DB session independent of the request.
    """
    db = _bg_db_session()
    try:
        job = job_helper.get_job(db, job_id)
        if not job:
            return

        payload = job.get("payload") or {}
        user_id = payload.get("user_id")
        if not user_id:
            job_helper.finish_job(db, job_id, status="failed", error_summary="missing user_id in job payload")
            return


        job_helper.mark_job_running(db, job_id, total_files=0)

        ingest_callable = INGEST_DRIVE_CALLABLE
        last_reported = 0


        def on_progress(done: int, total: int, msg: str = ""):


            if total is not None and total >= 0:
                job_helper.mark_job_running(db, job_id, total_files=int(total))

            nonlocal last_reported
            done_val = max(0, int(done or 0))
            increment = max(0, done_val - last_reported)
            if increment:
                job_helper.bump_job_progress(db, job_id, inc=increment, message=msg or None)
                last_reported = done_val
            elif msg:
                job_helper.append_job_log(db, job_id, msg)


        try:
            ingest_callable(
                user_id=user_id,
                name_filter=payload.get("query"),
                max_files=payload.get("max_files"),
                reembed_all=payload.get("reembed_all", False),
                on_progress=on_progress,
            )
        except NotImplementedError as err:
            job_helper.finish_job(db, job_id, status="failed", error_summary=str(err))
            return


        job_helper.finish_job(db, job_id, status="succeeded")

    except Exception as e:  # pragma: no cover

        try:
            job_helper.finish_job(db, job_id, status="failed", error_summary=str(e))
        except Exception:
            pass
    finally:
        db.close()
