from fastapi import APIRouter, Depends, HTTPException
from uuid import uuid4
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.core.models import IngestionJob
from app.core.auth import csrf_protect, get_current_user

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post("/ingest")
def start_ingest(
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
    _csrf=Depends(csrf_protect),
):
    """
    Create a new ingestion job row (queued) and return its ID.
    """
    job_id = str(uuid4())
    job = IngestionJob(
        id=job_id,
        user_id=user.user_id,
        source="drive",
        status="queued",
    )
    db.add(job)
    db.commit()
    return {"job_id": job_id, "status": "queued"}

@router.get("/{job_id}")
def get_job(job_id: str, user=Depends(get_current_user), db: Session = Depends(get_db)):
    """
    Return the current status and metrics for a given job.
    """
    job = db.get(IngestionJob, job_id)
    if not job or job.user_id != user.user_id:
        raise HTTPException(status_code=404, detail="job not found")

    return {
        "job_id": job.id,
        "user_id": job.user_id,
        "source": job.source,
        "status": job.status,
        "processed": job.processed_files,
        "total": job.total_files,
        "metrics": job.metrics,
        "error_summary": job.error_summary,
    }
