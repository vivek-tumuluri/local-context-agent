from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
import pytest

from app.ingest import job_helper, queue as ingest_queue
from app.models import IngestionJob


def _seed_job(db_session) -> str:
    job = IngestionJob(
        user_id="user-1",
        source="drive",
        kind="drive_ingest",
        status="queued",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    db_session.add(job)
    db_session.commit()
    db_session.refresh(job)
    return job.id


def test_run_ingest_marks_job_succeeded(db_session, monkeypatch):
    if ingest_queue.queue_enabled():
        raise pytest.skip("requires queue disabled for direct call")
    job_id = _seed_job(db_session)

    def fake_ingest(**kwargs):
        return {"found": 1}

    monkeypatch.setattr(ingest_queue.drive_ingest, "ingest_drive", fake_ingest)

    result = ingest_queue._run_ingest(job_id, {"user_id": "user-1"})
    assert result == {"found": 1}
    db_session.expire_all()
    job = db_session.get(IngestionJob, job_id)
    assert job.status == "succeeded"


def test_run_ingest_retries_on_transient_error(db_session, monkeypatch):
    if ingest_queue.queue_enabled():
        raise pytest.skip("requires queue disabled for direct call")
    job_id = _seed_job(db_session)

    def fake_ingest(**kwargs):
        raise RuntimeError("rate limit")

    monkeypatch.setattr(ingest_queue.drive_ingest, "ingest_drive", fake_ingest)
    monkeypatch.setattr(ingest_queue, "_is_transient_error", lambda exc: True)

    with pytest.raises(RuntimeError):
        ingest_queue._run_ingest(job_id, {"user_id": "user-1"})

    job = job_helper.get_job(db_session, job_id)
    assert job["status"] == "running"
    assert (job["metrics"] or {}).get("errors") == 1


def test_run_ingest_marks_failed_on_permanent_error(db_session, monkeypatch):
    if ingest_queue.queue_enabled():
        raise pytest.skip("requires queue disabled for direct call")
    job_id = _seed_job(db_session)

    def fake_ingest(**kwargs):
        raise ValueError("bad request")

    monkeypatch.setattr(ingest_queue.drive_ingest, "ingest_drive", fake_ingest)
    monkeypatch.setattr(ingest_queue, "_is_transient_error", lambda exc: False)

    with pytest.raises(ValueError):
        ingest_queue._run_ingest(job_id, {"user_id": "user-1"})

    job = job_helper.get_job(db_session, job_id)
    assert job["status"] == "failed"


def test_run_ingest_marks_failed_when_result_has_errors(db_session, monkeypatch):
    if ingest_queue.queue_enabled():
        raise pytest.skip("requires queue disabled for direct call")
    job_id = _seed_job(db_session)

    def fake_ingest(**kwargs):
        return {"found": 1, "ingested": 0, "errors": 2}

    monkeypatch.setattr(ingest_queue.drive_ingest, "ingest_drive", fake_ingest)

    with pytest.raises(RuntimeError):
        ingest_queue._run_ingest(job_id, {"user_id": "user-1"})

    job = job_helper.get_job(db_session, job_id)
    assert job["status"] == "failed"
