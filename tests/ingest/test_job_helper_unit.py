from __future__ import annotations

from datetime import datetime, timezone

from app.ingest import job_helper


def _new_job(db_session, **overrides):
    payload = overrides.pop("payload", {"user_id": "user-1"})
    job_id = job_helper.create_job(
        db_session,
        user_id=payload.get("user_id", "user-1"),
        payload=payload,
        total_files=overrides.get("total_files", 0),
        status=overrides.get("status", "queued"),
    )
    return job_id


def test_mark_job_running_updates_fields(db_session):
    job_id = _new_job(db_session)
    job_helper.mark_job_running(db_session, job_id, total_files=5)
    job = job_helper.get_job(db_session, job_id)
    assert job["status"] == "running"
    assert job["total_files"] == 5
    assert job["started_at"] is not None


def test_bump_progress_increments_counter(db_session):
    job_id = _new_job(db_session)
    job_helper.bump_job_progress(db_session, job_id, inc=2)
    db_session.expire_all()
    job = job_helper.get_job(db_session, job_id)
    assert job["processed_files"] == 2


def test_append_job_log_updates_timestamp(db_session):
    job_id = _new_job(db_session)
    before = job_helper.get_job(db_session, job_id)
    job_helper.append_job_log(db_session, job_id, "processing")
    after = job_helper.get_job(db_session, job_id)
    assert after["updated_at"] >= before["updated_at"]


def test_finish_job_merges_metrics(db_session):
    job_id = _new_job(db_session)
    job_helper.finish_job(db_session, job_id, status="partial", metrics={"errors": 1})
    job = job_helper.get_job(db_session, job_id)
    assert job["status"] == "partial"
    assert job["finished_at"] is not None


def test_record_job_error_increments_metrics(db_session):
    job_id = _new_job(db_session)
    job_helper.record_job_error(db_session, job_id, "temporary issue")
    job = job_helper.get_job(db_session, job_id)
    assert (job["metrics"] or {}).get("errors") == 1
