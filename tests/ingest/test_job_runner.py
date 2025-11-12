from __future__ import annotations

from app.ingest import job_helper, routes as ingest_routes


def test_run_drive_job_succeeds_and_logs_progress(db_session, session_factory, test_user, monkeypatch):
    job_id = job_helper.create_job(
        db_session,
        user_id=test_user.id,
        payload={"user_id": test_user.id, "max_files": 3},
        total_files=0,
        status="queued",
    )

    messages: list[str] = []
    original_append = job_helper._append_log_to_job

    def spy(job, message):
        messages.append(message)
        original_append(job, message)

    def fake_ingest(user_id, name_filter, max_files, reembed_all, on_progress):
        assert user_id == test_user.id
        for i in range(1, 4):
            on_progress(i, 3, f"processed file {i}")

    monkeypatch.setattr(ingest_routes, "INGEST_DRIVE_CALLABLE", fake_ingest)
    monkeypatch.setattr(job_helper, "_append_log_to_job", spy)
    ingest_routes._run_drive_job(job_id)

    db_session.expire_all()
    job = job_helper.get_job(db_session, job_id)
    assert job is not None
    assert job["status"] == "succeeded"
    assert job["processed_files"] == 3
    assert messages and messages[-1] == "processed file 3"


def test_run_drive_job_fails_when_payload_missing_user(db_session, monkeypatch):
    job_id = job_helper.create_job(
        db_session,
        user_id="ghost",
        payload={"max_files": 1},
        total_files=0,
        status="queued",
    )
    monkeypatch.setattr(ingest_routes, "INGEST_DRIVE_CALLABLE", lambda **_: None)
    ingest_routes._run_drive_job(job_id)
    job = job_helper.get_job(db_session, job_id)
    assert job["status"] == "failed"
    assert "missing user_id" in (job["error_summary"] or "")
