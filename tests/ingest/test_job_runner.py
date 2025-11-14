from __future__ import annotations

from app.ingest import job_helper, routes as ingest_routes, queue as ingest_queue


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
    assert messages
    assert "processed file 3" in messages[-1]


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


def test_run_drive_job_fails_when_ingest_reports_errors(db_session, session_factory, test_user, monkeypatch):
    job_id = job_helper.create_job(
        db_session,
        user_id=test_user.id,
        payload={"user_id": test_user.id},
        total_files=0,
        status="queued",
    )

    def fake_ingest(**kwargs):
        return {"found": 1, "ingested": 0, "errors": 1}

    monkeypatch.setattr(ingest_routes, "INGEST_DRIVE_CALLABLE", fake_ingest)
    ingest_routes._run_drive_job(job_id)

    job = job_helper.get_job(db_session, job_id)
    assert job is not None
    assert job["status"] == "failed"
    assert "error" in (job["error_summary"] or "").lower()


def test_worker_run_ingest_throttles_progress_updates(db_session, session_factory, test_user, monkeypatch):
    job_id = job_helper.create_job(
        db_session,
        user_id=test_user.id,
        payload={"user_id": test_user.id, "max_files": 5},
        total_files=0,
        status="queued",
    )

    bumps: list[int] = []
    original_bump = job_helper.bump_job_progress

    def spy_bump(db, job_id_arg, inc=1, message=None):
        bumps.append(int(inc or 0))
        return original_bump(db, job_id_arg, inc=inc, message=message)

    def fake_ingest(user_id, name_filter=None, max_files=None, reembed_all=False, on_progress=None):
        assert on_progress is not None
        for i in range(1, 6):
            on_progress(i, 5, f"processed file {i}")
        return {"found": 5, "ingested": 5, "errors": 0}

    monkeypatch.setattr(job_helper, "bump_job_progress", spy_bump)
    monkeypatch.setattr(ingest_queue.drive_ingest, "ingest_drive", fake_ingest)
    monkeypatch.setattr(ingest_queue, "PROGRESS_FLUSH_INTERVAL", 2)

    ingest_queue._run_ingest(job_id, {"user_id": test_user.id, "max_files": 5, "name_filter": None, "reembed_all": False})

    db_session.expire_all()
    job = job_helper.get_job(db_session, job_id)
    assert job is not None
    assert job["status"] == "succeeded"
    assert job["processed_files"] == 5
    assert bumps == [2, 2, 1]
