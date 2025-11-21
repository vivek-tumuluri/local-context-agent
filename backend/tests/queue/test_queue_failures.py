from __future__ import annotations

import pytest
from redis.exceptions import RedisError

from app.routes import ingest_routes


@pytest.mark.asyncio
async def test_start_drive_ingest_returns_service_unavailable_when_queue_missing(monkeypatch, api_client):
    monkeypatch.setattr(ingest_routes, "ENSURE_DRIVE_SESSION", lambda user_id: None)
    monkeypatch.setattr(ingest_routes.ingest_queue, "queue_enabled", lambda: False)
    monkeypatch.setattr(ingest_routes.settings, "ALLOW_INLINE_INGEST", False, raising=False)

    resp = await api_client.post("/ingest/drive/start", json={"max_files": 1})
    assert resp.status_code == 503


@pytest.mark.asyncio
async def test_start_drive_ingest_handles_queue_enqueue_error(monkeypatch, api_client, db_session, test_user):
    monkeypatch.setattr(ingest_routes, "ENSURE_DRIVE_SESSION", lambda user_id: None)
    monkeypatch.setattr(ingest_routes.ingest_queue, "queue_enabled", lambda: True)

    def boom(*args, **kwargs):
        raise RedisError("redis unavailable")

    monkeypatch.setattr(ingest_routes.ingest_queue, "enqueue_drive_job", boom)

    with pytest.raises(RedisError):
        await api_client.post("/ingest/drive/start", json={"max_files": 1})

    # Job row should still exist because creation happens before enqueue
    from app.ingest import job_helper

    jobs = job_helper.list_jobs(db_session, user_id=test_user.id, kind=None, limit=10, offset=0)
    assert jobs
