from __future__ import annotations

import pytest

from app.ingest import job_helper
from app.routes import ingest_routes
from fastapi import HTTPException


@pytest.mark.asyncio
async def test_start_drive_ingest_creates_job(api_client, db_session, monkeypatch, test_user):
    ran = {"called": False}

    def fake_run(job_id: str):
        ran["called"] = True

    monkeypatch.setattr(ingest_routes, "_run_drive_job", fake_run)
    monkeypatch.setattr(ingest_routes, "ENSURE_DRIVE_SESSION", lambda user_id: None)

    resp = await api_client.post("/ingest/drive/start", json={"max_files": 2, "reembed_all": False})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["existing"] is False
    job_id = body["job_id"]
    job = job_helper.get_job(db_session, job_id)
    assert job and job["status"] == "queued"
    if body.get("queue_job_id"):
        assert ran["called"] is False
    else:
        assert ran["called"] is True


@pytest.mark.asyncio
async def test_start_drive_ingest_enqueues_when_queue_enabled(api_client, monkeypatch, test_user):
    monkeypatch.setattr(ingest_routes, "ENSURE_DRIVE_SESSION", lambda user_id: None)
    fake_rq_id = "rq-123"
    seen = {}

    def fake_enqueue(job_id: str, payload: dict):
        seen["job_id"] = job_id
        seen["payload"] = payload
        return fake_rq_id

    monkeypatch.setattr(ingest_routes.ingest_queue, "queue_enabled", lambda: True)
    monkeypatch.setattr(ingest_routes.ingest_queue, "enqueue_drive_job", fake_enqueue)

    resp = await api_client.post("/ingest/drive/start", json={"max_files": 1})
    assert resp.status_code == 200
    body = resp.json()
    assert body["queue_job_id"] == fake_rq_id
    assert body["existing"] is False
    assert seen["payload"]["user_id"] == test_user.id
    assert seen["payload"]["max_files"] == 1


@pytest.mark.asyncio
async def test_start_drive_ingest_returns_existing_job(api_client, db_session, test_user, monkeypatch):
    monkeypatch.setattr(ingest_routes, "ENSURE_DRIVE_SESSION", lambda user_id: None)
    job_id = job_helper.create_job(
        db_session,
        user_id=test_user.id,
        payload={"user_id": test_user.id},
        status="running",
    )
    resp = await api_client.post("/ingest/drive/start", json={})
    assert resp.status_code == 200
    body = resp.json()
    assert body["existing"] is True
    assert body["job_id"] == job_id


@pytest.mark.asyncio
async def test_start_drive_ingest_respects_quota(api_client, monkeypatch, test_user):
    monkeypatch.setattr(ingest_routes, "ENSURE_DRIVE_SESSION", lambda user_id: None)

    def quota(*args, **kwargs):
        raise HTTPException(status_code=429, detail="limit")

    monkeypatch.setattr(ingest_routes, "check_ingest_quota", quota)
    resp = await api_client.post("/ingest/drive/start", json={})
    assert resp.status_code == 429


@pytest.mark.asyncio
async def test_start_drive_ingest_read_only(api_client, monkeypatch, test_user):
    monkeypatch.setattr(ingest_routes, "ENSURE_DRIVE_SESSION", lambda user_id: None)

    def deny():
        raise HTTPException(status_code=503, detail="ro")

    monkeypatch.setattr(ingest_routes, "ensure_writes_enabled", deny)
    resp = await api_client.post("/ingest/drive/start", json={})
    assert resp.status_code == 503


@pytest.mark.asyncio
async def test_ingest_drive_endpoint_enforces_limit(api_client):
    resp = await api_client.post("/ingest/drive?limit=999")
    assert resp.status_code == 422
