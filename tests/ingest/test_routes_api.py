from __future__ import annotations

import pytest

from app.ingest import job_helper, routes as ingest_routes


@pytest.mark.asyncio
async def test_start_drive_ingest_creates_job(api_client, db_session, monkeypatch, test_user):
    ran = {"called": False}

    def fake_run(job_id: str):
        ran["called"] = True

    monkeypatch.setattr(ingest_routes, "_run_drive_job", fake_run)
    monkeypatch.setattr(ingest_routes, "ENSURE_DRIVE_SESSION", lambda user_id: None)

    resp = await api_client.post("/ingest/drive/start", json={"max_files": 2, "reembed_all": False})
    assert resp.status_code == 200, resp.text
    job_id = resp.json()["job_id"]
    job = job_helper.get_job(db_session, job_id)
    assert job and job["status"] == "queued"
    assert ran["called"] is True


@pytest.mark.asyncio
async def test_ingest_drive_endpoint_enforces_limit(api_client):
    resp = await api_client.post("/ingest/drive?limit=999")
    assert resp.status_code == 422
