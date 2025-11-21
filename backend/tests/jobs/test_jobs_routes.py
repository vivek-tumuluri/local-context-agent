from __future__ import annotations

from contextlib import asynccontextmanager

import pytest
from httpx import AsyncClient, ASGITransport

from app.main import app as fastapi_app
from app.core import auth as auth_module
from app.core.models import IngestionJob
from app.routes import jobs as jobs_routes


@asynccontextmanager
async def client_for(user):
    override = lambda: user
    fastapi_app.dependency_overrides[auth_module.get_current_user] = override
    fastapi_app.dependency_overrides[getattr(jobs_routes, "get_current_user")] = override
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        try:
            yield client
        finally:
            fastapi_app.dependency_overrides.pop(auth_module.get_current_user, None)
            fastapi_app.dependency_overrides.pop(getattr(jobs_routes, "get_current_user"), None)


@pytest.mark.asyncio
async def test_jobs_route_returns_status_and_filters_by_user(db_session, user_factory):
    owner = user_factory(email="owner@example.com")
    other = user_factory(email="other@example.com")

    job = IngestionJob(id="job-owner", user_id=owner.id, status="running", processed_files=2, total_files=5)
    db_session.add(job)
    db_session.commit()

    async with client_for(owner) as client:
        resp = await client.get("/jobs/job-owner")
        assert resp.status_code == 200
        body = resp.json()
        assert body["job_id"] == "job-owner"
        assert body["status"] == "running"
        assert body["processed"] == 2
        assert body["total"] == 5

    async with client_for(other) as client:
        resp = await client.get("/jobs/job-owner")
        assert resp.status_code == 404


@pytest.mark.asyncio
async def test_start_ingest_creates_job_and_can_be_polled(db_session, user_factory):
    user = user_factory(email="starter@example.com")

    async with client_for(user) as client:
        start = await client.post("/jobs/ingest")
        assert start.status_code == 200
        job_id = start.json()["job_id"]

        # Update status to simulate worker progress
        row = db_session.get(IngestionJob, job_id)
        row.status = "failed"
        row.error_summary = "boom"
        db_session.commit()

        status_resp = await client.get(f"/jobs/{job_id}")
        assert status_resp.status_code == 200
        data = status_resp.json()
        assert data["status"] == "failed"
        assert data["error_summary"] == "boom"
