from __future__ import annotations

from contextlib import asynccontextmanager

import pytest
from httpx import AsyncClient, ASGITransport

from app.main import app as fastapi_app
from app.core import auth as auth_module
from app.rag import vector_store as vector
from app.routes import ingest_routes
from tests.conftest import _ORIGINAL_GET_CURRENT_USER


@asynccontextmanager
async def client_for(user):
    override = lambda: user
    fastapi_app.dependency_overrides[auth_module.get_current_user] = override
    fastapi_app.dependency_overrides[_ORIGINAL_GET_CURRENT_USER] = override
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        try:
            yield client
        finally:
            fastapi_app.dependency_overrides.pop(auth_module.get_current_user, None)
            fastapi_app.dependency_overrides.pop(_ORIGINAL_GET_CURRENT_USER, None)


@pytest.mark.asyncio
async def test_rag_search_returns_only_requesting_user_docs(user_factory, fake_vector_env):
    user_a = user_factory(email="a@example.com")
    user_b = user_factory(email="b@example.com")

    vector.upsert(
        [{"id": f"{user_a.id}-0", "text": "alpha notes", "meta": {"doc_id": "doc-a", "source": "drive", "title": "A"}}],
        user_id=user_a.id,
    )
    vector.upsert(
        [{"id": f"{user_b.id}-0", "text": "beta notes", "meta": {"doc_id": "doc-b", "source": "drive", "title": "B"}}],
        user_id=user_b.id,
    )

    async with client_for(user_a) as client:
        resp = await client.post("/rag/search", json={"query": "notes", "k": 5})
        assert resp.status_code == 200
        docs = {hit["meta"]["doc_id"] for hit in resp.json().get("results", [])}
        assert docs == {"doc-a"}

    async with client_for(user_b) as client:
        resp = await client.post("/rag/search", json={"query": "notes", "k": 5})
        assert resp.status_code == 200
        docs = {hit["meta"]["doc_id"] for hit in resp.json().get("results", [])}
        assert docs == {"doc-b"}


@pytest.mark.asyncio
async def test_drive_ingest_jobs_are_user_scoped(monkeypatch, db_session, user_factory):
    user_a = user_factory(email="ingest-a@example.com")
    user_b = user_factory(email="ingest-b@example.com")

    monkeypatch.setattr(ingest_routes, "ENSURE_DRIVE_SESSION", lambda user_id: None)
    monkeypatch.setattr(ingest_routes.settings, "ALLOW_INLINE_INGEST", True, raising=False)
    monkeypatch.setattr(ingest_routes.ingest_queue, "queue_enabled", lambda: False)
    monkeypatch.setattr(ingest_routes, "INGEST_DRIVE_CALLABLE", lambda **kwargs: {"ingested": 0})

    async with client_for(user_a) as client_a:
        resp_a = await client_a.post("/ingest/drive/start", json={"max_files": 1})
        assert resp_a.status_code == 200
        job_a = resp_a.json()["job_id"]

    # User B should not be able to view user A's job
    async with client_for(user_b) as client_b:
        resp_b = await client_b.get(f"/ingest/jobs/{job_a}")
        assert resp_b.status_code == 403

    # User A can see their own job and status should be surfaced
    async with client_for(user_a) as client_a:
        resp = await client_a.get(f"/ingest/jobs/{job_a}")
        assert resp.status_code == 200
        body = resp.json()
        assert body["user_id"] == user_a.id
        assert body["status"] in {"succeeded", "queued", "running", "failed"}
