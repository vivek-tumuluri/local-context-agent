from __future__ import annotations

import pytest
from redis.exceptions import RedisError

import app.main as main_module


@pytest.mark.asyncio
async def test_healthz_ok(api_client, monkeypatch):
    class FakeRedis:
        def ping(self):
            return True

    monkeypatch.setattr(main_module, "from_url", lambda *_, **__: FakeRedis())

    resp = await api_client.get("/healthz")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["checks"]["db"] == "ok"
    assert data["checks"]["redis"] == "ok"
    assert data["checks"]["openai"] in {"configured", "missing"}
    assert "version" in data
    assert "read_only_mode" in data


@pytest.mark.asyncio
async def test_healthz_db_error(api_client, monkeypatch):
    class FakeRedis:
        def ping(self):
            return True

    monkeypatch.setattr(main_module, "from_url", lambda *_, **__: FakeRedis())

    class BrokenSession:
        def __enter__(self):
            raise RuntimeError("db down")

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(main_module, "SessionLocal", lambda: BrokenSession())

    resp = await api_client.get("/healthz")
    assert resp.status_code == 503
    data = resp.json()
    assert data["status"] == "degraded"
    assert data["checks"]["db"] == "error"
    assert data["checks"]["redis"] == "ok"


@pytest.mark.asyncio
async def test_healthz_redis_error(api_client, monkeypatch):
    def broken_from_url(*args, **kwargs):
        raise RedisError("redis unavailable")

    monkeypatch.setattr(main_module, "from_url", broken_from_url)

    resp = await api_client.get("/healthz")
    assert resp.status_code == 503
    data = resp.json()
    assert data["status"] == "degraded"
    assert data["checks"]["redis"] == "error"
    assert data["checks"]["db"] == "ok"
