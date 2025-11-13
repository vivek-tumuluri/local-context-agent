from __future__ import annotations

import pytest

from app import auth
from app.rag import routes as rag_routes


@pytest.mark.asyncio
async def test_auth_me_sets_csrf_cookie(api_client):
    resp = await api_client.get("/auth/me")
    assert resp.status_code == 200
    token = resp.json().get("csrf_token")
    assert token
    assert api_client.cookies.get(auth.CSRF_COOKIE_NAME, domain="test.local") == token


@pytest.mark.asyncio
async def test_csrf_endpoint_refreshes_cookie(api_client):
    api_client.cookies.set(auth.CSRF_COOKIE_NAME, "stale", domain="test")
    resp = await api_client.get("/auth/csrf")
    assert resp.status_code == 200
    new_token = resp.json().get("csrf_token")
    assert new_token and new_token != "stale"
    assert api_client.cookies.get(auth.CSRF_COOKIE_NAME, domain="test.local") == new_token


@pytest.mark.asyncio
async def test_csrf_required_for_cookie_requests(api_client, monkeypatch):
    monkeypatch.setattr(rag_routes, "vec_query", lambda *args, **kwargs: [])

    cookies = {auth.SESSION_COOKIE_NAME: "session-token"}
    resp = await api_client.post("/rag/search", json={"query": "hi", "k": 1}, cookies=cookies)
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_valid_csrf_header_allows_request(api_client, monkeypatch):
    token = "csrf-token"

    def fake_query(query: str, k: int, user_id: str):
        return [
            {"text": "chunk", "meta": {"source": "drive", "title": "Doc"}, "similarity": 0.8},
        ]

    monkeypatch.setattr(rag_routes, "vec_query", fake_query)
    headers = {auth.CSRF_HEADER_NAME: token}
    cookies = {
        auth.SESSION_COOKIE_NAME: "session-token",
        auth.CSRF_COOKIE_NAME: token,
    }

    resp = await api_client.post("/rag/search", json={"query": "hi", "k": 1}, headers=headers, cookies=cookies)
    assert resp.status_code == 200
    data = resp.json()
    assert data["hits"] == 1
