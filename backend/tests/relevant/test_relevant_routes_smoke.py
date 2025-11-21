from __future__ import annotations

import pytest

from app.routes import relevant_routes


@pytest.mark.asyncio
async def test_relevant_now_smoke(monkeypatch, api_client):
    fake_events = [{"id": "evt-1", "title": "Standup", "description": "Daily sync", "start": "t1", "end": "t2"}]
    fake_hits = [
        {"text": "Doc text", "meta": {"doc_id": "doc-1", "source": "drive", "title": "Doc 1"}, "similarity": 0.8},
    ]

    monkeypatch.setattr(relevant_routes, "get_upcoming_events", lambda db, user_id, hours=24: list(fake_events))
    monkeypatch.setattr(relevant_routes, "vec_query", lambda q, k, user_id: list(fake_hits))

    resp = await api_client.get("/relevant/now")
    assert resp.status_code == 200
    body = resp.json()
    assert "results" in body and isinstance(body["results"], list)
    if body["results"]:
        entry = body["results"][0]
        assert "event" in entry
        assert "docs" in entry
