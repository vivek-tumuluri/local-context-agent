from __future__ import annotations

import pytest

from app.ingest import calendar_ingest


@pytest.mark.asyncio
async def test_calendar_ingest_smoke(monkeypatch, api_client, db_session, test_user):
    fake_events = [
        {
            "id": "cal-1",
            "summary": "Planning",
            "description": "Q3 plan",
            "start": {"dateTime": "2024-01-01T10:00:00Z"},
            "end": {"dateTime": "2024-01-01T11:00:00Z"},
        }
    ]

    class FakeRequest:
        def __init__(self, payload):
            self._payload = payload

        def execute(self):
            return self._payload

    class FakeEvents:
        def list(self, **kwargs):
            return FakeRequest({"items": list(fake_events)})

    class FakeService:
        def events(self):
            return FakeEvents()

    captured = {"upserts": []}

    monkeypatch.setattr(calendar_ingest, "get_google_credentials_for_user", lambda db, user_id: object())
    monkeypatch.setattr(calendar_ingest, "build", lambda *args, **kwargs: FakeService())
    monkeypatch.setattr(calendar_ingest, "chunk_text", lambda text, meta, **kwargs: [{"id": meta["id"], "text": text, "meta": meta}])
    monkeypatch.setattr(calendar_ingest, "upsert_chunks", lambda chunks, user_id=None: captured["upserts"].append((chunks, user_id)) or {"added": len(chunks)})

    resp = await api_client.post("/ingest/calendar")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ingested"] == 1
    assert captured["upserts"]
    _, user_id = captured["upserts"][0]
    assert user_id == test_user.id
