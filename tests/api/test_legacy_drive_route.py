from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.routes import ingest_drive as legacy_drive


@pytest.mark.asyncio
async def test_legacy_drive_route_aggregates_totals(api_client, db_session, monkeypatch):
    monkeypatch.setattr(legacy_drive, "get_google_credentials_for_user", lambda db, uid: object())
    monkeypatch.setattr(legacy_drive, "_drive_service", lambda creds: SimpleNamespace())

    call_count = {"n": 0}

    def fake_run_once(**kwargs):
        call_count["n"] += 1
        return {"processed": 2, "embedded": 2, "errors": 0, "nextPageToken": None}

    monkeypatch.setattr(legacy_drive, "run_drive_ingest_once", fake_run_once)

    resp = await api_client.post("/ingest/drive/run", json={})
    assert resp.status_code == 200
    body = resp.json()
    assert body["processed"] == 2
    assert body["embedded"] == 2
    assert body["errors"] == 0
    assert call_count["n"] == 1
