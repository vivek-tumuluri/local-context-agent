from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from googleapiclient.errors import HttpError

from app.ingest import drive_ingest


def test_should_retry_handles_429_until_limit():
    err = SimpleNamespace(resp=SimpleNamespace(status=429))
    assert drive_ingest._should_retry(err, 0) is True
    assert drive_ingest._should_retry(err, drive_ingest.MAX_LIST_RETRIES) is False


def test_sleep_with_backoff_prefers_retry_after(monkeypatch):
    captured = {"delay": None}

    def fake_sleep(delay):
        captured["delay"] = delay

    monkeypatch.setattr(drive_ingest.time, "sleep", fake_sleep)
    err = SimpleNamespace(resp={"retry-after": "1.5"})
    drive_ingest._sleep_with_backoff(err, 0)
    assert captured["delay"] == 1.5


def test_list_drive_files_accumulates_pages(monkeypatch):
    pages = [
        {"files": [{"id": "1"}], "nextPageToken": "token-1"},
        {"files": [{"id": "2"}, {"id": "3"}], "nextPageToken": None},
    ]

    class FakeRequest:
        def __init__(self, payload):
            self._payload = payload

        def execute(self):
            return self._payload

    class FakeFiles:
        def __init__(self):
            self.calls = 0

        def list(self, **kwargs):
            payload = pages[self.calls]
            self.calls += 1
            return FakeRequest(payload)

    class FakeService:
        def __init__(self):
            self._files = FakeFiles()

        def files(self):
            return self._files

    svc = FakeService()
    files = drive_ingest._list_drive_files(svc, query="q", limit=5)
    assert [f["id"] for f in files] == ["1", "2", "3"]


def test_ingest_drive_endpoint_surfaces_errors(db_session, test_user, monkeypatch):
    monkeypatch.setattr(drive_ingest, "get_google_credentials_for_user", lambda db, user_id: object())
    monkeypatch.setattr(drive_ingest, "_drive_service", lambda creds: object())
    monkeypatch.setattr(
        drive_ingest,
        "run_drive_ingest_once",
        lambda **kwargs: {
            "processed": 1,
            "embedded": 0,
            "errors": 1,
            "nextPageToken": None,
            "listing_failed": False,
        },
    )

    with pytest.raises(HTTPException):
        drive_ingest.ingest_drive_endpoint(limit=1, user=test_user, db=db_session, _csrf=None)


def test_list_page_factory_retries_on_transient_errors():
    attempts = {"count": 0}

    class FakeRequest:
        def execute(self):
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise HttpError(SimpleNamespace(status=429, reason="rate limit"), b"rate limit")
            return {"files": [{"id": "x"}], "nextPageToken": None}

    class FakeFiles:
        def list(self, **kwargs):
            return FakeRequest()

    svc = SimpleNamespace(files=lambda: FakeFiles())
    page_fn = drive_ingest._list_page_factory(svc, name_filter=None)
    result = page_fn("user", None, 10)
    assert result["files"][0]["id"] == "x"
    assert attempts["count"] == 2


def test_fetch_file_factory_retries_download(monkeypatch):
    calls = {"count": 0}

    def fake_download(svc, file_id, mime):
        calls["count"] += 1
        if calls["count"] < 2:
            raise HttpError(SimpleNamespace(status=503, reason="retry"), b"retry")
        return b"ok"

    monkeypatch.setattr(drive_ingest, "_download", fake_download)
    fetcher = drive_ingest._fetch_file_factory(object())
    blob = fetcher("user", "file-id", None)
    assert blob == b"ok"
    assert calls["count"] == 2
