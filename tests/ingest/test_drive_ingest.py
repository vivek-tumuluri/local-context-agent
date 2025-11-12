from __future__ import annotations

from types import SimpleNamespace

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
