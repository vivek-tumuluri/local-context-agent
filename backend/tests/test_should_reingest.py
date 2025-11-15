from datetime import datetime, timezone, timedelta
from types import SimpleNamespace

from app.ingest.should_ingest import should_reingest


def make_stored(**overrides):
    base = {
        "is_trashed": False,
        "modified_time": datetime(2024, 1, 2, tzinfo=timezone.utc),
        "version": "1",
        "md5": "abc",
        "content_hash": "hash-a",
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_should_reingest_returns_true_when_no_existing_row():
    assert should_reingest(None, {"id": "file"}) is True


def test_should_reingest_detects_newer_modified_time():
    stored = make_stored()
    incoming = {"modifiedTime": "2024-02-01T00:00:00Z"}
    assert should_reingest(stored, incoming) is True


def test_should_reingest_detects_version_or_hash_changes():
    stored = make_stored()
    assert should_reingest(stored, {"version": "2"}) is True
    assert should_reingest(stored, {"md5Checksum": "zzz"}) is True


def test_should_reingest_detects_content_hash_difference():
    stored = make_stored(content_hash="old-hash")
    new_text = "updated text"
    assert should_reingest(stored, {}, new_text=new_text) is True


def test_should_reingest_skips_when_meta_and_content_match():
    stored = make_stored(content_hash="hash-a")
    incoming = {
        "modifiedTime": "2024-01-02T00:00:00Z",
        "md5Checksum": "abc",
        "version": "1",
    }
    assert should_reingest(stored, incoming, new_text=None) is False
