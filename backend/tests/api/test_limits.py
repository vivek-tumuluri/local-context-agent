from __future__ import annotations

import os
import pytest

from app.core import limits


class DummyRedis:
    def __init__(self):
        self.store = {}

    def incr(self, key):
        self.store[key] = self.store.get(key, 0) + 1
        return self.store[key]

    def expire(self, key, ttl):
        return True


@pytest.fixture(autouse=True)
def reset_limits(monkeypatch):
    monkeypatch.setattr(limits, "_redis", DummyRedis())
    yield
    limits._redis = None


def test_check_ingest_quota_under_limit(monkeypatch):
    monkeypatch.setattr(limits, "MAX_INGESTS_PER_DAY", 2)
    limits.check_ingest_quota("user")
    limits.check_ingest_quota("user")


def test_check_ingest_quota_exceeds_limit(monkeypatch):
    monkeypatch.setattr(limits, "MAX_INGESTS_PER_DAY", 1)
    limits.check_ingest_quota("user")
    with pytest.raises(Exception):
        limits.check_ingest_quota("user")


def test_check_rag_quota_exceeds_limit(monkeypatch):
    monkeypatch.setattr(limits, "MAX_RAG_REQUESTS_PER_DAY", 1)
    limits.check_rag_quota("user")
    with pytest.raises(Exception):
        limits.check_rag_quota("user")
