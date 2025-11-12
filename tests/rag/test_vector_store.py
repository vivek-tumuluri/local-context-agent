from __future__ import annotations

import pytest

from app.rag import vector
from tests.fakes import FakeEmbeddingsClient


def test_upsert_query_and_list_ids(fake_vector_env):
    _, embeddings = fake_vector_env
    chunks = [
        {"id": "u-doc1-0", "text": "Launch plan draft outlines QA freeze.", "meta": {"doc_id": "doc1", "source": "drive"}},
        {"id": "u-doc2-0", "text": "Customer updates owned by Priya.", "meta": {"doc_id": "doc2", "source": "drive"}},
    ]
    summary = vector.upsert(chunks, user_id="user-1")
    assert summary["added"] == 2
    ids = vector.list_doc_chunk_ids("doc1", user_id="user-1")
    assert ids == ["u-doc1-0"]

    hits = vector.query("Who owns customer updates?", k=2, user_id="user-1")
    assert {h["meta"]["doc_id"] for h in hits} == {"doc1", "doc2"}


def test_delete_paths(fake_vector_env):
    chunks = [
        {"id": "docA-0", "text": "Alpha text", "meta": {"doc_id": "docA"}},
        {"id": "docA-1", "text": "Alpha text extra", "meta": {"doc_id": "docA"}},
        {"id": "docB-0", "text": "Beta text", "meta": {"doc_id": "docB"}},
    ]
    vector.upsert(chunks, user_id="user-42")
    deleted = vector.delete_by_doc_id("docA", user_id="user-42")
    assert deleted["deleted"] == 2
    remaining = vector.list_doc_chunk_ids("docA", user_id="user-42")
    assert remaining == []

    removed = vector.delete_ids(["docB-0"], user_id="user-42")
    assert removed == 1


def test_embed_with_retry_handles_rate_limits(fake_vector_env, monkeypatch):
    client = fake_vector_env[1]
    assert isinstance(client, FakeEmbeddingsClient)
    client.queue_failure(RuntimeError("Rate limit exceeded, try again in 10 ms"))
    points = vector._embed_with_retry(["text needing embedding"])
    assert len(points) == 1


def test_embed_with_retry_bubbles_non_rate_limit_errors(fake_vector_env):
    client = fake_vector_env[1]
    assert isinstance(client, FakeEmbeddingsClient)
    client.queue_failure(RuntimeError("fatal error"))
    with pytest.raises(RuntimeError, match="fatal"):
        vector._embed_with_retry(["boom"])


def test_healthcheck_reports_ok(fake_vector_env):
    result = vector.healthcheck(user_id="user-99")
    assert result["status"] == "ok"
