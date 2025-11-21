from __future__ import annotations

import pytest

from app.rag import vector_store as vector


@pytest.mark.asyncio
async def test_rag_search_returns_expected_hits(api_client, monkeypatch, test_user):
    fake_hits = [
        {
            "id": "chunk-1",
            "text": "Alpha content",
            "meta": {"doc_id": "doc-1", "source": "drive", "title": "Doc One"},
            "similarity": 0.9,
        },
        {
            "id": "chunk-2",
            "text": "Beta content",
            "meta": {"doc_id": "doc-2", "source": "drive", "title": "Doc Two"},
            "similarity": 0.5,
        },
    ]

    monkeypatch.setattr("app.routes.rag_routes.vec_query", lambda q, k, user_id: list(fake_hits))

    resp = await api_client.post("/rag/search", json={"query": "alpha", "k": 2})
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["hits"] == len(fake_hits[:2])
    assert payload["results"][0]["id"] == "chunk-1"
    assert payload["results"][0]["meta"]["doc_id"] == "doc-1"
    assert payload["results"][1]["meta"]["doc_id"] == "doc-2"
    assert payload["confidence"] > 0.0
