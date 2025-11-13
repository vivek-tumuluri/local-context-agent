from __future__ import annotations

import pytest
from fastapi import HTTPException

from app.rag import routes as rag_routes
from app.rag import vector


def _seed_docs(user_id: str):
    docs = [
        {"id": f"{user_id}-drive", "text": "drive doc", "meta": {"doc_id": "drive", "source": "drive"}},
        {"id": f"{user_id}-calendar", "text": "meeting", "meta": {"doc_id": "calendar", "source": "calendar"}},
    ]
    vector.upsert(docs, user_id=user_id)


@pytest.mark.asyncio
async def test_rag_search_filters_by_source(api_client, fake_vector_env, test_user):
    _seed_docs(test_user.id)
    resp = await api_client.post("/rag/search", json={"query": "meeting", "source": "calendar"})
    assert resp.status_code == 200
    body = resp.json()
    assert all(hit["meta"]["source"] == "calendar" for hit in body["results"])


def test_hit_confidence_uses_distance():
    hit = {"distance": 0.5}
    conf = rag_routes._hit_confidence(hit)
    assert 0 < conf < 1


def test_pack_context_truncates_long_text():
    hits = [
        {"text": "A" * 5000, "meta": {"title": "Doc", "source": "drive"}},
    ]
    context = rag_routes._pack_context(hits, max_chars=1000)
    assert context.count("[truncated]") == 1


@pytest.mark.asyncio
async def test_rag_answer_respects_quota(api_client, monkeypatch, fake_vector_env, fake_chat_client, test_user):
    def quota(user_id):
        raise HTTPException(status_code=429, detail="limit")

    monkeypatch.setattr(rag_routes, "check_rag_quota", quota)
    resp = await api_client.post("/rag/answer", json={"query": "test", "k": 1})
    assert resp.status_code == 429
