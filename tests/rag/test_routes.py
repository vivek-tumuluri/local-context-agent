from __future__ import annotations

import pytest

from app.rag import vector


def _seed_docs(user_id: str, docs):
    chunks = []
    for idx, text in docs:
        chunks.append(
            {
                "id": f"{user_id}-{idx}",
                "text": text,
                "meta": {"doc_id": idx, "source": "drive", "title": idx.title()},
            }
        )
    vector.upsert(chunks, user_id=user_id)


@pytest.mark.asyncio
async def test_rag_search_returns_confidence(api_client, fake_vector_env, golden_drive_docs, test_user):
    _seed_docs(test_user.id, golden_drive_docs)
    resp = await api_client.post("/rag/search", json={"query": "launch milestones", "k": 3})
    assert resp.status_code == 200
    body = resp.json()
    assert body["hits"] <= 3
    assert body["confidence"] >= 0.0
    assert body["results"]


@pytest.mark.asyncio
async def test_rag_answer_calls_chat_completion(api_client, fake_vector_env, fake_chat_client, golden_drive_docs, test_user):
    fake_chat_client.queue_response("Launch is on track [1]")
    _seed_docs(test_user.id, golden_drive_docs)
    resp = await api_client.post("/rag/answer", json={"query": "When is QA freeze?", "k": 2})
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["answer"].startswith("Launch")
    assert payload["sources"]
    assert payload["retrieved"] == len(payload["sources"])
