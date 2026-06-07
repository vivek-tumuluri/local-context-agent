from __future__ import annotations

import pytest
from fastapi import HTTPException

from app.routes import rag_routes
from app.rag import vector_store as vector


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


@pytest.mark.asyncio
async def test_rag_search_passes_source_and_uses_candidate_pool(api_client, monkeypatch):
    calls = []

    def fake_query(query: str, k: int, user_id: str, source=None):
        calls.append({"query": query, "k": k, "user_id": user_id, "source": source})
        return [
            {
                "id": f"cal-{idx}",
                "text": f"This is a long enough calendar result {idx} for filtering.",
                "meta": {"doc_id": f"cal-{idx}", "source": "calendar", "title": "Calendar Result"},
                "similarity": 0.9 - (idx * 0.01),
            }
            for idx in range(6)
        ]

    monkeypatch.setattr(rag_routes, "hybrid_query", fake_query)

    resp = await api_client.post("/rag/search", json={"query": "meeting", "k": 2, "source": "calendar"})

    assert resp.status_code == 200
    assert calls
    assert calls[0]["source"] == "calendar"
    assert calls[0]["k"] > 2
    body = resp.json()
    assert body["hits"] == 2
    assert len(body["results"]) == 2


@pytest.mark.asyncio
async def test_rag_answer_passes_source_and_bounds_final_hits(api_client, monkeypatch, fake_chat_client):
    fake_chat_client.queue_response("The calendar says review launch blockers [1].")
    calls = []

    def fake_query(query: str, k: int, user_id: str, source=None):
        calls.append({"query": query, "k": k, "user_id": user_id, "source": source})
        return [
            {
                "id": f"cal-answer-{idx}",
                "text": f"Calendar event context {idx} includes launch blocker review and QA readiness.",
                "meta": {"doc_id": f"cal-answer-{idx}", "source": "calendar", "title": "Weekly Sync"},
                "similarity": 0.95 - (idx * 0.01),
            }
            for idx in range(5)
        ]

    monkeypatch.setattr(rag_routes, "hybrid_query", fake_query)

    resp = await api_client.post("/rag/answer", json={"query": "weekly sync", "k": 2, "source": "calendar"})

    assert resp.status_code == 200
    assert calls
    assert calls[0]["source"] == "calendar"
    assert calls[0]["k"] > 2
    body = resp.json()
    assert body["retrieved"] == 2
    assert len(body["sources"]) == 2


@pytest.mark.asyncio
async def test_rag_answer_preserves_valid_citations_and_source_metadata(api_client, monkeypatch, fake_chat_client):
    fake_chat_client.queue_response("Security Review is on June 10 [1].")

    def fake_query(query: str, k: int, user_id: str, source=None):
        return [
            {
                "id": "cal-security",
                "text": "Event: Security Review\nStart: 2026-06-10T15:00:00Z\nDescription: Review OAuth scopes.",
                "meta": {
                    "doc_id": "cal-security",
                    "source": "calendar",
                    "title": "Security Review",
                    "start": "2026-06-10T15:00:00Z",
                    "end": "2026-06-10T16:00:00Z",
                    "organizer": "mina@example.com",
                    "link": "https://calendar.example/event",
                },
                "similarity": 0.95,
            }
        ]

    monkeypatch.setattr(rag_routes, "hybrid_query", fake_query)

    resp = await api_client.post("/rag/answer", json={"query": "When is Security Review?", "k": 1})

    assert resp.status_code == 200
    body = resp.json()
    assert body["answer"] == "Security Review is on June 10 [1]."
    assert body["sources"][0]["start"] == "2026-06-10T15:00:00Z"
    assert body["sources"][0]["organizer"] == "mina@example.com"


@pytest.mark.asyncio
async def test_rag_answer_strips_invalid_citation_indices(api_client, monkeypatch, fake_chat_client):
    fake_chat_client.queue_response("QA freeze is May 2 [1]. Security review is June 10 [9].")

    def fake_query(query: str, k: int, user_id: str, source=None):
        return [
            {
                "id": "launch-plan",
                "text": "Launch plan says QA freeze is May 2.",
                "meta": {"doc_id": "launch-plan", "source": "drive", "title": "Launch Plan"},
                "similarity": 0.95,
            }
        ]

    monkeypatch.setattr(rag_routes, "hybrid_query", fake_query)

    resp = await api_client.post("/rag/answer", json={"query": "When is QA freeze?", "k": 1})

    assert resp.status_code == 200
    body = resp.json()
    assert body["answer"] == "QA freeze is May 2 [1]. Security review is June 10."
    assert "[9]" not in body["answer"]


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
