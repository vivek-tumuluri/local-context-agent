from __future__ import annotations

import pytest

from app.core.models import DocChunk
from app.rag import vector_store as vector
from app.rag import vector_pg


def _chunk(user_id: str, doc_id: str, text: str):
    return {"id": f"{user_id}-{doc_id}", "text": text, "meta": {"doc_id": doc_id, "source": "drive", "title": doc_id}}


@pytest.mark.asyncio
async def test_vector_queries_are_user_scoped(db_session, user_factory, monkeypatch, fake_vector_env):
    monkeypatch.setattr(vector_pg, "_embed_with_retry", lambda texts: [[0.0] * vector_pg.EMBED_DIM for _ in texts])
    user_a = user_factory(email="a@example.com")
    user_b = user_factory(email="b@example.com")

    # Seed chunks for each user
    vector.upsert([_chunk(user_a.id, "doc-a1", "alpha content")], user_id=user_a.id)
    vector.upsert([_chunk(user_b.id, "doc-b1", "beta content")], user_id=user_b.id)

    hits_a = vector.query("alpha", user_id=user_a.id, k=5)
    assert all((h.get("meta") or {}).get("doc_id", "").startswith("doc-a") for h in hits_a)

    hits_b = vector.query("beta", user_id=user_b.id, k=5)
    assert all((h.get("meta") or {}).get("doc_id", "").startswith("doc-b") for h in hits_b)

    # Ensure cross-user leakage does not occur
    assert not any((h.get("meta") or {}).get("doc_id") == "doc-b1" for h in hits_a)
    assert not any((h.get("meta") or {}).get("doc_id") == "doc-a1" for h in hits_b)
