from __future__ import annotations

import os
from typing import List

import pytest
from sqlalchemy import text

from app.core.db import engine
from app.core.db_utils import create_doc_chunk_indexes, ensure_vector_extension
from app.core.models import Base
from app.rag import vector_pg

_TEST_USER_ID = "pg-user"


def _requires_postgres() -> bool:
    return engine.dialect.name == "postgresql"


@pytest.fixture
def pgvector_backend(monkeypatch):
    if os.getenv("ALLOW_PGVECTOR_INTEGRATION_TESTS") != "1":
        pytest.skip("pgvector integration tests require ALLOW_PGVECTOR_INTEGRATION_TESTS=1")
    if not _requires_postgres():
        pytest.skip("pgvector tests require PostgreSQL")
    ensure_vector_extension(engine)
    Base.metadata.create_all(bind=engine)
    create_doc_chunk_indexes(engine)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM doc_chunks WHERE user_id = :user_id"), {"user_id": _TEST_USER_ID})
    yield
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM doc_chunks WHERE user_id = :user_id"), {"user_id": _TEST_USER_ID})


@pytest.fixture
def fake_pg_embeddings(monkeypatch):
    dim = vector_pg.EMBED_DIM

    def _fake_embed(texts: List[str]) -> List[List[float]]:
        out = []
        for idx, _ in enumerate(texts):
            vec = [0.0] * dim
            vec[idx % dim] = 1.0
            out.append(vec)
        return out

    monkeypatch.setattr(vector_pg, "_embed_with_retry", _fake_embed, raising=False)
    return _fake_embed


@pytest.mark.usefixtures("pgvector_backend", "fake_pg_embeddings")
def test_pgvector_upsert_and_query(monkeypatch):
    chunks = [
        {"id": "pg-doc1-0", "text": "alpha beta docs", "meta": {"doc_id": "pg-doc1", "source": "drive"}},
        {"id": "pg-doc2-0", "text": "charlie delta notes", "meta": {"doc_id": "pg-doc2", "source": "drive"}},
    ]
    summary = vector_pg.upsert(chunks, user_id=_TEST_USER_ID)
    assert summary["added"] == 2

    hits = vector_pg.query("alpha question", k=2, user_id=_TEST_USER_ID)
    assert len(hits) == 2
    assert {hit["meta"]["doc_id"] for hit in hits} == {"pg-doc1", "pg-doc2"}

    deleted = vector_pg.delete_by_doc_id("pg-doc1", user_id=_TEST_USER_ID)
    assert deleted["deleted"] == 1
    remaining = vector_pg.list_doc_chunk_ids("pg-doc1", user_id=_TEST_USER_ID)
    assert remaining == []
