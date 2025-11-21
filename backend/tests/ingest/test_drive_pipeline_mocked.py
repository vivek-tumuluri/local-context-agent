from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.core.models import ContentIndex, DocChunk
from app.ingest import drive_pipeline
from app.rag import vector_store as vector


def _fake_list_page(user_id: str, page_token, page_size: int):
    return {"files": [{"id": "file-1", "name": "Test Doc", "mimeType": "text/plain"}]}


def _fake_fetch(user_id: str, file_id: str, mime_type: str | None):
    return b"Hello world from Drive."


def _fake_parse(content: bytes, mime: str | None) -> str:
    return content.decode()


class _UpsertSpy:
    def __init__(self):
        self.calls = []

    def upsert(self, *args, **kwargs):
        self.calls.append((args, kwargs))


def test_drive_ingest_pipeline_creates_rows(monkeypatch, db_session, user_factory, fake_vector_env):
    user = user_factory(email="ingest@example.com")

    # Patch vector embedding + collection
    monkeypatch.setattr(vector, "_embed_with_retry", lambda texts: [[0.1] * 8 for _ in texts])
    spy = _UpsertSpy()

    class _FakeCol:
        def upsert(self, ids, documents, metadatas, embeddings):
            spy.upsert(ids, documents, metadatas, embeddings)

    monkeypatch.setattr(vector, "_col", lambda user_id: _FakeCol())

    summary = drive_pipeline.run_drive_ingest_once(
        db=db_session,
        user_id=user.id,
        list_page=_fake_list_page,
        fetch_file_bytes=_fake_fetch,
        parse_bytes=_fake_parse,
        job=None,
        page_token=None,
        page_size=5,
    )

    assert summary["processed"] == 1
    assert summary["embedded"] >= 1
    # ContentIndex row created
    saved = db_session.query(ContentIndex).filter(ContentIndex.user_id == user.id).all()
    assert len(saved) == 1
    # Vector store called
    assert spy.calls
