from __future__ import annotations

from types import SimpleNamespace

from app.core import db_utils


class _RecordingConn:
    def __init__(self):
        self.calls = []

    def execute(self, stmt):
        self.calls.append(str(stmt))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _RecordingEngine:
    def __init__(self):
        self.dialect = SimpleNamespace(name="postgresql")
        self.calls = []

    def begin(self):
        conn = _RecordingConn()
        self.calls.append(conn)
        return conn


def test_helpers_noop_on_non_postgres():
    engine = SimpleNamespace(dialect=SimpleNamespace(name="sqlite"))
    db_utils.ensure_vector_extension(engine)
    db_utils.create_doc_chunk_indexes(engine)


def test_helpers_idempotent_and_safe():
    engine = _RecordingEngine()
    db_utils.ensure_vector_extension(engine)
    db_utils.ensure_vector_extension(engine)
    db_utils.create_doc_chunk_indexes(engine)
    db_utils.create_doc_chunk_indexes(engine)

    executed = []
    for conn in engine.calls:
        executed.extend(conn.calls)

    assert any("CREATE EXTENSION IF NOT EXISTS vector" in stmt for stmt in executed)
    assert any("CREATE INDEX IF NOT EXISTS doc_chunks_embedding_ivfflat" in stmt for stmt in executed)
