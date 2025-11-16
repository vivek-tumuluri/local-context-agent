from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine


def ensure_vector_extension(engine: Engine) -> None:
    if engine.dialect.name != "postgresql":
        return
    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))


def create_doc_chunk_indexes(engine: Engine) -> None:
    if engine.dialect.name != "postgresql":
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS doc_chunks_embedding_ivfflat "
                "ON doc_chunks USING ivfflat (embedding vector_l2_ops) WITH (lists = 100)"
            )
        )
