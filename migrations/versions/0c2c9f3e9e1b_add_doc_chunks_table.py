"""add doc_chunks table

Revision ID: 0c2c9f3e9e1b
Revises: 31341363563c
Create Date: 2026-01-01 00:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from pgvector.sqlalchemy import Vector

from app.rag.embedding_config import EMBED_DIM


# revision identifiers, used by Alembic.
revision: str = "0c2c9f3e9e1b"
down_revision: Union[str, Sequence[str], None] = "31341363563c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _ensure_vector_extension() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute(sa.text("CREATE EXTENSION IF NOT EXISTS vector"))


def upgrade() -> None:
    """Add doc_chunks table and vector index."""
    _ensure_vector_extension()

    op.create_table(
        "doc_chunks",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("user_id", sa.String(), nullable=False, index=True),
        sa.Column("doc_id", sa.String(), nullable=False, index=True),
        sa.Column("source", sa.String(), nullable=False, server_default="drive"),
        sa.Column("title", sa.String(), nullable=True),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("embedding", Vector(dim=EMBED_DIM), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_doc_chunks_user_doc",
        "doc_chunks",
        ["user_id", "doc_id"],
        unique=False,
    )
    op.create_index(
        "doc_chunks_embedding_ivfflat",
        "doc_chunks",
        ["embedding"],
        unique=False,
        postgresql_using="ivfflat",
        postgresql_ops={"embedding": "vector_l2_ops"},
        postgresql_with={"lists": "100"},
    )


def downgrade() -> None:
    """Drop doc_chunks table and index."""
    op.drop_index("doc_chunks_embedding_ivfflat", table_name="doc_chunks")
    op.drop_index("ix_doc_chunks_user_doc", table_name="doc_chunks")
    op.drop_table("doc_chunks")
