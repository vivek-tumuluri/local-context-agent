from __future__ import annotations

from app.rag.chunk import build_retrieval_prefix


def test_build_retrieval_prefix_skips_empty_fields():
    prefix = build_retrieval_prefix(
        [
            ("Title", "Launch Plan"),
            ("Source", "drive"),
            ("MIME", ""),
            ("Status", None),
        ]
    )

    assert prefix == "Title: Launch Plan\nSource: drive"


def test_build_retrieval_prefix_applies_length_cap():
    prefix = build_retrieval_prefix([("Title", "A" * 100)], max_chars=20)

    assert len(prefix) == 20
    assert prefix.startswith("Title: ")
