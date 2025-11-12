from __future__ import annotations

from app.rag import routes as rag_routes


def test_format_sources_uses_titles():
    hits = [
        {"meta": {"title": "Launch Plan", "source": "drive", "doc_id": "doc1"}, "confidence": 0.9},
        {"meta": {"title": "Retro Notes", "source": "drive", "doc_id": "doc2"}, "confidence": 0.8},
    ]
    formatted = rag_routes._format_sources(hits)
    titles = [item["title"] for item in formatted]
    assert titles == ["Launch Plan", "Retro Notes"]
    assert formatted[0]["link"].endswith("/doc1/view")


def test_pack_context_truncates_when_needed():
    hits = [
        {"text": "A" * 4000, "meta": {"title": "DocA", "source": "drive"}},
        {"text": "B" * 4000, "meta": {"title": "DocB", "source": "drive"}},
    ]
    ctx = rag_routes._pack_context(hits, max_chars=4500)
    assert "DocA" in ctx
    assert "…[truncated]" in ctx


def test_confidence_falls_back_to_similarity():
    hits = [{"similarity": 0.5}, {"distance": 0.2}]
    conf = rag_routes._confidence(hits)
    assert 0 < conf < 1
