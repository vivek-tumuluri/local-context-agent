from __future__ import annotations

from app.routes import rag_routes


def test_format_sources_uses_titles():
    hits = [
        {"meta": {"title": "Launch Plan", "source": "drive", "doc_id": "doc1"}, "confidence": 0.9},
        {"meta": {"title": "Retro Notes", "source": "drive", "doc_id": "doc2"}, "confidence": 0.8},
    ]
    formatted = rag_routes._format_sources(hits)
    titles = [item["title"] for item in formatted]
    assert titles == ["Launch Plan", "Retro Notes"]
    assert formatted[0]["link"].endswith("/doc1/view")


def test_format_sources_preserves_trace_metadata():
    hits = [
        {
            "meta": {
                "title": "Security Review",
                "source": "calendar",
                "doc_id": "cal-1",
                "link": "https://calendar.example/event",
                "start": "2026-06-10T15:00:00Z",
                "end": "2026-06-10T16:00:00Z",
                "updated": "2026-06-01T12:00:00Z",
                "organizer": "mina@example.com",
            },
            "confidence": 0.9,
        }
    ]

    formatted = rag_routes._format_sources(hits)

    assert formatted[0]["start"] == "2026-06-10T15:00:00Z"
    assert formatted[0]["end"] == "2026-06-10T16:00:00Z"
    assert formatted[0]["updated_at"] == "2026-06-01T12:00:00Z"
    assert formatted[0]["organizer"] == "mina@example.com"


def test_pack_context_truncates_when_needed():
    hits = [
        {"text": "A" * 4000, "meta": {"title": "DocA", "source": "drive"}},
        {"text": "B" * 4000, "meta": {"title": "DocB", "source": "drive"}},
    ]
    ctx = rag_routes._pack_context(hits, max_chars=4500)
    assert "DocA" in ctx
    assert "…[truncated]" in ctx


def test_pack_context_includes_source_trace_metadata():
    hits = [
        {
            "text": "Description: Review OAuth scopes and credential storage.",
            "meta": {
                "title": "Security Review",
                "source": "calendar",
                "doc_id": "cal-security",
                "start": "2026-06-10T15:00:00Z",
                "end": "2026-06-10T16:00:00Z",
                "link": "https://calendar.example/event",
            },
        }
    ]

    ctx = rag_routes._pack_context(hits, max_chars=2000)

    assert "[1]" in ctx
    assert "Source: calendar" in ctx
    assert "Title: Security Review" in ctx
    assert "Doc ID: cal-security" in ctx
    assert "Start: 2026-06-10T15:00:00Z" in ctx
    assert "Evidence:\nDescription: Review OAuth scopes" in ctx


def test_pack_context_strips_duplicate_retrieval_prefix():
    hits = [
        {
            "text": "Title: Launch Plan\nSource: drive\nDocument ID: launch-plan\nMIME: text/plain\n\nQA freeze is May 2.",
            "meta": {"title": "Launch Plan", "source": "drive", "doc_id": "launch-plan"},
        }
    ]

    ctx = rag_routes._pack_context(hits, max_chars=2000)

    assert ctx.count("Title: Launch Plan") == 1
    assert ctx.count("Source: drive") == 1
    assert "Evidence:\nQA freeze is May 2." in ctx


def test_citation_helpers_detect_and_strip_invalid_indices():
    answer = "QA freeze is May 2 [1]. Security review is June 10 [9]."

    assert rag_routes._extract_citation_indices(answer) == {1, 9}
    assert rag_routes._invalid_citation_indices(answer, source_count=2) == {9}
    assert rag_routes._strip_invalid_citations(answer, source_count=2) == "QA freeze is May 2 [1]. Security review is June 10."


def test_answer_prompt_requires_claim_level_grounding():
    prompt = rag_routes._answer_prompt("[1]\nText:\nQA freeze is May 2.", "When is QA freeze?", allow_partial=False)

    assert "If relevant evidence exists" in prompt
    assert "Every factual claim must include inline citations" in prompt
    assert "Cite only blocks that directly contain the supporting evidence" in prompt
    assert "If sources conflict" in prompt
    assert 'reply exactly: "I don’t know based on the synced data."' in prompt


def test_confidence_falls_back_to_similarity():
    hits = [{"similarity": 0.5}, {"distance": 0.2}]
    conf = rag_routes._confidence(hits)
    assert 0 < conf < 1


def test_rerank_keeps_best_chunk_from_repeated_doc():
    hits = [
        {"text": "DigiSwasthya overview", "similarity": 0.18, "meta": {"title": "DigiSwasthya Pitch", "doc_id": "pitch"}},
        {"text": "DigiSwasthya process", "similarity": 0.16, "meta": {"title": "DigiSwasthya Pitch", "doc_id": "pitch"}},
        {"text": "DigiSwasthya team", "similarity": -0.05, "meta": {"title": "DigiSwasthya Pitch", "doc_id": "pitch"}},
        {"text": "Unrelated farm assistant", "similarity": -0.16, "meta": {"title": "Digital Agronomist", "doc_id": "farm"}},
    ]

    ranked = rag_routes._rerank_hits(hits, top_k=4, diversity_weight=0.1, query="What is DigiSwasthya?")

    assert ranked[0]["meta"]["doc_id"] == "pitch"
    assert "overview" in ranked[0]["text"]


def test_rerank_boosts_title_and_doc_id_matches():
    hits = [
        {
            "text": "Generic launch content with some semantic overlap.",
            "similarity": 0.18,
            "meta": {"title": "Generic Plan", "doc_id": "generic-plan"},
        },
        {
            "text": "Sparse body text.",
            "similarity": 0.1,
            "meta": {"title": "Customer Update Plan", "doc_id": "customer_update_plan"},
        },
    ]

    ranked = rag_routes._rerank_hits(
        hits,
        top_k=2,
        diversity_weight=0.1,
        query="Customer Update Plan",
    )

    assert ranked[0]["meta"]["doc_id"] == "customer_update_plan"
