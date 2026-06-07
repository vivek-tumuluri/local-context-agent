from __future__ import annotations

from app.rag import vector_store as vector
from app.rag.retrieval import _merge_candidates, analyze_query, hybrid_query, lexical_query, lexical_score
from app.routes import rag_routes


def test_analyze_query_extracts_identity_features():
    features = analyze_query('"rollback window closes" DOC-9F3A QBR')

    assert features.quoted_phrases == ["rollback window closes"]
    assert "doc9f3a" in features.identifier_candidates
    assert "qbr" in features.acronyms


def test_lexical_score_prioritizes_exact_title_and_doc_id():
    title_score = lexical_score(
        "Security FAQ",
        "Credential storage uses encrypted OAuth tokens.",
        {"doc_id": "security-faq-copy", "source": "drive", "title": "Security FAQ"},
    )
    body_score = lexical_score(
        "Security FAQ",
        "Security notes mention FAQ cleanup and credential storage.",
        {"doc_id": "security-notes", "source": "drive", "title": "Credential Notes"},
    )
    id_score = lexical_score(
        "DOC-9F3A",
        "Credential rotation appendix.",
        {"doc_id": "DOC-9F3A", "source": "drive", "title": "Credential Rotation"},
    )

    assert title_score > body_score
    assert id_score >= title_score


def test_lexical_score_handles_quoted_phrases_and_short_acronyms():
    phrase_score = lexical_score(
        '"rollback window closes"',
        "The rollback window closes after support confirms no priority regressions.",
        {"doc_id": "release-runbook", "source": "drive", "title": "Release Runbook"},
    )
    partial_score = lexical_score(
        '"rollback window closes"',
        "The release window includes rollback owners and support checks.",
        {"doc_id": "release-checklist", "source": "drive", "title": "Release Checklist"},
    )
    acronym_score = lexical_score(
        "QBR",
        "Expansion risks and renewal blockers.",
        {"doc_id": "qbr-packet", "source": "drive", "title": "QBR Packet"},
    )

    assert phrase_score > partial_score
    assert acronym_score > 0.5


def test_lexical_score_applies_source_intent_as_boost_not_filter():
    calendar_score = lexical_score(
        "security review meeting",
        "Review encrypted credentials and OAuth scope approval.",
        {"doc_id": "cal-security-review", "source": "calendar", "title": "Security Review"},
    )
    drive_score = lexical_score(
        "security review meeting",
        "Review encrypted credentials and OAuth scope approval.",
        {"doc_id": "drive-security-review", "source": "drive", "title": "Security Review"},
    )
    drive_plan_score = lexical_score(
        "security review plan",
        "Review encrypted credentials and OAuth scope approval.",
        {"doc_id": "drive-security-plan", "source": "drive", "title": "Security Review"},
    )

    assert calendar_score > drive_score
    assert drive_plan_score > drive_score


def test_hybrid_merge_does_not_let_stopword_overlap_beat_vector_evidence():
    query = "what is the plan for day 1 in montreal"
    vector_hits = [
        {
            "id": "montreal-day-1",
            "text": "Day 1: Arrival plus Chinatown and free evening in Montreal.",
            "meta": {"doc_id": "montreal", "source": "drive", "title": "Montreal Itinerary.docx"},
            "similarity": 0.53,
        }
    ]
    lexical_hits = [
        {
            "id": "generic-plan",
            "text": "What is the plan for monitoring whether farmers follow the process?",
            "meta": {"doc_id": "generic", "source": "drive", "title": "Weekly Connect Notes"},
            "lexical_score": lexical_score(query, "What is the plan for monitoring whether farmers follow the process?", {"title": "Weekly Connect Notes"}),
        }
    ]

    hits = _merge_candidates(vector_hits, lexical_hits, query=query)

    assert lexical_hits[0]["lexical_score"] < 0.2
    assert hits[0]["id"] == "montreal-day-1"


def test_lexical_query_finds_exact_title(fake_vector_env):
    user_id = "lexical-title-user"
    vector.upsert(
        [
            {
                "id": "lexical-title-0",
                "text": "Escalation contacts and on-call handoff notes live here.",
                "meta": {"doc_id": "lexical-title", "source": "drive", "title": "Security FAQ"},
            },
            {
                "id": "lexical-other-0",
                "text": "Security review notes mention vendor questionnaires.",
                "meta": {"doc_id": "lexical-other", "source": "drive", "title": "Vendor Review"},
            },
        ],
        user_id=user_id,
    )

    hits = lexical_query("Security FAQ", user_id=user_id, k=5)

    assert hits
    assert hits[0]["id"] == "lexical-title-0"
    assert hits[0]["meta"]["doc_id"] == "lexical-title"
    assert hits[0]["lexical_score"] > 0


def test_lexical_query_respects_source(fake_vector_env):
    user_id = "lexical-source-user"
    vector.upsert(
        [
            {
                "id": "lexical-source-drive-0",
                "text": "Launch review is documented in the drive memo.",
                "meta": {"doc_id": "lexical-source-drive", "source": "drive", "title": "Launch Review"},
            },
            {
                "id": "lexical-source-calendar-0",
                "text": "Launch review happens at 10 AM with Priya.",
                "meta": {"doc_id": "lexical-source-calendar", "source": "calendar", "title": "Launch Review"},
            },
        ],
        user_id=user_id,
    )

    hits = lexical_query("Launch Review", user_id=user_id, k=5, source="calendar")

    assert hits
    assert {hit["meta"]["source"] for hit in hits} == {"calendar"}
    assert {hit["meta"]["doc_id"] for hit in hits} == {"lexical-source-calendar"}


def test_hybrid_query_deduplicates_candidates(fake_vector_env):
    user_id = "hybrid-dedupe-user"
    vector.upsert(
        [
            {
                "id": "hybrid-dedupe-0",
                "text": "Security FAQ covers access review, onboarding, and incident contacts.",
                "meta": {"doc_id": "hybrid-dedupe", "source": "drive", "title": "Security FAQ"},
            }
        ],
        user_id=user_id,
    )

    hits = hybrid_query("Security FAQ", user_id=user_id, k=5)

    assert hits
    assert len({hit["id"] for hit in hits}) == len(hits)
    assert hits[0]["id"] == "hybrid-dedupe-0"
    assert "hybrid_score" in hits[0]


def test_merge_prefers_title_match_when_vector_weak():
    vector_hits = [
        {
            "id": "generic-0",
            "text": "Generic status update with nearby embedding.",
            "meta": {"doc_id": "generic", "source": "drive", "title": "Weekly Update"},
            "similarity": 0.9,
        },
        {
            "id": "title-0",
            "text": "Access review details and escalation notes.",
            "meta": {"doc_id": "title", "source": "drive", "title": "Security FAQ"},
            "similarity": 0.1,
        },
    ]
    lexical_hits = [
        {
            "id": "title-0",
            "text": "Access review details and escalation notes.",
            "meta": {"doc_id": "title", "source": "drive", "title": "Security FAQ"},
            "lexical_score": 1.0,
        }
    ]

    hits = _merge_candidates(vector_hits, lexical_hits)

    assert hits[0]["id"] == "title-0"
    assert len({hit["id"] for hit in hits}) == len(hits)


def test_hybrid_query_returns_current_hit_shape(fake_vector_env):
    user_id = "hybrid-shape-user"
    vector.upsert(
        [
            {
                "id": "hybrid-shape-0",
                "text": "Calendar planning session includes launch review and QA readiness.",
                "meta": {"doc_id": "hybrid-shape", "source": "calendar", "title": "Launch Planning"},
            }
        ],
        user_id=user_id,
    )

    hits = hybrid_query("launch review", user_id=user_id, k=3, source="calendar")

    assert hits
    hit = hits[0]
    assert {"id", "text", "meta", "hybrid_score"}.issubset(hit)
    assert hit["meta"]["source"] == "calendar"
    assert hit["meta"]["doc_id"] == "hybrid-shape"


def test_route_retrieval_falls_back_to_vector_query(monkeypatch):
    fallback_hits = [
        {
            "id": "fallback-0",
            "text": "Fallback vector result.",
            "meta": {"doc_id": "fallback", "source": "drive", "title": "Fallback"},
            "similarity": 0.8,
        }
    ]

    monkeypatch.setattr(rag_routes, "hybrid_query", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr(rag_routes, "vec_query", lambda *args, **kwargs: list(fallback_hits))

    hits = rag_routes._retrieve_hits("fallback", k=1, user_id="route-fallback-user", source="drive")

    assert hits == fallback_hits
