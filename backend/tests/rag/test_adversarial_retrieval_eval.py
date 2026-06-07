from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set

import pytest

from app.rag import vector_store as vector
from app.rag.retrieval import _merge_candidates, hybrid_query


DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "rag_adversarial_eval_cases.json"
REQUIRED_CATEGORIES = {
    "calendar_event_title",
    "document_id",
    "exact_file_title",
    "person_name",
    "project_name",
    "quoted_phrase",
    "short_query",
    "vague_lexically_precise",
    "weird_acronym",
}


def _load_cases() -> List[Dict[str, Any]]:
    with DATA_PATH.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _expected_doc_ids(case: Dict[str, Any]) -> Set[str]:
    return {str(doc_id) for doc_id in case.get("expected_doc_ids") or []}


def _doc_rank(results: Sequence[Dict[str, Any]], expected_doc_ids: Set[str]) -> Optional[int]:
    for idx, hit in enumerate(results, start=1):
        meta = hit.get("meta") or {}
        doc_id = str(meta.get("doc_id") or meta.get("id") or "")
        if doc_id in expected_doc_ids:
            return idx
    return None


def _seed_case(case: Dict[str, Any], user_id: str) -> None:
    chunks = []
    docs = sorted(case["documents"], key=lambda doc: bool(doc.get("relevant")))
    for idx, doc in enumerate(docs):
        chunks.append(
            {
                "id": f"{case['id']}::{doc['id']}::{idx}",
                "text": doc["text"],
                "meta": {
                    "doc_id": doc["id"],
                    "source": doc["source"],
                    "title": doc["title"],
                },
            }
        )
    vector.upsert(chunks, user_id=user_id)


def _compare_case(case: Dict[str, Any], user_id: str, k: int = 5) -> Dict[str, Any]:
    _seed_case(case, user_id=user_id)
    source_filter = case.get("source_filter")
    expected = _expected_doc_ids(case)
    vector_hits = vector.query(case["query"], user_id=user_id, k=k, source=source_filter)
    hybrid_hits = hybrid_query(case["query"], user_id=user_id, k=k, source=source_filter)
    vector_rank = _doc_rank(vector_hits, expected)
    hybrid_rank = _doc_rank(hybrid_hits, expected)
    return {
        "case_id": case["id"],
        "category": case["category"],
        "query": case["query"],
        "expected_doc_ids": sorted(expected),
        "vector_rank": vector_rank,
        "hybrid_rank": hybrid_rank,
        "rank_delta": (0 if vector_rank is None else vector_rank) - (0 if hybrid_rank is None else hybrid_rank),
        "vector_top_ids": [(hit.get("meta") or {}).get("doc_id") for hit in vector_hits],
        "hybrid_top_ids": [(hit.get("meta") or {}).get("doc_id") for hit in hybrid_hits],
    }


def _hit_at_1(rank: Optional[int]) -> float:
    return 1.0 if rank == 1 else 0.0


def _hit_at_3(rank: Optional[int]) -> float:
    return 1.0 if rank is not None and rank <= 3 else 0.0


def _mrr_at_5(rank: Optional[int]) -> float:
    return 1.0 / rank if rank is not None and rank <= 5 else 0.0


@pytest.fixture(scope="module")
def adversarial_cases() -> List[Dict[str, Any]]:
    return _load_cases()


def test_adversarial_eval_dataset_schema(adversarial_cases):
    assert len(adversarial_cases) >= 20
    seen_ids: Set[str] = set()
    for case in adversarial_cases:
        assert case.get("id")
        assert case["id"] not in seen_ids
        seen_ids.add(case["id"])
        assert case.get("query")
        assert case.get("category") in REQUIRED_CATEGORIES
        assert case.get("expected_behavior") == "answerable"
        assert isinstance(case.get("expected_doc_ids"), list)
        assert case["expected_doc_ids"]
        assert case.get("expected_source") in {"drive", "calendar"}
        assert isinstance(case.get("expected_hybrid_rank_max"), int)
        assert case["expected_hybrid_rank_max"] >= 1
        assert isinstance(case.get("documents"), list)
        assert len(case["documents"]) >= 3

        doc_ids = {doc.get("id") for doc in case["documents"]}
        assert len(doc_ids) == len(case["documents"])
        assert set(case["expected_doc_ids"]).issubset(doc_ids)
        assert sum(1 for doc in case["documents"] if doc.get("relevant")) == 1
        for doc in case["documents"]:
            assert doc.get("source") in {"drive", "calendar"}
            assert doc.get("title")
            assert doc.get("text")
            assert isinstance(doc.get("relevant"), bool)


def test_adversarial_eval_category_coverage(adversarial_cases):
    categories = {case["category"] for case in adversarial_cases}
    assert REQUIRED_CATEGORIES <= categories
    assert any(case.get("source_filter") == "calendar" for case in adversarial_cases)
    assert any(case["expected_source"] == "drive" for case in adversarial_cases)
    assert any(case["expected_source"] == "calendar" for case in adversarial_cases)


def test_hybrid_beats_vector_on_adversarial_identity_queries(fake_vector_env, adversarial_cases):
    metrics = [
        _compare_case(case, user_id=f"adv-eval-{idx}", k=5)
        for idx, case in enumerate(adversarial_cases)
    ]

    hybrid_hit_1 = sum(_hit_at_1(m["hybrid_rank"]) for m in metrics) / len(metrics)
    hybrid_hit_3 = sum(_hit_at_3(m["hybrid_rank"]) for m in metrics) / len(metrics)
    hybrid_mrr = sum(_mrr_at_5(m["hybrid_rank"]) for m in metrics) / len(metrics)
    vector_mrr = sum(_mrr_at_5(m["vector_rank"]) for m in metrics) / len(metrics)
    improved = [
        m
        for m in metrics
        if m["hybrid_rank"] is not None
        and (m["vector_rank"] is None or m["hybrid_rank"] < m["vector_rank"])
    ]
    regressions = [
        m
        for m in metrics
        if m["vector_rank"] is not None
        and (m["hybrid_rank"] is None or m["hybrid_rank"] > m["vector_rank"])
    ]
    misses = [m for m in metrics if m["hybrid_rank"] is None or m["hybrid_rank"] > 3]

    assert hybrid_hit_1 >= 0.85, metrics
    assert hybrid_hit_3 >= 0.95, metrics
    assert hybrid_mrr >= vector_mrr + 0.15, metrics
    assert len(improved) >= int(len(metrics) * 0.4), metrics
    assert not regressions
    assert not misses


@pytest.mark.parametrize(
    "query,expected_id,vector_hits,lexical_score",
    [
        (
            "QBR",
            "qbr-0",
            [
                {"id": "revenue-0", "text": "Revenue review and renewal blockers.", "meta": {"doc_id": "revenue"}},
                {"id": "qbr-0", "text": "Expansion risks and executive asks.", "meta": {"doc_id": "qbr", "title": "QBR Packet"}},
            ],
            1.0,
        ),
        (
            "Professor Hwang",
            "hwang-0",
            [
                {"id": "paper-0", "text": "Ablation table and retrieval grounding.", "meta": {"doc_id": "paper"}},
                {"id": "hwang-0", "text": "Advisor requested evaluation cleanup.", "meta": {"doc_id": "hwang", "title": "Professor Hwang Notes"}},
            ],
            1.0,
        ),
        (
            "DOC-9F3A",
            "doc-9f3a-0",
            [
                {"id": "rotation-0", "text": "Credential rotation and OAuth scopes.", "meta": {"doc_id": "rotation"}},
                {"id": "doc-9f3a-0", "text": "Refresh token reconnect appendix.", "meta": {"doc_id": "DOC-9F3A"}},
            ],
            1.0,
        ),
    ],
)
def test_hybrid_merge_rescues_lexical_identity_matches(query, expected_id, vector_hits, lexical_score):
    lexical_hits = [
        {
            "id": expected_id,
            "text": "Lexical identity match.",
            "meta": {"doc_id": expected_id, "title": query},
            "lexical_score": lexical_score,
        }
    ]

    merged = _merge_candidates(vector_hits, lexical_hits)

    assert merged[0]["id"] == expected_id
    assert merged[0]["lexical_rank"] == 1
    assert merged[0]["hybrid_score"] > merged[1]["hybrid_score"]
