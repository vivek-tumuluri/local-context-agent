from __future__ import annotations

from pathlib import Path

import pytest

from tests import benchmark_retrieval as bench
from tests.perf_utils import load_json


DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "rag_eval_cases.json"
REQUIRED_CATEGORIES = {
    "calendar",
    "conflict",
    "date_drive",
    "distractors",
    "drive_semantic",
    "exact_phrase",
    "exact_title",
    "mixed",
    "multi_doc",
    "owner_person",
    "source_specific",
    "unknown",
}


@pytest.fixture(scope="module")
def eval_cases() -> list[dict]:
    return load_json(str(DATA_PATH), fallback="rag_eval_cases.json")


def test_rag_eval_dataset_schema(eval_cases):
    assert len(eval_cases) >= 25
    seen_ids: set[str] = set()
    for case in eval_cases:
        assert case.get("id")
        assert case["id"] not in seen_ids
        seen_ids.add(case["id"])
        assert case.get("query")
        assert case.get("category")
        assert case.get("expected_behavior") in {"answerable", "unknown"}
        assert isinstance(case.get("documents"), list)
        assert len(case["documents"]) >= 2
        assert isinstance(case.get("expected_doc_ids"), list)

        doc_ids = {doc.get("id") for doc in case["documents"]}
        assert None not in doc_ids
        assert len(doc_ids) == len(case["documents"])
        for doc in case["documents"]:
            assert doc.get("source") in {"drive", "calendar"}
            assert doc.get("title")
            assert doc.get("text")
            assert isinstance(doc.get("relevant"), bool)

        if case["expected_behavior"] == "answerable":
            assert case["expected_doc_ids"]
            assert any(doc.get("relevant") for doc in case["documents"])
            assert set(case["expected_doc_ids"]).issubset(doc_ids)
            assert case.get("expected_source") in {"drive", "calendar"}
        else:
            assert not case["expected_doc_ids"]
            assert not any(doc.get("relevant") for doc in case["documents"])


def test_rag_eval_dataset_category_coverage(eval_cases):
    categories = {case["category"] for case in eval_cases}
    missing = REQUIRED_CATEGORIES - categories
    assert not missing
    assert any(case.get("source_filter") == "calendar" for case in eval_cases)
    assert any(case.get("source_filter") == "drive" for case in eval_cases)


def test_retrieval_metric_helpers():
    ranked = [
        {"id": "a", "source": "drive"},
        {"id": "b", "source": "calendar"},
        {"id": "c", "source": "drive"},
    ]
    relevant = {"b"}
    assert bench.hit_at_k(ranked, relevant, 1) == 0.0
    assert bench.hit_at_k(ranked, relevant, 2) == 1.0
    assert bench.reciprocal_rank(ranked, relevant, 5) == 0.5
    assert bench.ndcg_at_k(ranked, relevant, 3) > 0.0
    assert bench.expected_doc_rank(ranked, relevant) == 2
    assert bench.source_accuracy_at_k(ranked, "calendar", 2) == 1.0
    assert bench.wrong_source_rate_at_k(ranked, "drive", 3) == pytest.approx(1 / 3)


def test_rag_eval_offline_baseline_thresholds(eval_cases):
    metrics = [bench.evaluate_entry(case, topk=10) for case in eval_cases]
    summary = bench.summarize(metrics)
    assert summary["cases"] == len(eval_cases)
    assert summary["unknown_cases"] >= 2

    # These conservative thresholds describe the current lexical smoke benchmark.
    # They are intentionally not aspirational; Phase 1+ should raise them.
    assert summary["hit_at_5"] >= 0.55
    assert summary["mrr_at_5"] >= 0.25
    assert summary["ndcg_at_10"] >= 0.35
    assert summary["source_accuracy_at_5"] >= 0.80
