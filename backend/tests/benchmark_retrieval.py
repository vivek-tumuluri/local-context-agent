from __future__ import annotations

import argparse
import math
import statistics
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from tests.perf_utils import load_json, rank_documents


def hit_at_k(ranked: Sequence[dict], relevant: Set[str], k: int) -> float:
    cutoff = ranked[:k]
    return 1.0 if any(item["id"] in relevant for item in cutoff) else 0.0


def reciprocal_rank(ranked: Sequence[dict], relevant: Set[str], k: int) -> float:
    for idx, item in enumerate(ranked[:k], start=1):
        if item["id"] in relevant:
            return 1.0 / idx
    return 0.0


def ndcg_at_k(ranked: Sequence[dict], relevant: Set[str], k: int) -> float:
    def rel_gain(item_id: str) -> int:
        return 1 if item_id in relevant else 0

    dcg = 0.0
    for idx, item in enumerate(ranked[:k], start=1):
        gain = rel_gain(item["id"])
        if gain == 0:
            continue
        dcg += (2**gain - 1) / (math.log2(idx + 1))

    ideal_order = sorted(relevant, key=lambda _: 1, reverse=True)[:k]
    idcg = 0.0
    for idx, _ in enumerate(ideal_order, start=1):
        idcg += (2**1 - 1) / (math.log2(idx + 1))
    if idcg == 0:
        return 0.0
    return dcg / idcg


def relevant_ids_for(entry: Dict[str, Any]) -> Set[str]:
    explicit = entry.get("expected_doc_ids")
    if isinstance(explicit, list):
        return {str(doc_id) for doc_id in explicit}
    return {str(doc["id"]) for doc in entry["documents"] if doc.get("relevant")}


def candidate_docs_for(entry: Dict[str, Any]) -> List[dict]:
    docs = list(entry["documents"])
    source_filter = entry.get("source_filter")
    if source_filter:
        target = str(source_filter).strip().lower()
        docs = [doc for doc in docs if str(doc.get("source") or "").strip().lower() == target]
    return docs


def expected_doc_rank(ranked: Sequence[dict], relevant: Set[str]) -> Optional[int]:
    for idx, item in enumerate(ranked, start=1):
        if str(item["id"]) in relevant:
            return idx
    return None


def source_accuracy_at_k(ranked: Sequence[dict], expected_source: Optional[str], k: int) -> Optional[float]:
    if not expected_source:
        return None
    target = str(expected_source).strip().lower()
    return 1.0 if any(str(item.get("source") or "").strip().lower() == target for item in ranked[:k]) else 0.0


def wrong_source_rate_at_k(ranked: Sequence[dict], source_filter: Optional[str], k: int) -> Optional[float]:
    if not source_filter:
        return None
    cutoff = ranked[:k]
    if not cutoff:
        return 0.0
    target = str(source_filter).strip().lower()
    wrong = sum(1 for item in cutoff if str(item.get("source") or "").strip().lower() != target)
    return wrong / len(cutoff)


def is_unknown_case(entry: Dict[str, Any]) -> bool:
    return str(entry.get("expected_behavior") or "").strip().lower() == "unknown"


def evaluate_entry(entry: Dict[str, Any], topk: int) -> Dict[str, Any]:
    docs = candidate_docs_for(entry)
    relevant_ids = relevant_ids_for(entry)
    ranked = rank_documents(entry["query"], docs, topk=max(topk, 10))
    rank = expected_doc_rank(ranked, relevant_ids)
    return {
        "case_id": entry.get("id"),
        "category": entry.get("category"),
        "query": entry.get("query"),
        "unknown": is_unknown_case(entry),
        "expected_doc_ids": sorted(relevant_ids),
        "expected_source": entry.get("expected_source"),
        "source_filter": entry.get("source_filter"),
        "top_ids": [item["id"] for item in ranked[:5]],
        "top_sources": [item.get("source") for item in ranked[:5]],
        "expected_doc_rank": rank,
        "hit_at_1": hit_at_k(ranked, relevant_ids, 1),
        "hit_at_3": hit_at_k(ranked, relevant_ids, 3),
        "hit_at_5": hit_at_k(ranked, relevant_ids, 5),
        "mrr_at_5": reciprocal_rank(ranked, relevant_ids, 5),
        "ndcg_at_10": ndcg_at_k(ranked, relevant_ids, 10),
        "source_accuracy_at_5": source_accuracy_at_k(ranked, entry.get("expected_source"), 5),
        "wrong_source_rate_at_5": wrong_source_rate_at_k(ranked, entry.get("source_filter"), 5),
    }


def summarize(metrics: List[Dict[str, Any]]) -> Dict[str, float]:
    summary: Dict[str, float] = {
        "cases": float(len(metrics)),
        "unknown_cases": float(sum(1 for m in metrics if m.get("unknown"))),
    }
    if not metrics:
        return summary

    answerable = [m for m in metrics if not m.get("unknown")]
    if not answerable:
        return summary

    for key in ("hit_at_1", "hit_at_3", "hit_at_5", "mrr_at_5", "ndcg_at_10"):
        summary[key] = statistics.mean(float(m[key]) for m in answerable)

    source_vals = [
        float(m["source_accuracy_at_5"])
        for m in answerable
        if m.get("source_accuracy_at_5") is not None
    ]
    if source_vals:
        summary["source_accuracy_at_5"] = statistics.mean(source_vals)

    wrong_source_vals = [
        float(m["wrong_source_rate_at_5"])
        for m in answerable
        if m.get("wrong_source_rate_at_5") is not None
    ]
    if wrong_source_vals:
        summary["wrong_source_rate_at_5"] = statistics.mean(wrong_source_vals)
    return summary


def missed_cases(metrics: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        m
        for m in metrics
        if not m.get("unknown") and not bool(m.get("hit_at_5"))
    ]


def report(summary: Dict[str, float], metrics: Sequence[Dict[str, Any]]) -> None:
    if not summary:
        print("No data to report.")
        return
    print("Retrieval Benchmark Results")
    print("===========================")
    print(f"Cases : {int(summary.get('cases', 0))}")
    print(f"Unknown cases : {int(summary.get('unknown_cases', 0))}")
    print(f"Hit@1 : {summary.get('hit_at_1', 0.0):.3f}")
    print(f"Hit@3 : {summary.get('hit_at_3', 0.0):.3f}")
    print(f"Hit@5 : {summary.get('hit_at_5', 0.0):.3f}")
    print(f"MRR@5 : {summary.get('mrr_at_5', 0.0):.3f}")
    print(f"nDCG@10 : {summary.get('ndcg_at_10', 0.0):.3f}")
    if "source_accuracy_at_5" in summary:
        print(f"Source Accuracy@5 : {summary['source_accuracy_at_5']:.3f}")
    if "wrong_source_rate_at_5" in summary:
        print(f"Wrong Source Rate@5 : {summary['wrong_source_rate_at_5']:.3f}")

    misses = missed_cases(metrics)
    if misses:
        print()
        print("Missed Cases")
        print("------------")
        for item in misses:
            expected = ", ".join(item.get("expected_doc_ids") or [])
            got = ", ".join(
                f"{src}:{doc_id}"
                for src, doc_id in zip(item.get("top_sources") or [], item.get("top_ids") or [])
            )
            print(f"- {item.get('case_id')}: expected {expected}; got {got}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Offline retrieval benchmark harness.")
    parser.add_argument("--golden", type=str, default=None, help="Path to golden JSON file.")
    parser.add_argument("--topk", type=int, default=10, help="Candidate cut-off for ranking.")
    args = parser.parse_args(argv)

    data = load_json(args.golden, fallback="golden_set.json")
    results = [evaluate_entry(entry, topk=args.topk) for entry in data]
    summary = summarize(results)
    report(summary, results)
    return 0


if __name__ == "__main__":
    sys.exit(main())
