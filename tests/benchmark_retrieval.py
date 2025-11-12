from __future__ import annotations

import argparse
import math
import statistics
import sys
from pathlib import Path
from typing import Dict, List, Sequence, Set

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


def evaluate_entry(entry: Dict, topk: int) -> Dict[str, float]:
    docs = entry["documents"]
    relevant_ids = {doc["id"] for doc in docs if doc.get("relevant")}
    ranked = rank_documents(entry["query"], docs, topk=max(topk, 10))
    return {
        "hit_at_5": hit_at_k(ranked, relevant_ids, 5),
        "mrr_at_5": reciprocal_rank(ranked, relevant_ids, 5),
        "ndcg_at_10": ndcg_at_k(ranked, relevant_ids, 10),
    }


def summarize(metrics: List[Dict[str, float]]) -> Dict[str, float]:
    summary: Dict[str, float] = {}
    if not metrics:
        return summary
    keys = metrics[0].keys()
    for key in keys:
        summary[key] = statistics.mean(m[key] for m in metrics)
    return summary


def report(summary: Dict[str, float]) -> None:
    if not summary:
        print("No data to report.")
        return
    print("Retrieval Benchmark Results")
    print("===========================")
    print(f"Hit@5 : {summary['hit_at_5']:.3f}")
    print(f"MRR@5 : {summary['mrr_at_5']:.3f}")
    print(f"nDCG@10 : {summary['ndcg_at_10']:.3f}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Offline retrieval benchmark harness.")
    parser.add_argument("--golden", type=str, default=None, help="Path to golden JSON file.")
    parser.add_argument("--topk", type=int, default=10, help="Candidate cut-off for ranking.")
    args = parser.parse_args(argv)

    data = load_json(args.golden, fallback="golden_set.json")
    results = [evaluate_entry(entry, topk=args.topk) for entry in data]
    summary = summarize(results)
    report(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
