from __future__ import annotations

import argparse
import statistics
import time
from pathlib import Path
from typing import Dict, List, Sequence
import sys

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.ingest.text_normalize import normalize_text
from tests.perf_utils import StageTiming, load_json, rank_documents, summarize_timings


def simulate_pipeline(query: Dict, topk: int) -> Dict[str, object]:
    timings: List[StageTiming] = []

    start = time.perf_counter()
    normalized_query = normalize_text(query["query"])
    for _ in range(500):
        normalized_query.encode("utf-8")
    timings.append(StageTiming("embed", (time.perf_counter() - start) * 1000))

    start = time.perf_counter()
    docs = [{"id": f"{i}", "text": text} for i, text in enumerate(query.get("context", []))]
    ranked = rank_documents(normalized_query, docs, topk)
    timings.append(StageTiming("retrieval", (time.perf_counter() - start) * 1000))

    start = time.perf_counter()
    answer = " ".join(doc["text"][:80] for doc in ranked)
    timings.append(StageTiming("answer", (time.perf_counter() - start) * 1000))

    ttft_ms = sum(t.duration_ms for t in timings if t.stage in {"embed", "retrieval"})
    total_ms = ttft_ms + next((t.duration_ms for t in timings if t.stage == "answer"), 0.0)

    return {
        "timings": timings,
        "ttft_ms": ttft_ms,
        "total_ms": total_ms,
        "answer_preview": answer[:120],
    }


def profile_latency(queries_path: str | None, topk: int) -> Dict[str, object]:
    data = load_json(queries_path, fallback="queries.json")
    per_query = [simulate_pipeline(entry, topk) for entry in data]

    ttft_vals = [entry["ttft_ms"] for entry in per_query]
    total_vals = [entry["total_ms"] for entry in per_query]
    stage_stats = summarize_timings(stage for entry in per_query for stage in entry["timings"])

    return {
        "queries": len(per_query),
        "avg_ttft_ms": statistics.mean(ttft_vals) if ttft_vals else 0.0,
        "avg_total_ms": statistics.mean(total_vals) if total_vals else 0.0,
        "stage_stats": stage_stats,
    }


def cli(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Synthetic latency profiler for the RAG pipeline.")
    parser.add_argument("--queries", type=str, default=None, help="Path to queries JSON file.")
    parser.add_argument("--topk", type=int, default=4, help="How many context chunks to simulate.")
    args = parser.parse_args(argv)

    summary = profile_latency(args.queries, args.topk)
    print("Latency Profile")
    print("===============")
    print(f"Queries processed : {summary['queries']}")
    print(f"Avg TTFT (ms)     : {summary['avg_ttft_ms']:.2f}")
    print(f"Avg Total (ms)    : {summary['avg_total_ms']:.2f}")
    for stage, stats in summary["stage_stats"].items():
        print(f"  - {stage}: avg={stats['avg_ms']:.2f}ms p95={stats['p95_ms']:.2f}ms")
    return 0


if __name__ == "__main__":
    import sys

    raise SystemExit(cli())
