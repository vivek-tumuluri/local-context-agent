from __future__ import annotations

import argparse
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Sequence
import sys

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.ingest.chunking import split_by_chars
from app.ingest.text_normalize import compute_content_hash, normalize_text
from tests.perf_utils import StageTiming, summarize_timings


def _fake_text(seed: int, length: int = 1500) -> str:
    random.seed(seed)
    words = [
        "launch",
        "plan",
        "telemetry",
        "customer",
        "update",
        "QA",
        "freeze",
        "doc",
        "note",
        "owner",
        "email",
        "deadline",
    ]
    out = []
    for _ in range(length // 12):
        size = random.randint(4, 10)
        chunk = " ".join(random.choices(words, k=size))
        out.append(chunk.capitalize() + ".")
    return " ".join(out)


def _process_file(idx: int) -> Dict[str, object]:
    text = _fake_text(idx)
    timings: List[StageTiming] = []

    start = time.perf_counter()
    normalized = normalize_text(text)
    timings.append(StageTiming("normalize", (time.perf_counter() - start) * 1000))

    start = time.perf_counter()
    split_by_chars(normalized)
    timings.append(StageTiming("chunk", (time.perf_counter() - start) * 1000))

    start = time.perf_counter()
    compute_content_hash(normalized)
    timings.append(StageTiming("hash", (time.perf_counter() - start) * 1000))

    total_ms = sum(t.duration_ms for t in timings)
    return {"idx": idx, "timings": timings, "total_ms": total_ms}


def run_load_test(n_files: int, concurrency: int) -> Dict[str, object]:
    futures = []
    all_timings: List[StageTiming] = []
    totals: List[float] = []

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        for i in range(n_files):
            futures.append(pool.submit(_process_file, i))

        for fut in as_completed(futures):
            result = fut.result()
            all_timings.extend(result["timings"])
            totals.append(result["total_ms"])

    summary = {
        "files": n_files,
        "avg_ms_per_file": sum(totals) / len(totals),
        "p95_ms_per_file": sorted(totals)[max(0, int(len(totals) * 0.95) - 1)] if totals else 0.0,
        "stage_stats": summarize_timings(all_timings),
    }
    return summary


def cli(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Synthetic Drive ingestion load test.")
    parser.add_argument("--n_files", type=int, default=100, help="Number of synthetic files to process.")
    parser.add_argument("--concurrency", type=int, default=4, help="Parallel worker count.")
    args = parser.parse_args(argv)

    summary = run_load_test(args.n_files, args.concurrency)
    print("Ingestion Load Test")
    print("===================")
    print(f"Files processed : {summary['files']}")
    print(f"Avg ms/file     : {summary['avg_ms_per_file']:.2f}")
    print(f"p95 ms/file     : {summary['p95_ms_per_file']:.2f}")
    print("Stage stats:")
    for stage, stats in summary["stage_stats"].items():
        print(f"  - {stage}: avg={stats['avg_ms']:.2f}ms p95={stats['p95_ms']:.2f}ms count={stats['count']}")
    return 0


def test_ingest_load_smoke():
    summary = run_load_test(n_files=5, concurrency=2)
    assert summary["files"] == 5
    assert summary["avg_ms_per_file"] >= 0.0


if __name__ == "__main__":
    import sys

    raise SystemExit(cli())
