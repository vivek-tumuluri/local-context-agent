from __future__ import annotations

import json
import math
import os
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Sequence


def _this_dir() -> Path:
    return Path(__file__).resolve().parent


def load_json(path: str | None, *, fallback: str) -> list:
    """
    Load a JSON file, falling back to tests/data/<fallback> when path is None.
    """
    if path:
        target = Path(path)
    else:
        target = _this_dir() / "data" / fallback
    if not target.exists():
        raise FileNotFoundError(f"File not found: {target}")
    with target.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def tokenize(text: str) -> List[str]:
    return [tok for tok in text.lower().split() if tok]


def lexical_similarity(query: str, text: str) -> float:
    """
    Simple token overlap score in [0, 1].
    """
    q_tokens = set(tokenize(query))
    if not q_tokens:
        return 0.0
    t_tokens = set(tokenize(text))
    if not t_tokens:
        return 0.0
    overlap = len(q_tokens & t_tokens)
    return overlap / len(q_tokens | t_tokens)


def rank_documents(query: str, docs: Sequence[dict], topk: int) -> List[dict]:
    scored = []
    for doc in docs:
        score = lexical_similarity(query, doc.get("text", ""))
        scored.append({**doc, "score": score})
    scored.sort(key=lambda d: d["score"], reverse=True)
    return scored[:topk]


def percentile(values: Sequence[float], pct: float) -> float:
    if not values:
        return 0.0
    pct = max(0.0, min(100.0, pct))
    idx = (len(values) - 1) * pct / 100.0
    lower = math.floor(idx)
    upper = math.ceil(idx)
    if lower == upper:
        return float(values[int(idx)])
    lower_val = values[lower]
    upper_val = values[upper]
    return float(lower_val + (upper_val - lower_val) * (idx - lower))


@dataclass
class StageTiming:
    stage: str
    duration_ms: float


def summarize_timings(records: Iterable[StageTiming]) -> dict:
    out: dict[str, dict[str, float]] = {}
    grouped: dict[str, List[float]] = {}
    for rec in records:
        grouped.setdefault(rec.stage, []).append(rec.duration_ms)
    for stage, vals in grouped.items():
        out[stage] = {
            "avg_ms": statistics.mean(vals),
            "p95_ms": percentile(sorted(vals), 95),
            "count": len(vals),
        }
    return out
