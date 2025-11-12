from __future__ import annotations

import json
import math
from pathlib import Path

from app.rag import vector

DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "golden_set.json"


def _load_dataset():
    with DATA_PATH.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _ingest_all_docs(user_id: str, dataset):
    text_by_doc = {}
    for entry in dataset:
        for doc in entry["documents"]:
            text_by_doc.setdefault(doc["id"], doc["text"])
    chunks = []
    for idx, (doc_id, text) in enumerate(sorted(text_by_doc.items())):
        chunks.append(
            {
                "id": f"{doc_id}-{idx}",
                "text": text,
                "meta": {"doc_id": doc_id, "source": "drive", "title": doc_id},
            }
        )
    vector.upsert(chunks, user_id=user_id)


def _metric_scores(results, relevant_ids):
    hits = [hit["meta"].get("doc_id") for hit in results]
    hit = any(doc_id in relevant_ids for doc_id in hits[:5])
    rank = next((i + 1 for i, doc_id in enumerate(hits[:5]) if doc_id in relevant_ids), None)
    mrr = 1.0 / rank if rank else 0.0

    gains = [1.0 if doc_id in relevant_ids else 0.0 for doc_id in hits[:10]]
    dcg = sum(g / math.log2(idx + 2) for idx, g in enumerate(gains))
    ideal = sorted([1.0] * len(relevant_ids) + [0.0] * (10 - len(relevant_ids)), reverse=True)[:10]
    idcg = sum(g / math.log2(idx + 2) for idx, g in enumerate(ideal)) or 1.0
    ndcg = dcg / idcg
    return hit, mrr, ndcg


def test_golden_retrieval_metrics(fake_vector_env):
    dataset = _load_dataset()
    user_id = "golden-user"
    _ingest_all_docs(user_id, dataset)

    hits = []
    mrrs = []
    ndcgs = []
    for entry in dataset:
        relevant = [doc["id"] for doc in entry["documents"] if doc.get("relevant")]
        results = vector.query(entry["query"], k=5, user_id=user_id)
        hit, mrr, ndcg = _metric_scores(results, relevant)
        hits.append(1.0 if hit else 0.0)
        mrrs.append(mrr)
        ndcgs.append(ndcg)

    hit_at_5 = sum(hits) / len(hits)
    mean_mrr = sum(mrrs) / len(mrrs)
    mean_ndcg = sum(ndcgs) / len(ndcgs)

    assert hit_at_5 >= 0.66
    assert mean_mrr >= 0.4
    assert mean_ndcg >= 0.5
