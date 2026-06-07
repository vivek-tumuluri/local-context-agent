from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Sequence

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.core.db import SessionLocal
from app.core.models import User
from app.rag import vector_store as vector
from app.rag.retrieval import hybrid_query, lexical_query
from app.routes import rag_routes


def _default_user_id() -> str:
    session = SessionLocal()
    try:
        user = session.query(User).order_by(User.created_at.desc()).first()
        if not user:
            raise SystemExit("No users found. Pass --user-id after logging in or ingesting data.")
        return str(user.id)
    finally:
        session.close()


def _summarize_hits(hits: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for idx, hit in enumerate(hits, 1):
        meta = hit.get("meta") or {}
        text = rag_routes._strip_retrieval_prefix(str(hit.get("text") or "")).replace("\n", " ").strip()
        out.append(
            {
                "rank": idx,
                "id": hit.get("id"),
                "doc_id": meta.get("doc_id") or meta.get("id"),
                "title": meta.get("title") or meta.get("name"),
                "source": meta.get("source"),
                "confidence": hit.get("confidence", rag_routes._hit_confidence(hit)),
                "hybrid_score": hit.get("hybrid_score"),
                "vector_rank": hit.get("vector_rank"),
                "lexical_rank": hit.get("lexical_rank"),
                "lexical_score": hit.get("lexical_score"),
                "preview": text[:300],
            }
        )
    return out


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Debug vector, lexical, hybrid, and final RAG answer context for one query.")
    parser.add_argument("--query", required=True, help="Question or search query to inspect.")
    parser.add_argument("--user-id", default=None, help="User UUID. Defaults to the most recently created user.")
    parser.add_argument("--source", default=None, choices=["drive", "calendar"], help="Optional source filter.")
    parser.add_argument("--k", type=int, default=6, help="Final answer hit count.")
    parser.add_argument("--max-ctx-chars", type=int, default=8000, help="Packed context character budget.")
    args = parser.parse_args(argv)

    user_id = args.user_id or _default_user_id()
    initial_k = rag_routes._candidate_k_for_answer(args.k)
    vector_hits = vector.query(args.query, user_id=user_id, k=initial_k, source=args.source)
    lexical_hits = lexical_query(args.query, user_id=user_id, k=initial_k, source=args.source)
    hybrid_hits = hybrid_query(args.query, user_id=user_id, k=initial_k, source=args.source)
    final_hits = rag_routes._filter_hits(hybrid_hits, args.source)
    final_hits = rag_routes._filter_low_quality(final_hits)
    final_hits = rag_routes._annotate_hit_confidence(final_hits)
    final_hits = rag_routes._rerank_hits(
        final_hits,
        top_k=min(rag_routes.RERANK_TOP_K, len(final_hits)),
        diversity_weight=rag_routes.RERANK_DIVERSITY_WEIGHT,
        query=args.query,
    )
    final_hits = rag_routes._cap_per_doc(final_hits, max_per_doc=rag_routes.RAG_MAX_CHUNKS_PER_DOC)[: args.k]
    context = rag_routes._pack_context(final_hits, args.max_ctx_chars)

    payload = {
        "query": args.query,
        "user_id": user_id,
        "source": args.source,
        "initial_k": initial_k,
        "vector": _summarize_hits(vector_hits),
        "lexical": _summarize_hits(lexical_hits),
        "hybrid": _summarize_hits(hybrid_hits),
        "final_answer_hits": _summarize_hits(final_hits),
        "context_chars": len(context),
        "context_preview": context[:2000],
    }
    print(json.dumps(payload, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
