from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.auth import csrf_protect, get_current_user
from app.core.db import get_db
from app.ingest.calendar_ingest import get_upcoming_events
from app.rag.vector_store import query as vec_query

router = APIRouter(prefix="/relevant", tags=["relevant"])


def _confidence(hit: Dict[str, Any]) -> Optional[float]:
    if isinstance(hit.get("confidence"), (int, float)):
        return float(hit["confidence"])
    if isinstance(hit.get("similarity"), (int, float)):
        return float(hit["similarity"])
    dist = hit.get("distance")
    if isinstance(dist, (int, float)):
        return 1.0 / (1.0 + max(0.0, float(dist)))
    return None


def _normalize_doc(hit: Dict[str, Any]) -> Dict[str, Any]:
    meta = hit.get("meta", {}) or {}
    text = (hit.get("text") or "").strip()
    return {
        "doc_id": meta.get("doc_id") or meta.get("id"),
        "title": meta.get("title") or meta.get("name") or "(untitled)",
        "snippet": text[:240],
        "confidence": _confidence(hit),
        "source": (meta.get("source") or meta.get("src") or "unknown"),
    }


def _format_event_query(event: Dict[str, Any]) -> str:
    title = event.get("title") or ""
    desc = event.get("description") or ""
    start = event.get("start") or ""
    end = event.get("end") or ""
    return f"{title}\n{desc}\nStart: {start}\nEnd: {end}"


@router.get("/now")
def relevant_now(
    hours: int = 24,
    per_event: int = 3,
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
    _csrf=Depends(csrf_protect),
):
    events = get_upcoming_events(db, user.user_id, hours=hours)

    results: List[Dict[str, Any]] = []
    for event in events:
        query = _format_event_query(event)
        hits = vec_query(query, k=max(1, per_event), user_id=user.user_id)
        hits = [h for h in hits if (h.get("meta", {}) or {}).get("source", "").lower() == "drive"]
        docs = [_normalize_doc(h) for h in hits[:per_event]]
        results.append({"event": event, "docs": docs})

    count = sum(1 for r in results if r.get("docs"))
    return {"results": results, "count": count}
