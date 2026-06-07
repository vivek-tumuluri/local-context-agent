from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Set

from sqlalchemy import select

from app.core import db as app_db
from app.core.models import DocChunk
from app.rag import vector_store as vector

DEFAULT_LEXICAL_SCAN_LIMIT = 1000
RRF_K = 60

_TOKEN_RE = re.compile(r"[a-z0-9_]+")
_QUOTED_PHRASE_RE = re.compile(r'"([^"]+)"')
_DOC_ID_RE = re.compile(r"\b[a-z]{2,}[-_][a-z0-9][a-z0-9_-]*\b", re.IGNORECASE)

_CALENDAR_INTENT_TERMS = {
    "calendar",
    "event",
    "events",
    "meeting",
    "meetings",
    "office",
    "hours",
    "sync",
    "when",
    "schedule",
}
_DRIVE_INTENT_TERMS = {
    "doc",
    "docs",
    "document",
    "documents",
    "file",
    "files",
    "faq",
    "memo",
    "notes",
    "plan",
    "runbook",
}
_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "has",
    "have",
    "how",
    "i",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "this",
    "to",
    "was",
    "what",
    "when",
    "where",
    "who",
    "why",
    "with",
}


@dataclass(frozen=True)
class QueryFeatures:
    raw: str
    normalized: str
    tokens: List[str]
    quoted_phrases: List[str]
    identifier_candidates: List[str]
    acronyms: List[str]
    short_query: bool
    calendar_intent: bool
    drive_intent: bool


def tokenize_query(query: Optional[str]) -> List[str]:
    if not query:
        return []
    return _TOKEN_RE.findall(query.lower())


def _normalize_space(text: str) -> str:
    return " ".join(tokenize_query(text))


def _normalize_identifier(text: str) -> str:
    return "".join(ch for ch in text.lower() if ch.isalnum())


def _contains_token(tokens: Set[str], value: str) -> bool:
    return value.lower() in tokens


def analyze_query(query: Optional[str]) -> QueryFeatures:
    raw = query or ""
    tokens = tokenize_query(raw)
    token_set = set(tokens)
    quoted = [_normalize_space(match) for match in _QUOTED_PHRASE_RE.findall(raw)]
    quoted = [phrase for phrase in quoted if phrase]
    identifiers = [_normalize_identifier(match) for match in _DOC_ID_RE.findall(raw)]
    acronyms = [
        token.lower()
        for token in re.findall(r"\b[A-Z][A-Z0-9]{1,}\b", raw)
        if token.lower() not in token_set or len(token) <= 5
    ]
    normalized = " ".join(tokens)
    return QueryFeatures(
        raw=raw,
        normalized=normalized,
        tokens=tokens,
        quoted_phrases=quoted,
        identifier_candidates=[item for item in identifiers if item],
        acronyms=acronyms,
        short_query=0 < len(tokens) <= 2,
        calendar_intent=any(_contains_token(token_set, term) for term in _CALENDAR_INTENT_TERMS),
        drive_intent=any(_contains_token(token_set, term) for term in _DRIVE_INTENT_TERMS),
    )


def _field_text(meta: Dict[str, Any], hit_text: str) -> Dict[str, str]:
    return {
        "title": _normalize_space(str(meta.get("title") or meta.get("name") or "")),
        "doc_id": _normalize_space(str(meta.get("doc_id") or meta.get("id") or "")),
        "doc_id_compact": _normalize_identifier(str(meta.get("doc_id") or meta.get("id") or "")),
        "source": str(meta.get("source") or "").lower(),
        "text": _normalize_space(str(hit_text or "")),
    }


def _content_tokens(tokens: Sequence[str]) -> List[str]:
    content = [tok for tok in tokens if tok and (tok not in _STOPWORDS or tok.isdigit())]
    return content or [tok for tok in tokens if tok]


def _token_overlap_score(tokens: Sequence[str], field: str, per_hit: float, cap: float) -> float:
    if not tokens or not field:
        return 0.0
    field_tokens = set(tokenize_query(field))
    hits = sum(1 for tok in tokens if tok and tok in field_tokens)
    return min(cap, per_hit * hits)


def _source_intent_boost(features: QueryFeatures, source: str) -> float:
    if source == "calendar" and features.calendar_intent:
        return 0.08
    if source == "drive" and features.drive_intent:
        return 0.06
    return 0.0


def lexical_score(query: str, text: str, meta: Optional[Dict[str, Any]] = None) -> float:
    features = analyze_query(query)
    if not features.tokens:
        return 0.0
    meta = meta or {}
    fields = _field_text(meta, text)
    query_norm = features.normalized
    query_compact = _normalize_identifier(query)
    content_tokens = _content_tokens(features.tokens)

    score = 0.0
    if query_norm and fields["title"] == query_norm:
        score += 0.75
    elif query_norm and fields["title"] and query_norm in fields["title"]:
        score += 0.55

    if query_compact and fields["doc_id_compact"] == query_compact:
        score += 0.90
    elif query_compact and fields["doc_id_compact"] and query_compact in fields["doc_id_compact"]:
        score += 0.60

    if query_norm and fields["text"] and query_norm in fields["text"]:
        score += 0.35

    for phrase in features.quoted_phrases:
        if phrase and phrase in fields["title"]:
            score += 0.65
        elif phrase and phrase in fields["text"]:
            score += 0.70

    title_tokens = set(tokenize_query(fields["title"]))
    doc_id_tokens = set(tokenize_query(fields["doc_id"]))
    text_tokens = set(tokenize_query(fields["text"]))
    for acronym in features.acronyms:
        if acronym in title_tokens:
            score += 0.45
        elif acronym in doc_id_tokens:
            score += 0.50
        elif acronym in text_tokens:
            score += 0.25

    score += _token_overlap_score(content_tokens, fields["title"], 0.08, 0.32)
    score += _token_overlap_score(content_tokens, fields["doc_id"], 0.07, 0.28)
    score += _token_overlap_score(content_tokens, fields["text"], 0.025, 0.18)
    if features.short_query and fields["title"]:
        score += min(0.18, _token_overlap_score(content_tokens, fields["title"], 0.09, 0.18))
    score += _source_intent_boost(features, fields["source"])
    return min(1.0, score)


def _row_to_hit(row: Any, score: float) -> Dict[str, Any]:
    meta = dict(row.chunk_metadata or {})
    if row.source and not meta.get("source"):
        meta["source"] = row.source
    if row.title and not meta.get("title"):
        meta["title"] = row.title
    if row.doc_id and not meta.get("doc_id"):
        meta["doc_id"] = row.doc_id
    return {
        "id": row.id,
        "text": row.text,
        "meta": meta,
        "lexical_score": score,
    }


def lexical_query(
    q: str,
    user_id: str,
    k: int = 20,
    source: Optional[str] = None,
    scan_limit: int = DEFAULT_LEXICAL_SCAN_LIMIT,
) -> List[Dict[str, Any]]:
    if not user_id:
        raise ValueError("user_id is required for lexical retrieval")
    source_filter = source.strip().lower() if source else None
    session = app_db.SessionLocal()
    try:
        filters = [DocChunk.user_id == user_id]
        if source_filter:
            filters.append(DocChunk.source == source_filter)
        stmt = (
            select(
                DocChunk.id,
                DocChunk.doc_id,
                DocChunk.source,
                DocChunk.title,
                DocChunk.text,
                DocChunk.chunk_metadata,
            )
            .where(*filters)
            .limit(max(k, scan_limit))
        )
        rows = session.execute(stmt).all()
    finally:
        session.close()

    scored: List[Dict[str, Any]] = []
    for row in rows:
        meta = dict(row.chunk_metadata or {})
        if row.title and not meta.get("title"):
            meta["title"] = row.title
        if row.doc_id and not meta.get("doc_id"):
            meta["doc_id"] = row.doc_id
        if row.source and not meta.get("source"):
            meta["source"] = row.source
        score = lexical_score(q, row.text or "", meta)
        if score <= 0:
            continue
        scored.append(_row_to_hit(row, score))
    scored.sort(key=lambda hit: (-float(hit.get("lexical_score") or 0.0), str(hit.get("id") or "")))
    return scored[:k]


def _rank_score(rank: int) -> float:
    return 1.0 / (RRF_K + rank)


def _merge_candidates(
    vector_hits: Sequence[Dict[str, Any]],
    lexical_hits: Sequence[Dict[str, Any]],
    query: Optional[str] = None,
) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    order = 0
    features = analyze_query(query) if query else None

    for rank, hit in enumerate(vector_hits, start=1):
        key = str(hit.get("id") or f"vector-{order}")
        order += 1
        item = dict(hit)
        item["_order"] = order
        item["vector_rank"] = rank
        item["hybrid_score"] = float(item.get("hybrid_score") or 0.0) + _rank_score(rank)
        merged[key] = item

    for rank, hit in enumerate(lexical_hits, start=1):
        key = str(hit.get("id") or f"lexical-{order}")
        order += 1
        item = merged.get(key)
        if item is None:
            item = dict(hit)
            item["_order"] = order
            item["hybrid_score"] = 0.0
            merged[key] = item
        else:
            item.setdefault("lexical_score", hit.get("lexical_score"))
            if not item.get("text") and hit.get("text"):
                item["text"] = hit["text"]
            existing_meta = dict(item.get("meta") or {})
            existing_meta.update({k: v for k, v in (hit.get("meta") or {}).items() if v is not None})
            item["meta"] = existing_meta
        item["lexical_rank"] = rank
        item["hybrid_score"] = float(item.get("hybrid_score") or 0.0) + _rank_score(rank)
        item["hybrid_score"] += min(0.25, float(item.get("lexical_score") or 0.0) * 0.25)

    if features:
        for item in merged.values():
            meta = item.get("meta") or {}
            rich_score = lexical_score(features.raw, str(item.get("text") or ""), meta)
            item["hybrid_score"] = float(item.get("hybrid_score") or 0.0) + min(0.35, rich_score * 0.30)

    out = list(merged.values())
    out.sort(key=lambda hit: (-float(hit.get("hybrid_score") or 0.0), int(hit.get("_order") or 0)))
    for hit in out:
        hit.pop("_order", None)
    return out


def hybrid_query(
    q: str,
    user_id: str,
    k: int = 10,
    source: Optional[str] = None,
    vector_k: Optional[int] = None,
    lexical_k: Optional[int] = None,
) -> List[Dict[str, Any]]:
    vector_limit = max(1, vector_k or k)
    lexical_limit = max(1, lexical_k or k)
    vector_hits = vector.query(q, user_id=user_id, k=vector_limit, source=source)
    lexical_hits = lexical_query(q, user_id=user_id, k=lexical_limit, source=source)
    return _merge_candidates(vector_hits, lexical_hits, query=q)[:k]
