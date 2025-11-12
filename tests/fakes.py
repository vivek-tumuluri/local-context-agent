from __future__ import annotations

import hashlib
from collections import deque
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Deque, Dict, Iterable, List, Optional, Sequence, Tuple


_KEYWORDS = [
    "launch",
    "qa",
    "customer",
    "telemetry",
    "retro",
    "timeline",
    "security",
    "faq",
]


def _text_to_vector(text: str) -> List[float]:
    tokens = text.lower().split()
    features = [tokens.count(word) for word in _KEYWORDS]
    if len(features) < 8:
        digest = hashlib.sha256(text.encode("utf-8")).digest()
        features.extend(int(b) / 255.0 for b in digest[: 8 - len(features)])
    return [float(val) for val in features[:8]]


class FakeEmbeddingsClient:
    def __init__(self) -> None:
        self.failures: Deque[Exception] = deque()
        self.calls: List[Dict[str, Any]] = []

    def queue_failure(self, exc: Exception) -> None:
        self.failures.append(exc)

    def create(self, input: Sequence[str], model: str) -> Any:
        self.calls.append({"input": list(input), "model": model})
        if self.failures:
            raise self.failures.popleft()
        vectors = [_text_to_vector(text.strip()) for text in input]
        return SimpleNamespace(data=[SimpleNamespace(embedding=v) for v in vectors])


class FakeChatCompletions:
    def __init__(self) -> None:
        self.responses: Deque[str] = deque()
        self.failures: Deque[Exception] = deque()
        self.calls: List[Dict[str, Any]] = []

    def queue_response(self, text: str) -> None:
        self.responses.append(text)

    def queue_failure(self, exc: Exception) -> None:
        self.failures.append(exc)

    def create(self, *, model: str, messages: Sequence[Dict[str, str]], temperature: float) -> Any:
        self.calls.append({"model": model, "messages": list(messages), "temperature": temperature})
        if self.failures:
            raise self.failures.popleft()
        content = self.responses.popleft() if self.responses else "stub-response"
        msg = SimpleNamespace(content=content)
        choice = SimpleNamespace(message=msg)
        return SimpleNamespace(choices=[choice])


@dataclass
class _VectorRow:
    id: str
    text: str
    meta: Dict[str, Any]
    embedding: List[float]


class FakeCollection:
    def __init__(self) -> None:
        self.rows: Dict[str, _VectorRow] = {}

    def upsert(
        self,
        *,
        ids: Sequence[str],
        documents: Sequence[str],
        metadatas: Sequence[Dict[str, Any]],
        embeddings: Sequence[Sequence[float]],
    ) -> None:
        for idx, doc, meta, emb in zip(ids, documents, metadatas, embeddings):
            self.rows[idx] = _VectorRow(id=idx, text=doc, meta=dict(meta), embedding=list(emb))

    def query(self, *, query_embeddings: Sequence[Sequence[float]], n_results: int, include: Iterable[str]) -> Dict[str, List[List[Any]]]:
        query_vec = list(query_embeddings[0])
        scored: List[Tuple[float, _VectorRow]] = []
        for row in self.rows.values():
            dist = sum((a - b) ** 2 for a, b in zip(query_vec, row.embedding))
            scored.append((dist, row))
        scored.sort(key=lambda tup: tup[0])
        top = scored[:n_results]
        documents = [[row.text for _, row in top]]
        metadatas = [[row.meta for _, row in top]]
        ids = [[row.id for _, row in top]]
        distances = [[dist for dist, _ in top]]
        return {"documents": documents, "metadatas": metadatas, "ids": ids, "distances": distances}

    def get(self, where: Optional[Dict[str, Any]] = None) -> Dict[str, List[str]]:
        if where and "doc_id" in where:
            doc_id = where["doc_id"]
            ids = [row.id for row in self.rows.values() if row.meta.get("doc_id") == doc_id]
        else:
            ids = list(self.rows.keys())
        return {"ids": ids}

    def delete(self, ids: Sequence[str]) -> None:
        for idx in ids:
            self.rows.pop(idx, None)

    def count(self) -> int:
        return len(self.rows)


class FakeChromaClient:
    def __init__(self) -> None:
        self.collections: Dict[str, FakeCollection] = {}

    def get_or_create_collection(self, key: str) -> FakeCollection:
        if key not in self.collections:
            self.collections[key] = FakeCollection()
        return self.collections[key]

    def delete_collection(self, key: str) -> None:
        self.collections.pop(key, None)

    def persist(self) -> None:
        return None
