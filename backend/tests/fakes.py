from __future__ import annotations

import hashlib
from collections import deque
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

