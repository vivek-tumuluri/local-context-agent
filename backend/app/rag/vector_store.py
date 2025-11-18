from __future__ import annotations

from __future__ import annotations

from typing import Any

from . import vector_pg as BACKEND

BACKEND_NAME: str = "pgvector"


def __getattr__(name: str) -> Any:
    return getattr(BACKEND, name)


def __dir__() -> list[str]:
    combined = set(globals().keys()) | set(dir(BACKEND))
    return sorted(combined)
