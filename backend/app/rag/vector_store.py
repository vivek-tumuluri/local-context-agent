from __future__ import annotations

import os
from types import ModuleType
from typing import Any


def _load_backend() -> ModuleType:
    backend = os.getenv("VECTOR_BACKEND", "chroma").lower()
    if backend == "pgvector":
        from . import vector_pg as impl
    else:
        from . import vector as impl
        backend = "chroma"
    impl.BACKEND_NAME = backend  # type: ignore[attr-defined]
    return impl


BACKEND: ModuleType = _load_backend()
BACKEND_NAME: str = getattr(BACKEND, "BACKEND_NAME", os.getenv("VECTOR_BACKEND", "chroma").lower())


def reload_backend() -> None:
    global BACKEND, BACKEND_NAME
    BACKEND = _load_backend()
    BACKEND_NAME = getattr(BACKEND, "BACKEND_NAME", os.getenv("VECTOR_BACKEND", "chroma").lower())


def __getattr__(name: str) -> Any:
    return getattr(BACKEND, name)


def __dir__() -> list[str]:
    combined = set(globals().keys()) | set(dir(BACKEND))
    return sorted(combined)
