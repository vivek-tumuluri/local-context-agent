from __future__ import annotations

import logging
import time
from contextlib import ContextDecorator
from typing import Any, Dict, Optional

_log = logging.getLogger("perf.stage")


class StageTimer(ContextDecorator):
    """
    Lightweight context manager for measuring ingest / query stages.
    Usage:

        with StageTimer("embed", user_id=user.id):
            embed_chunks(...)
    """

    def __init__(self, stage: str, **tags: Any) -> None:
        if not stage:
            raise ValueError("stage is required")
        self.stage = stage
        self.tags: Dict[str, Any] = {k: v for k, v in tags.items() if v is not None}
        self._start: Optional[float] = None

    def __enter__(self) -> "StageTimer":
        self._start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        elapsed = 0.0
        if self._start is not None:
            elapsed = (time.perf_counter() - self._start) * 1000
        payload = {"stage": self.stage, "elapsed_ms": round(elapsed, 4), **self.tags}
        if exc is not None:
            payload["error"] = str(exc)
            _log.error("perf.stage %s", payload)
            return False
        _log.info("perf.stage %s", payload)
        return False

    def log(self, message: str, **extra: Any) -> None:
        """Emit an ad-hoc structured log tied to this stage."""
        payload = {"stage": self.stage, **self.tags, **extra, "message": message}
        _log.info("perf.stage %s", payload)
