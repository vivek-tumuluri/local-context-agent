import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(message)s")
LOGGER = logging.getLogger("app")


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def log_event(event: str, *, level: str = "info", **fields: Any) -> None:
    payload: Dict[str, Any] = {
        "event": event,
        "ts": _timestamp(),
        "level": level.lower(),
    }
    for key, value in fields.items():
        if value is not None:
            payload[key] = value
    LOGGER.log(getattr(logging, level.upper(), logging.INFO), json.dumps(payload, separators=(",", ":")))
