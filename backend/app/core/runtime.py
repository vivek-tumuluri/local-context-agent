from __future__ import annotations

from fastapi import HTTPException, status

from app.core.settings import READ_ONLY_MODE


def ensure_writes_enabled() -> None:
    if READ_ONLY_MODE:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Write operations are temporarily disabled while the service is read-only.",
        )
