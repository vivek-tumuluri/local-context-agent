from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Optional

from fastapi import HTTPException, status

from redis import Redis, from_url
from redis.exceptions import RedisError

from app.core.logging_utils import log_event

log = logging.getLogger("limits")


_redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
_redis: Optional[Redis] = None


def _redis_conn() -> Optional[Redis]:
    global _redis
    if _redis is not None:
        return _redis
    try:
        client = from_url(_redis_url)
        client.ping()
        _redis = client
        return _redis
    except Exception as exc:
        log.warning("[limits] Redis unavailable; skipping quota enforcement", exc_info=True)
        log_event(
            "quota_backend_unavailable",
            backend="redis",
            level="warning",
            error=str(exc),
        )
        return None


MAX_INGESTS_PER_DAY = int(os.getenv("MAX_INGESTS_PER_USER_PER_DAY", "3"))
MAX_RAG_REQUESTS_PER_DAY = int(os.getenv("MAX_RAG_REQUESTS_PER_DAY", "200"))


def _today() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


def check_ingest_quota(user_id: str) -> None:
    if MAX_INGESTS_PER_DAY <= 0:
        return
    redis = _redis_conn()
    if not redis:
        return
    key = f"quota:ingest:drive:{user_id}:{_today()}"
    try:
        count = redis.incr(key)
        if count == 1:
            redis.expire(key, 48 * 3600)
        if count > MAX_INGESTS_PER_DAY:
            log_event(
                "quota_denied",
                user_id=user_id,
                quota="ingest_drive_daily",
                count=int(count),
                limit=MAX_INGESTS_PER_DAY,
                level="warning",
            )
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Daily Drive ingest limit reached. Try again tomorrow."
            )
    except RedisError as exc:
        log.warning("[limits] Redis error enforcing ingest quota", exc_info=True)
        log_event(
            "quota_backend_error",
            user_id=user_id,
            quota="ingest_drive_daily",
            error=str(exc),
            level="warning",
        )


def check_rag_quota(user_id: str) -> None:
    if MAX_RAG_REQUESTS_PER_DAY <= 0:
        return
    redis = _redis_conn()
    if not redis:
        return
    key = f"quota:rag_answer:day:{user_id}:{_today()}"
    try:
        count = redis.incr(key)
        if count == 1:
            redis.expire(key, 48 * 3600)
        if count > MAX_RAG_REQUESTS_PER_DAY:
            log_event(
                "quota_denied",
                user_id=user_id,
                quota="rag_answer_daily",
                count=int(count),
                limit=MAX_RAG_REQUESTS_PER_DAY,
                level="warning",
            )
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Daily RAG request limit reached. Try again tomorrow."
            )
    except RedisError as exc:
        log.warning("[limits] Redis error enforcing rag quota", exc_info=True)
        log_event(
            "quota_backend_error",
            user_id=user_id,
            quota="rag_answer_daily",
            error=str(exc),
            level="warning",
        )
