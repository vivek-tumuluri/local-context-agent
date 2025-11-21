from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


def _to_bool(val: Optional[str], default: bool = False) -> bool:
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class Settings:
    env: str = field(
        default_factory=lambda: os.getenv("ENV")
        or os.getenv("ENVIRONMENT")
        or os.getenv("APP_ENV")
        or "local"
    )
    database_url: str = field(default_factory=lambda: os.getenv("DATABASE_URL", ""))
    redis_url: str = field(default_factory=lambda: os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    google_client_id: str = field(default_factory=lambda: os.getenv("GOOGLE_CLIENT_ID", ""))
    google_client_secret: str = field(default_factory=lambda: os.getenv("GOOGLE_CLIENT_SECRET", ""))
    oauth_redirect_uri: str = field(default_factory=lambda: os.getenv("OAUTH_REDIRECT_URI", ""))
    openai_api_key: str = field(default_factory=lambda: os.getenv("OPENAI_API_KEY", ""))
    session_secret: str = field(default_factory=lambda: os.getenv("SESSION_SECRET", ""))
    drive_credentials_key: str = field(default_factory=lambda: os.getenv("DRIVE_CREDENTIALS_KEY") or os.getenv("FERNET_KEY", ""))

    # Feature / mode flags
    read_only_mode: bool = field(default_factory=lambda: _to_bool(os.getenv("READ_ONLY_MODE"), False))
    allow_inline_ingest: bool = field(default_factory=lambda: _to_bool(os.getenv("ALLOW_INLINE_INGEST"), False))

    # Cookies / session
    session_cookie_name: str = field(default_factory=lambda: os.getenv("SESSION_COOKIE_NAME", "lc_session"))
    csrf_cookie_name: str = field(default_factory=lambda: os.getenv("CSRF_COOKIE_NAME", "lc_csrf"))
    csrf_header_name: str = field(default_factory=lambda: os.getenv("CSRF_HEADER_NAME", "X-CSRF-Token"))
    session_ttl_days: int = field(default_factory=lambda: int(os.getenv("SESSION_TTL_DAYS", "30")))
    session_cookie_secure_override: Optional[bool] = field(
        default_factory=lambda: (_to_bool(os.getenv("SESSION_COOKIE_SECURE"), False) if os.getenv("SESSION_COOKIE_SECURE") is not None else None)
    )
    session_cookie_samesite_override: Optional[str] = field(default_factory=lambda: os.getenv("SESSION_COOKIE_SAMESITE"))

    # OpenAI / embeddings
    embed_model: str = field(default_factory=lambda: os.getenv("EMBED_MODEL", "text-embedding-3-small"))
    embed_dim: int = field(default_factory=lambda: int(os.getenv("EMBED_DIM", "1536")))
    embed_batch_size: int = field(default_factory=lambda: int(os.getenv("EMBED_BATCH_SIZE", "48")))
    embed_max_retries: int = field(default_factory=lambda: int(os.getenv("EMBED_MAX_RETRIES", "6")))
    embed_base_backoff: float = field(default_factory=lambda: float(os.getenv("EMBED_BASE_BACKOFF", "0.6")))
    answer_model: str = field(default_factory=lambda: os.getenv("ANSWER_MODEL", "gpt-4o-mini"))

    # Quotas
    max_ingests_per_day: int = field(default_factory=lambda: int(os.getenv("MAX_INGESTS_PER_USER_PER_DAY", "3")))
    max_rag_requests_per_day: int = field(default_factory=lambda: int(os.getenv("MAX_RAG_REQUESTS_PER_DAY", "200")))

    @property
    def is_local(self) -> bool:
        return self.env.lower() in {"local", "development", "dev"}

    @property
    def is_prod_like(self) -> bool:
        return not self.is_local

    @property
    def session_cookie_secure(self) -> bool:
        if self.session_cookie_secure_override is not None:
            return bool(self.session_cookie_secure_override)
        return not self.is_local

    @property
    def session_cookie_samesite(self) -> str:
        if self.session_cookie_samesite_override:
            return self.session_cookie_samesite_override
        return "lax" if self.is_local else "strict"

    def _require(self, name: str, value: str, *, min_len: int = 1, hard_min_len: Optional[int] = None) -> list[str]:
        errors: list[str] = []
        if not value:
            errors.append(f"{name} must be set")
        limit = hard_min_len if self.is_prod_like else min_len
        if limit and len(value) < limit:
            errors.append(f"{name} must be at least {limit} characters long in this environment")
        return errors

    def validate(self) -> None:
        errors: list[str] = []

        required = {
            "DATABASE_URL": self.database_url,
            "GOOGLE_CLIENT_ID": self.google_client_id,
            "GOOGLE_CLIENT_SECRET": self.google_client_secret,
            "OAUTH_REDIRECT_URI": self.oauth_redirect_uri,
            "OPENAI_API_KEY": self.openai_api_key,
        }
        for key, val in required.items():
            errors.extend(self._require(key, val, min_len=1))

        errors.extend(self._require("SESSION_SECRET", self.session_secret, min_len=1, hard_min_len=32))
        errors.extend(self._require("DRIVE_CREDENTIALS_KEY/FERNET_KEY", self.drive_credentials_key, min_len=1, hard_min_len=32))

        if self.is_prod_like and not self.database_url.lower().startswith("postgresql"):
            errors.append("DATABASE_URL must point to Postgres in non-local environments.")

        if errors:
            raise RuntimeError("; ".join(errors))


settings = Settings()
settings.validate()

# Backwards-compatible aliases
ENVIRONMENT = settings.env
READ_ONLY_MODE = settings.read_only_mode
ALLOW_INLINE_INGEST = settings.allow_inline_ingest
SESSION_SECRET = settings.session_secret
DRIVE_CREDENTIALS_KEY = settings.drive_credentials_key
