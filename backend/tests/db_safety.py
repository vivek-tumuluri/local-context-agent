from __future__ import annotations

from collections.abc import MutableMapping


DEFAULT_TEST_DATABASE_URL = "sqlite:////tmp/local_context_agent_test.db"

_LIVE_DB_MARKERS = (
    "supabase.com",
    "pooler.supabase.com",
)


def _is_postgres_url(url: str) -> bool:
    lower = url.lower()
    return lower.startswith("postgresql://") or lower.startswith("postgres://")


def _looks_like_live_database(url: str) -> bool:
    lower = url.lower()
    return any(marker in lower for marker in _LIVE_DB_MARKERS)


def _looks_like_test_database(url: str) -> bool:
    return "test" in url.lower()


def configure_test_database(env: MutableMapping[str, str]) -> str:
    """Force tests onto an isolated DB and refuse known live database URLs."""
    chosen = env.get("TEST_DATABASE_URL") or DEFAULT_TEST_DATABASE_URL

    if _looks_like_live_database(chosen):
        raise RuntimeError("Refusing to run tests against a live/Supabase DATABASE_URL.")

    if _is_postgres_url(chosen):
        allowed = env.get("ALLOW_TEST_POSTGRES") == "1" or env.get("ALLOW_PGVECTOR_INTEGRATION_TESTS") == "1"
        if not allowed:
            raise RuntimeError("Postgres tests require ALLOW_TEST_POSTGRES=1 or ALLOW_PGVECTOR_INTEGRATION_TESTS=1.")
        if not _looks_like_test_database(chosen):
            raise RuntimeError("Postgres test database URL must clearly look like a test database.")

    env["DATABASE_URL"] = chosen
    return chosen
