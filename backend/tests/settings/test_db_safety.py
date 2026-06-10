from __future__ import annotations

import pytest

from tests.db_safety import DEFAULT_TEST_DATABASE_URL, configure_test_database


def test_configure_test_database_defaults_to_sqlite():
    env = {}

    chosen = configure_test_database(env)

    assert chosen == DEFAULT_TEST_DATABASE_URL
    assert env["DATABASE_URL"] == DEFAULT_TEST_DATABASE_URL


def test_configure_test_database_refuses_supabase_url():
    env = {
        "TEST_DATABASE_URL": "postgresql://postgres:secret@aws-1-us-east-1.pooler.supabase.com:5432/postgres",
        "ALLOW_PGVECTOR_INTEGRATION_TESTS": "1",
        "ALLOW_NON_TEST_POSTGRES_FOR_TESTS": "1",
    }

    with pytest.raises(RuntimeError, match="Supabase"):
        configure_test_database(env)


def test_configure_test_database_requires_postgres_opt_in():
    env = {"TEST_DATABASE_URL": "postgresql://localhost:5432/local_context_test"}

    with pytest.raises(RuntimeError, match="Postgres tests require"):
        configure_test_database(env)


def test_configure_test_database_rejects_non_test_postgres_even_when_opted_in():
    env = {
        "TEST_DATABASE_URL": "postgresql://localhost:5432/local_context",
        "ALLOW_PGVECTOR_INTEGRATION_TESTS": "1",
    }

    with pytest.raises(RuntimeError, match="test database"):
        configure_test_database(env)


def test_configure_test_database_allows_explicit_local_test_postgres():
    env = {
        "TEST_DATABASE_URL": "postgresql://localhost:5432/local_context_test",
        "ALLOW_PGVECTOR_INTEGRATION_TESTS": "1",
    }

    chosen = configure_test_database(env)

    assert chosen == "postgresql://localhost:5432/local_context_test"
    assert env["DATABASE_URL"] == chosen
