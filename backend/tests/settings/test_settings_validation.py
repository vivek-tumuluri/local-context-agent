from __future__ import annotations

import importlib
import os

import pytest


def _reload_settings(monkeypatch, **env):
    for key, val in env.items():
        if val is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, val)
    import app.core.settings as settings
    return importlib.reload(settings)


def _base_env():
    return {
        "GOOGLE_CLIENT_ID": "gcid",
        "GOOGLE_CLIENT_SECRET": "gsecret",
        "OAUTH_REDIRECT_URI": "http://localhost/auth/callback",
        "OPENAI_API_KEY": "okey",
        "SESSION_SECRET": "s" * 32,
        "DRIVE_CREDENTIALS_KEY": "d" * 32,
        "DATABASE_URL": "postgresql://user:pass@localhost:5432/db",
    }


def test_local_missing_database_url(monkeypatch):
    env = _base_env()
    env["ENV"] = "local"
    env["DATABASE_URL"] = ""
    with pytest.raises(RuntimeError, match="DATABASE_URL"):
        _reload_settings(monkeypatch, **env)


def test_local_missing_session_secret(monkeypatch):
    env = _base_env()
    env["ENV"] = "local"
    env["SESSION_SECRET"] = ""
    with pytest.raises(RuntimeError, match="SESSION_SECRET"):
        _reload_settings(monkeypatch, **env)


def test_local_missing_drive_key(monkeypatch):
    env = _base_env()
    env["ENV"] = "local"
    env["DRIVE_CREDENTIALS_KEY"] = ""
    with pytest.raises(RuntimeError, match="DRIVE_CREDENTIALS_KEY"):
        _reload_settings(monkeypatch, **env)


def test_prod_weak_session_secret(monkeypatch):
    env = _base_env()
    env["ENV"] = "prod"
    env["SESSION_SECRET"] = "short"
    with pytest.raises(RuntimeError, match="SESSION_SECRET"):
        _reload_settings(monkeypatch, **env)


def test_prod_weak_drive_key(monkeypatch):
    env = _base_env()
    env["ENV"] = "prod"
    env["DRIVE_CREDENTIALS_KEY"] = "short"
    with pytest.raises(RuntimeError, match="DRIVE_CREDENTIALS_KEY"):
        _reload_settings(monkeypatch, **env)


def test_prod_non_postgres_database(monkeypatch):
    env = _base_env()
    env["ENV"] = "prod"
    env["DATABASE_URL"] = "sqlite:///tmp.db"
    with pytest.raises(RuntimeError, match="Postgres"):
        _reload_settings(monkeypatch, **env)


def test_prod_missing_openai(monkeypatch):
    env = _base_env()
    env["ENV"] = "prod"
    env["OPENAI_API_KEY"] = ""
    with pytest.raises(RuntimeError, match="OPENAI_API_KEY"):
        _reload_settings(monkeypatch, **env)


def test_app_env_alias(monkeypatch):
    monkeypatch.delenv("ENV", raising=False)
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    env = _base_env()
    env["ENV"] = ""
    env["APP_ENV"] = "prod"
    env["DATABASE_URL"] = "sqlite:///tmp.db"
    with pytest.raises(RuntimeError):
        _reload_settings(monkeypatch, **env)
