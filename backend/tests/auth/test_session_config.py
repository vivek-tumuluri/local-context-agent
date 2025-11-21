from __future__ import annotations

import importlib

import pytest
from fastapi.responses import RedirectResponse

import app.core.auth as auth_module


@pytest.fixture()
def reload_auth_settings(monkeypatch):
    def _reload(**env):
        for k, v in env.items():
            if v is None:
                monkeypatch.delenv(k, raising=False)
            else:
                monkeypatch.setenv(k, v)
        import app.core.settings as settings
        importlib.reload(settings)
        importlib.reload(auth_module)
        return auth_module

    return _reload


def _extract_cookie(resp: RedirectResponse, name: str) -> str:
    header = resp.headers.get("set-cookie", "")
    return header


def test_session_cookie_local_flags(reload_auth_settings):
    auth = reload_auth_settings(
        ENV="local",
        SESSION_SECRET="s" * 32,
        DRIVE_CREDENTIALS_KEY=VALID_FERNET,
        GOOGLE_CLIENT_ID="x",
        GOOGLE_CLIENT_SECRET="y",
        OAUTH_REDIRECT_URI="http://example.com",
        OPENAI_API_KEY="z",
        DATABASE_URL="postgresql://u:p@localhost/db",
        SESSION_COOKIE_SECURE="0",
        SESSION_COOKIE_SAMESITE="lax",
    )
    resp = RedirectResponse(url="/auth/me")
    auth._set_session_cookie(resp, "tkn")
    cookie = _extract_cookie(resp, auth.SESSION_COOKIE_NAME)
    assert "Secure" not in cookie
    assert "HttpOnly" in cookie
    assert "SameSite=lax" in cookie


def test_session_cookie_prod_flags(reload_auth_settings):
    auth = reload_auth_settings(
        ENV="prod",
        SESSION_SECRET="s" * 32,
        DRIVE_CREDENTIALS_KEY=VALID_FERNET,
        SESSION_COOKIE_SECURE="1",
        SESSION_COOKIE_SAMESITE="strict",
        GOOGLE_CLIENT_ID="x",
        GOOGLE_CLIENT_SECRET="y",
        OAUTH_REDIRECT_URI="http://example.com",
        OPENAI_API_KEY="z",
        DATABASE_URL="postgresql://u:p@localhost/db",
    )
    resp = RedirectResponse(url="/auth/me")
    auth._set_session_cookie(resp, "tkn")
    cookie = _extract_cookie(resp, auth.SESSION_COOKIE_NAME)
    assert "Secure" in cookie
    assert "HttpOnly" in cookie
    assert "SameSite=strict" in cookie
VALID_FERNET = "RjaLbvsu8b_3Nn-VsQJKhNz1Iv-pWQmERgUe0beTr0g="
