from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi.responses import RedirectResponse

import app.auth as auth_module


def test_session_cookie_flags_enforced(reload_auth, fernet_key):
    auth = reload_auth(
        APP_ENV="production",
        SESSION_SECRET="x" * 40,
        SESSION_COOKIE_SECURE="1",
        SESSION_COOKIE_SAMESITE="strict",
        DRIVE_CREDENTIALS_KEY=fernet_key,
    )
    resp = RedirectResponse(url="/auth/me")
    auth._set_session_cookie(resp, "token-123")
    cookie = resp.headers["set-cookie"]
    assert "Secure" in cookie
    assert "HttpOnly" in cookie
    assert "SameSite=strict" in cookie


def test_missing_session_secret_in_prod_raises(reload_auth, fernet_key):
    with pytest.raises(RuntimeError, match="SESSION_SECRET"):
        reload_auth(APP_ENV="production", SESSION_SECRET="", DRIVE_CREDENTIALS_KEY=fernet_key)


def test_extract_session_token_prefers_cookie():
    request = SimpleNamespace(
        cookies={"lc_session": "cookie-token"},
        headers={"Authorization": "Bearer header-token", "X-Session": "alt-token"},
    )
    token = auth_module._extract_session_token(request)
    assert token == "cookie-token"


def test_query_params_not_accepted_for_session():
    request = SimpleNamespace(cookies={}, headers={}, query_params={"lc_session": "query-token"})
    token = auth_module._extract_session_token(request)
    assert token is None
