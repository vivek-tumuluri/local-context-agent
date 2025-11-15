from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

import app.core.auth as auth_module


def _dummy_creds():
    return SimpleNamespace(
        token="token-xyz",
        refresh_token="refresh-abc",
        token_uri="https://oauth2.googleapis.com/token",
        client_id="client",
        client_secret="secret",
        scopes=["email"],
    )


def test_credentials_round_trip_with_encryption():
    payload = auth_module._serialize_credentials(_dummy_creds())
    assert "ciphertext" in payload
    decoded = auth_module._deserialize_credentials(payload)
    assert decoded["token"] == "token-xyz"
    assert decoded["client_id"] == "client"


def test_wrong_key_cannot_decrypt(reload_auth, fernet_key):
    auth = reload_auth(APP_ENV="production", SESSION_SECRET="y" * 40, DRIVE_CREDENTIALS_KEY=fernet_key)
    encrypted = auth._serialize_credentials(_dummy_creds())

    new_key = "MTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTE="
    auth2 = reload_auth(APP_ENV="production", SESSION_SECRET="y" * 40, DRIVE_CREDENTIALS_KEY=new_key)
    with pytest.raises(RuntimeError) as exc:
        auth2._deserialize_credentials(encrypted)
    assert "decrypt" in str(exc.value).lower()
    assert "token-xyz" not in str(exc.value)


def test_missing_google_credentials_surface_http_error(db_session, user_factory):
    user = user_factory("nocreds@example.com")
    with pytest.raises(HTTPException) as exc:
        auth_module.get_google_credentials_for_user(db_session, user.id)
    assert exc.value.status_code == 400
