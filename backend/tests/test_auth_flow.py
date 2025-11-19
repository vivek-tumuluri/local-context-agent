from __future__ import annotations

from typing import Dict, Optional

import importlib

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from fastapi import HTTPException
from google.oauth2.credentials import Credentials

from app.core import auth
from app.core import db as app_db
from app.core.models import Base, User, DriveSession


class DummyRequest:
    def __init__(
        self,
        *,
        cookies: Optional[Dict[str, str]] = None,
        headers: Optional[Dict[str, str]] = None,
        query_params: Optional[Dict[str, str]] = None,
    ):
        self.cookies = cookies or {}
        self.headers = headers or {}
        self.query_params = query_params or {}


@pytest.fixture()
def session_factory(monkeypatch, tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path}/auth.db", future=True)
    TestingSessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    Base.metadata.create_all(engine)

    def _get_db():
        db = TestingSessionLocal()
        try:
            yield db
        finally:
            db.close()

    monkeypatch.setattr(app_db, "engine", engine, raising=False)
    monkeypatch.setattr(app_db, "SessionLocal", TestingSessionLocal, raising=False)
    monkeypatch.setattr(app_db, "get_db", _get_db, raising=False)
    monkeypatch.setattr(auth, "SessionLocal", TestingSessionLocal, raising=False)
    monkeypatch.setattr(auth, "get_db", _get_db, raising=False)
    return TestingSessionLocal


@pytest.fixture()
def db_session(session_factory):
    db = session_factory()
    try:
        yield db
    finally:
        db.close()


def _create_user(db) -> User:
    user = User(google_sub="sub123", email="user@example.com")
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def test_issue_session_and_get_current_user(db_session, session_factory):
    user = _create_user(db_session)
    token = auth._issue_session(db_session, user)

    request = DummyRequest(cookies={auth.SESSION_COOKIE_NAME: token})
    with session_factory() as fresh_db:
        authed = auth.get_current_user(request, db=fresh_db)
    assert authed.id == user.id


def test_get_current_user_without_token_fails(db_session):
    _create_user(db_session)
    request = DummyRequest()
    with pytest.raises(HTTPException) as exc:
        auth.get_current_user(request, db=db_session)
    assert exc.value.status_code == 401


def test_google_credentials_round_trip(db_session, session_factory):
    user = _create_user(db_session)
    creds = Credentials(
        token="token-123",
        refresh_token=None,
        token_uri="https://oauth2.googleapis.com/token",
        client_id="client",
        client_secret="secret",
        scopes=["openid"],
    )
    auth._persist_google_credentials(db_session, user.id, creds)

    with session_factory() as fresh_db:
        loaded = auth.get_google_credentials_for_user(fresh_db, user.id)
    assert loaded.token == "token-123"

    unmanaged = auth.get_google_credentials_for_user_unmanaged(user.id)
    assert unmanaged.token == "token-123"


def test_google_credentials_encrypted_when_key_set(db_session, session_factory, monkeypatch):
    # Ensure a known key and reload auth to pick it up
    key = "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA="
    monkeypatch.setenv("DRIVE_CREDENTIALS_KEY", key)
    import app.core.auth as auth_module
    importlib.reload(auth_module)

    user = _create_user(db_session)
    creds = Credentials(
        token="token-xyz",
        refresh_token=None,
        token_uri="https://oauth2.googleapis.com/token",
        client_id="c",
        client_secret="s",
        scopes=["openid"],
    )
    auth_module._persist_google_credentials(db_session, user.id, creds)
    row = db_session.get(DriveSession, user.id)
    assert row is not None
    # Stored credentials should be encrypted (ciphertext key present)
    assert isinstance(row.credentials, dict)
    assert "ciphertext" in row.credentials
    # Round-trip via managed getter
    loaded = auth_module.get_google_credentials_for_user(db_session, user.id)
    assert loaded.token == "token-xyz"


def test_non_local_requires_drive_key(monkeypatch):
    import app.core.settings as settings

    monkeypatch.setenv("DRIVE_CREDENTIALS_KEY", "")
    monkeypatch.setenv("ENVIRONMENT", "prod")
    with pytest.raises(RuntimeError):
        importlib.reload(settings)

    # restore local defaults for other tests
    monkeypatch.setenv("ENVIRONMENT", "local")
    monkeypatch.setenv("DRIVE_CREDENTIALS_KEY", "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA=")
    importlib.reload(settings)
