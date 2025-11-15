from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.core import auth
from app.core.models import ContentIndex, DriveSession, IngestionJob, SourceState, UserSession


def _make_session(user_id: str) -> UserSession:
    return UserSession(
        user_id=user_id,
        token_hash=auth._hash_token("token"),
        expires_at=datetime.now(timezone.utc) + timedelta(days=1),
    )


@pytest.mark.asyncio
async def test_disconnect_deletes_user_data(api_client, db_session, test_user, monkeypatch):
    db_session.add(
        ContentIndex(
            id="doc-1",
            user_id=test_user.id,
            source="drive",
            external_id="doc-1",
            name="Doc",
            is_trashed=False,
        )
    )
    db_session.add(SourceState(user_id=test_user.id, source="drive"))
    db_session.add(DriveSession(user_id=test_user.id, credentials={"token": "x"}))
    db_session.add(IngestionJob(user_id=test_user.id, source="drive", kind="drive_ingest"))
    db_session.add(_make_session(test_user.id))
    db_session.commit()

    called = {}

    def _reset(user_id=None, name=None):
        called["user_id"] = user_id

    monkeypatch.setattr(auth.vector, "reset_collection", _reset)

    csrf = "csrf-token"
    api_client.cookies.set(auth.SESSION_COOKIE_NAME, "session-token", domain="test.local")
    api_client.cookies.set(auth.CSRF_COOKIE_NAME, csrf, domain="test.local")

    resp = await api_client.post("/auth/disconnect", headers={auth.CSRF_HEADER_NAME: csrf})
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    deleted = body["deleted"]
    assert deleted["content_index"] == 1
    assert deleted["user_sessions"] >= 1

    db_session.expire_all()
    assert db_session.query(ContentIndex).filter_by(user_id=test_user.id).count() == 0
    assert db_session.query(SourceState).filter_by(user_id=test_user.id).count() == 0
    assert db_session.query(DriveSession).filter_by(user_id=test_user.id).count() == 0
    assert db_session.query(IngestionJob).filter_by(user_id=test_user.id).count() == 0
    assert db_session.query(UserSession).filter_by(user_id=test_user.id).count() == 0
    assert called["user_id"] == test_user.id
