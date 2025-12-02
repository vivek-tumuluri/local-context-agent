from __future__ import annotations

import pytest

from app.ingest import drive_ingest, drive_pipeline
from app.rag import vector_store as vector
from app.rag import vector_pg
from app.routes import ingest_routes


def test_drive_ingest_skips_unsupported_mime(monkeypatch, db_session, user_factory, fake_vector_env):
    user = user_factory(email="skip-mime@example.com")

    def _list_page(user_id: str, page_token, page_size: int):
        return {"files": [{"id": "file-skip", "name": "Skip Me", "mimeType": "image/png", "size": 1234}]}

    def _fetch(*args, **kwargs):
        raise AssertionError("fetch should not be called for unsupported MIME")

    summary = drive_pipeline.run_drive_ingest_once(
        db=db_session,
        user_id=user.id,
        list_page=_list_page,
        fetch_file_bytes=_fetch,
        parse_bytes=lambda b, m: "",
        job=None,
        page_token=None,
        page_size=5,
    )

    assert summary["files_skipped_unsupported_mime"] == 1
    assert summary["embedded"] == 0


def test_drive_ingest_skips_too_large_bytes(monkeypatch, db_session, user_factory, fake_vector_env):
    user = user_factory(email="skip-size@example.com")
    monkeypatch.setattr(drive_pipeline, "MAX_FILE_BYTES", 50)

    def _list_page(user_id: str, page_token, page_size: int):
        return {"files": [{"id": "file-big", "name": "Big File", "mimeType": "text/plain", "size": 100}]}

    def _fetch(*args, **kwargs):
        raise AssertionError("fetch should not be called for oversized files")

    summary = drive_pipeline.run_drive_ingest_once(
        db=db_session,
        user_id=user.id,
        list_page=_list_page,
        fetch_file_bytes=_fetch,
        parse_bytes=lambda b, m: "",
        job=None,
        page_token=None,
        page_size=5,
    )

    assert summary["files_skipped_too_large_bytes"] == 1
    assert summary["embedded"] == 0


def test_drive_ingest_allows_google_slides(monkeypatch, db_session, user_factory, fake_vector_env):
    user = user_factory(email="slides@example.com")
    monkeypatch.setattr(drive_pipeline, "MAX_FILE_BYTES", 1_000_000)
    monkeypatch.setattr(drive_pipeline, "MAX_CHUNKS_PER_FILE", 50)
    monkeypatch.setattr(drive_pipeline, "MAX_TOKENS_PER_JOB", 0)

    def _list_page(user_id: str, page_token, page_size: int):
        return {
            "files": [
                {
                    "id": "slide-1",
                    "name": "Deck",
                    "mimeType": "application/vnd.google-apps.presentation",
                }
            ]
        }

    def _fetch(user_id: str, file_id: str, mime_type: str | None):
        return b"Slide content for testing"

    def _parse(content: bytes, mime: str | None) -> str:
        return content.decode()

    # Ensure vector embedding is predictable
    monkeypatch.setattr(
        vector,
        "_embed_with_retry",
        lambda texts: [[0.0] * vector_pg.EMBED_DIM for _ in texts],
        raising=False,
    )

    summary = drive_pipeline.run_drive_ingest_once(
        db=db_session,
        user_id=user.id,
        list_page=_list_page,
        fetch_file_bytes=_fetch,
        parse_bytes=_parse,
        job=None,
        page_token=None,
        page_size=5,
    )

    assert summary["processed"] == 1
    assert summary["embedded"] >= 1
    assert summary["files_skipped_unsupported_mime"] == 0


def test_ingest_drive_propagates_token_budget(monkeypatch, user_factory, fake_vector_env):
    user = user_factory(email="budget@example.com")

    calls: list[int] = []

    def _stub_run_drive_ingest_once(
        *,
        tokens_already_embedded: int = 0,
        page_token=None,
        **kwargs,
    ):
        calls.append(tokens_already_embedded)
        token_total = tokens_already_embedded + 600
        token_hit = token_total >= 1_000
        return {
            "processed": 1,
            "embedded": 1,
            "errors": 0,
            "nextPageToken": "next" if page_token is None else None,
            "listing_failed": False,
            "token_budget_hit": token_hit,
            "estimated_tokens_embedded": token_total,
            "files_skipped_unsupported_mime": 0,
            "files_skipped_too_large_bytes": 0,
            "files_skipped_token_cap": 0,
            "files_partial_indexed": 0,
            "chunks_embedded": 1,
        }

    class _DummyDB:
        def close(self):  # pragma: no cover - trivial
            pass

    monkeypatch.setattr(drive_ingest, "run_drive_ingest_once", _stub_run_drive_ingest_once, raising=False)
    monkeypatch.setattr(drive_ingest, "SessionLocal", lambda: _DummyDB(), raising=False)
    monkeypatch.setattr(drive_ingest, "get_google_credentials_for_user_unmanaged", lambda user_id: object(), raising=False)
    monkeypatch.setattr(drive_ingest, "_drive_service", lambda creds: object(), raising=False)
    monkeypatch.setattr(
        drive_ingest, "_list_page_factory", lambda svc, name_filter: lambda user_id, page_token, page_size: {}, raising=False
    )
    monkeypatch.setattr(drive_ingest, "_fetch_file_factory", lambda svc: lambda user_id, file_id, mime_type: b"", raising=False)
    monkeypatch.setattr(drive_ingest, "load_drive_cursor", lambda db, user_id: None, raising=False)
    monkeypatch.setattr(drive_ingest, "save_drive_cursor", lambda db, user_id, cursor: None, raising=False)

    result = drive_ingest.ingest_drive(user.id, max_files=10)

    assert result["token_budget_hit"] is True
    assert result["estimated_tokens_embedded"] == 1_200
    assert calls == [0, 600]


def test_ingest_drive_advances_after_all_skipped_page(monkeypatch, user_factory, fake_vector_env):
    user = user_factory(email="skip-page@example.com")
    calls: list[str | None] = []

    def _stub_run_drive_ingest_once(*, page_token=None, tokens_already_embedded: int = 0, **kwargs):
        calls.append(page_token)
        base = {
            "embedded": 0,
            "errors": 0,
            "listing_failed": False,
            "token_budget_hit": False,
            "estimated_tokens_embedded": tokens_already_embedded,
            "files_skipped_unsupported_mime": 0,
            "files_skipped_too_large_bytes": 0,
            "files_skipped_token_cap": 0,
            "files_partial_indexed": 0,
            "chunks_embedded": 0,
        }
        if page_token is None:
            return {"processed": 0, "nextPageToken": "next", **base}
        return {"processed": 1, "nextPageToken": None, **base}

    class _DummyDB:
        def close(self):  # pragma: no cover - trivial
            pass

    monkeypatch.setattr(drive_ingest, "run_drive_ingest_once", _stub_run_drive_ingest_once, raising=False)
    monkeypatch.setattr(drive_ingest, "SessionLocal", lambda: _DummyDB(), raising=False)
    monkeypatch.setattr(drive_ingest, "get_google_credentials_for_user_unmanaged", lambda user_id: object(), raising=False)
    monkeypatch.setattr(drive_ingest, "_drive_service", lambda creds: object(), raising=False)
    monkeypatch.setattr(
        drive_ingest, "_list_page_factory", lambda svc, name_filter: lambda user_id, page_token, page_size: {}, raising=False
    )
    monkeypatch.setattr(drive_ingest, "_fetch_file_factory", lambda svc: lambda user_id, file_id, mime_type: b"", raising=False)
    monkeypatch.setattr(drive_ingest, "load_drive_cursor", lambda db, user_id: None, raising=False)
    monkeypatch.setattr(drive_ingest, "save_drive_cursor", lambda db, user_id, cursor: None, raising=False)

    result = drive_ingest.ingest_drive(user.id, max_files=5)

    assert result["found"] == 1
    assert calls == [None, "next"]


def test_run_drive_job_handles_token_budget(monkeypatch):
    seen: dict[str, object] = {}

    class _DummyDB:
        def close(self):
            seen["closed"] = True

    monkeypatch.setattr(ingest_routes, "_bg_db_session", lambda: _DummyDB(), raising=False)

    def _fake_get_job(db, job_id):
        return {"payload": {"user_id": "user-123"}}

    monkeypatch.setattr(ingest_routes.job_helper, "get_job", _fake_get_job, raising=False)
    monkeypatch.setattr(ingest_routes.job_helper, "mark_job_running", lambda db, job_id, total_files: None, raising=False)
    monkeypatch.setattr(ingest_routes.job_helper, "bump_job_progress", lambda db, job_id, inc=0, message=None: None, raising=False)

    def _fake_finish_job(db, job_id, status, error_summary=None, metrics=None):
        seen["status"] = status
        seen["summary"] = error_summary
        seen["metrics"] = metrics

    monkeypatch.setattr(ingest_routes.job_helper, "finish_job", _fake_finish_job, raising=False)
    monkeypatch.setattr(ingest_routes, "log_event", lambda *args, **kwargs: None, raising=False)

    def _fake_ingest_callable(**kwargs):
        return {
            "processed": 1,
            "embedded": 1,
            "errors": 0,
            "token_budget_hit": True,
            "estimated_tokens_embedded": 1234,
        }

    monkeypatch.setattr(ingest_routes, "INGEST_DRIVE_CALLABLE", _fake_ingest_callable, raising=False)

    ingest_routes._run_drive_job("job-token-budget")

    assert seen.get("status") == "partial"
    assert "token budget" in str(seen.get("summary", "")).lower()
