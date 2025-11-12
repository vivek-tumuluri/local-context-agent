from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Optional

import pytest
from sqlalchemy.orm import Session

from app.ingest import drive_pipeline
from app.models import ContentIndex, IngestionJob, SourceState
from app.ingest.text_normalize import compute_content_hash
from app.rag import vector as vector_module


def _make_file(fid: str, **meta) -> Dict[str, str]:
    defaults = {
        "id": fid,
        "name": f"file-{fid}",
        "mimeType": "text/plain",
        "md5Checksum": "md5",
        "modifiedTime": "2024-01-01T00:00:00Z",
        "version": "1",
    }
    defaults.update(meta)
    return defaults


def _add_index_row(db: Session, user_id: str, fid: str, content_hash: str) -> ContentIndex:
    row = ContentIndex(
        id=fid,
        user_id=user_id,
        source="drive",
        external_id=fid,
        name="Test",
        mime_type="text/plain",
        md5="md5",
        modified_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        version="1",
        is_trashed=False,
        content_hash=content_hash,
        last_ingested_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    db.add(row)
    db.commit()
    return row


def test_process_drive_file_skips_when_hash_unchanged(db_session, fake_vector_env, test_user):
    text = "unchanged text body"
    content_hash = compute_content_hash(text)
    _add_index_row(db_session, test_user.id, "file-1", content_hash)

    summary = drive_pipeline.process_drive_file(
        db_session,
        user_id=test_user.id,
        file_meta=_make_file("file-1", md5Checksum="md5", modifiedTime=None),
        fetch_file_bytes=lambda **_: text.encode(),
        parse_bytes=lambda data, mime: data.decode(),
    )
    assert summary["processed"] == 1
    assert summary["embedded"] == 0
    _, embeddings = fake_vector_env
    assert embeddings.calls == []


def test_process_drive_file_replaces_stale_chunks(db_session, fake_vector_env, test_user):
    first_text = "A" * 1300
    second_text = "fresh text"

    summary = drive_pipeline.process_drive_file(
        db_session,
        user_id=test_user.id,
        file_meta=_make_file("doc-1", version="1", modifiedTime=None),
        fetch_file_bytes=lambda **_: first_text.encode(),
        parse_bytes=lambda data, mime: data.decode(),
    )
    assert summary["embedded"] > 0

    summary = drive_pipeline.process_drive_file(
        db_session,
        user_id=test_user.id,
        file_meta=_make_file("doc-1", version="2", modifiedTime=None),
        fetch_file_bytes=lambda **_: second_text.encode(),
        parse_bytes=lambda data, mime: data.decode(),
    )
    assert summary["embedded"] > 0
    chunk_ids = vector_module.list_doc_chunk_ids("doc-1", user_id=test_user.id)
    assert len(chunk_ids) == summary["embedded"]


def test_process_drive_file_does_not_delete_on_embedding_failure(monkeypatch, db_session, test_user):
    text = "updated text body"
    content_hash = compute_content_hash("old text")
    _add_index_row(db_session, test_user.id, "doc-err", content_hash)

    deleted = {"called": False}

    def fake_upsert(*args, **kwargs):
        raise RuntimeError("boom")

    def fake_delete(ids, user_id=None):
        deleted["called"] = True
        return len(ids or [])

    monkeypatch.setattr(drive_pipeline.vector, "upsert", fake_upsert)
    monkeypatch.setattr(drive_pipeline.vector, "delete_ids", fake_delete)
    monkeypatch.setattr(drive_pipeline.vector, "list_doc_chunk_ids", lambda *args, **kwargs: ["doc-err-0"])

    with pytest.raises(RuntimeError):
        drive_pipeline.process_drive_file(
            db_session,
            user_id=test_user.id,
            file_meta=_make_file("doc-err", version="2", modifiedTime=None),
            fetch_file_bytes=lambda **_: text.encode(),
            parse_bytes=lambda data, mime: data.decode(),
        )
    assert deleted["called"] is False


def test_run_drive_ingest_once_handles_listing_errors(db_session, test_user):
    job = IngestionJob(id="job-1", user_id=test_user.id, status="running", processed_files=0, total_files=0)
    db_session.add(job)
    db_session.commit()

    def bad_list(**kwargs):
        raise RuntimeError("throttle")

    summary = drive_pipeline.run_drive_ingest_once(
        db_session,
        user_id=test_user.id,
        list_page=bad_list,
        fetch_file_bytes=lambda **_: b"",
        parse_bytes=lambda data, mime: "",
        job=job,
    )
    assert summary["errors"] == 1
    db_session.refresh(job)
    assert job.status == "failed"
    assert "list error" in (job.error_summary or "")


def test_save_and_load_drive_cursor(db_session, test_user):
    assert drive_pipeline.load_drive_cursor(db_session, test_user.id) is None
    drive_pipeline.save_drive_cursor(db_session, test_user.id, "token-1", extra={"seen": 5})
    token = drive_pipeline.load_drive_cursor(db_session, test_user.id)
    assert token == "token-1"
    state = db_session.get(SourceState, (test_user.id, drive_pipeline.DRIVE_SOURCE))
    assert state.extra == {"seen": 5}
