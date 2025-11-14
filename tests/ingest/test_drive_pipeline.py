from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

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


def _ingest_single_doc(db: Session, user_id: str, summary: Dict[str, Any]) -> Dict[str, Any]:  # type: ignore[name-defined]
    doc_work = summary.get("doc_work")
    if not doc_work:
        return summary
    batcher = drive_pipeline.EmbeddingBatcher(user_id)
    ready = batcher.enqueue_doc(doc_work)
    ready += batcher.flush(force=True)
    drive_pipeline._finalize_ready_docs(db, user_id, ready)
    summary["embedded"] = doc_work.embedded_count
    return summary


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
    assert _ingest_single_doc(db_session, test_user.id, summary)["embedded"] > 0

    summary = drive_pipeline.process_drive_file(
        db_session,
        user_id=test_user.id,
        file_meta=_make_file("doc-1", version="2", modifiedTime=None),
        fetch_file_bytes=lambda **_: second_text.encode(),
        parse_bytes=lambda data, mime: data.decode(),
    )
    summary = _ingest_single_doc(db_session, test_user.id, summary)
    assert summary["embedded"] > 0
    chunk_ids = vector_module.list_doc_chunk_ids("doc-1", user_id=test_user.id)
    assert len(chunk_ids) == summary["embedded"]


def test_process_drive_file_does_not_delete_on_embedding_failure(monkeypatch, db_session, fake_vector_env, test_user):
    first = "initial body"
    content_hash = compute_content_hash(first)
    summary = drive_pipeline.process_drive_file(
        db_session,
        user_id=test_user.id,
        file_meta=_make_file("doc-err", version="1", modifiedTime=None),
        fetch_file_bytes=lambda **_: first.encode(),
        parse_bytes=lambda data, mime: data.decode(),
    )
    _ingest_single_doc(db_session, test_user.id, summary)

    second = "updated text body"
    summary = drive_pipeline.process_drive_file(
        db_session,
        user_id=test_user.id,
        file_meta=_make_file("doc-err", version="2", modifiedTime=None),
        fetch_file_bytes=lambda **_: second.encode(),
        parse_bytes=lambda data, mime: data.decode(),
    )
    doc_work = summary.get("doc_work")
    assert doc_work is not None
    assert doc_work.existing_chunk_ids, "expected previous chunks to exist"

    deleted = {"called": False}

    def fake_delete(ids, user_id=None):
        deleted["called"] = True
        return len(ids or [])

    def raise_embed(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(drive_pipeline.vector, "delete_ids", fake_delete)
    monkeypatch.setattr(drive_pipeline.vector, "_embed_with_retry", raise_embed)

    batcher = drive_pipeline.EmbeddingBatcher(test_user.id, max_batch_size=1)
    with pytest.raises(drive_pipeline.EmbeddingBatchError):
        batcher.enqueue_doc(doc_work)
    assert deleted["called"] is False


def test_process_drive_file_skips_when_parse_returns_empty(monkeypatch, db_session, test_user, fake_vector_env):
    drive_pipeline.process_drive_file(
        db_session,
        user_id=test_user.id,
        file_meta=_make_file("doc-skip", version="1", modifiedTime=None),
        fetch_file_bytes=lambda **_: b"content",
        parse_bytes=lambda data, mime: "some text",
    )

    deleted = {"called": False}
    monkeypatch.setattr(drive_pipeline.vector, "delete_ids", lambda *args, **kwargs: deleted.update(called=True))
    monkeypatch.setattr(drive_pipeline.vector, "list_doc_chunk_ids", lambda *args, **kwargs: ["doc-skip-0"])

    summary = drive_pipeline.process_drive_file(
        db_session,
        user_id=test_user.id,
        file_meta=_make_file("doc-skip", version="2", modifiedTime=None),
        fetch_file_bytes=lambda **_: b"new",
        parse_bytes=lambda data, mime: "",
    )
    assert summary["embedded"] == 0
    assert deleted["called"] is False


def test_process_drive_file_raises_when_embeddings_return_no_chunks(monkeypatch, db_session, test_user, fake_vector_env):
    first_text = "A" * 1300
    second_text = "New content forcing reembed"
    drive_pipeline.process_drive_file(
        db_session,
        user_id=test_user.id,
        file_meta=_make_file("doc-empty", version="1", modifiedTime=None),
        fetch_file_bytes=lambda **_: first_text.encode(),
        parse_bytes=lambda data, mime: data.decode(),
    )

    monkeypatch.setattr(drive_pipeline.vector, "list_doc_chunk_ids", lambda *args, **kwargs: ["ext-id"])

    def fake_split(text):
        return [" ", "\n"]

    monkeypatch.setattr(drive_pipeline, "split_by_chars", fake_split)

    with pytest.raises(RuntimeError, match="returned no chunks"):
        drive_pipeline.process_drive_file(
            db_session,
            user_id=test_user.id,
            file_meta=_make_file("doc-empty", version="2", modifiedTime=None),
            fetch_file_bytes=lambda **_: second_text.encode(),
            parse_bytes=lambda data, mime: data.decode(),
        )


def test_run_drive_ingest_once_handles_listing_errors(db_session, test_user):
    job = IngestionJob(id="job-1", user_id=test_user.id, status="running", processed_files=0, total_files=0)
    db_session.add(job)
    db_session.commit()

    def bad_list(**kwargs):
        raise RuntimeError("throttle")

    with pytest.raises(RuntimeError, match="Drive listing failed"):
        drive_pipeline.run_drive_ingest_once(
            db_session,
            user_id=test_user.id,
            list_page=bad_list,
            fetch_file_bytes=lambda **_: b"",
            parse_bytes=lambda data, mime: "",
            job=job,
        )
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


def test_process_drive_file_attaches_drive_metadata(db_session, fake_vector_env, test_user):
    file_meta = _make_file("doc-meta", name="Launch Plan", mimeType="application/pdf")
    summary = drive_pipeline.process_drive_file(
        db_session,
        user_id=test_user.id,
        file_meta=file_meta,
        fetch_file_bytes=lambda **_: b"launch content",
        parse_bytes=lambda data, mime: data.decode(),
    )
    _ingest_single_doc(db_session, test_user.id, summary)
    fake_client, _ = fake_vector_env
    key = vector_module._collection_key(user_id=test_user.id)
    collection = fake_client.collections[key]
    assert collection.rows, "expected chunks to be stored"
    sample_row = next(iter(collection.rows.values()))
    meta = sample_row.meta
    assert meta["source"] == "drive"
    assert meta["title"] == "Launch Plan"
    assert meta["doc_id"] == "doc-meta"
    assert meta["link"].endswith("/doc-meta/view")


def test_embedding_batcher_batches_multiple_docs(db_session, fake_vector_env, test_user):
    batcher = drive_pipeline.EmbeddingBatcher(test_user.id, max_batch_size=100, max_tokens=10000)
    texts = {
        "doc-b1": "Doc One body text",
        "doc-b2": "Doc Two content here",
    }
    for fid, body in texts.items():
        summary = drive_pipeline.process_drive_file(
            db_session,
            user_id=test_user.id,
            file_meta=_make_file(fid, version="1", modifiedTime=None),
            fetch_file_bytes=lambda body_text=body, **_: body_text.encode(),
            parse_bytes=lambda data, mime: data.decode(),
        )
        doc_work = summary.get("doc_work")
        assert doc_work is not None
        ready = batcher.enqueue_doc(doc_work)
        drive_pipeline._finalize_ready_docs(db_session, test_user.id, ready)
    ready = batcher.flush(force=True)
    drive_pipeline._finalize_ready_docs(db_session, test_user.id, ready)
    _, embeddings = fake_vector_env
    assert len(embeddings.calls) == 1
    combined_input = embeddings.calls[0]["input"]
    assert any("Doc One" in text for text in combined_input)
    assert any("Doc Two" in text for text in combined_input)


def test_stale_chunks_removed_after_doc_complete(monkeypatch, db_session, fake_vector_env, test_user):
    first = " ".join(["chunk"] * 2000)
    summary = drive_pipeline.process_drive_file(
        db_session,
        user_id=test_user.id,
        file_meta=_make_file("doc-long", version="1", modifiedTime=None),
        fetch_file_bytes=lambda **_: first.encode(),
        parse_bytes=lambda data, mime: data.decode(),
    )
    _ingest_single_doc(db_session, test_user.id, summary)
    initial_ids = vector_module.list_doc_chunk_ids("doc-long", user_id=test_user.id)
    assert len(initial_ids) > 1

    long_text = " ".join(["updated"] * 300)
    summary = drive_pipeline.process_drive_file(
        db_session,
        user_id=test_user.id,
        file_meta=_make_file("doc-long", version="2", modifiedTime=None),
        fetch_file_bytes=lambda **_: long_text.encode(),
        parse_bytes=lambda data, mime: data.decode(),
    )
    doc_work = summary.get("doc_work")
    assert doc_work is not None
    assert doc_work.embedded_count < len(initial_ids)
    delete_calls: List[List[str]] = []

    def fake_delete(ids, user_id=None):
        delete_calls.append(list(ids))
        return len(ids or [])

    monkeypatch.setattr(drive_pipeline.vector, "delete_ids", fake_delete)
    batcher = drive_pipeline.EmbeddingBatcher(test_user.id, max_batch_size=1, max_tokens=50)
    ready = batcher.enqueue_doc(doc_work)
    drive_pipeline._finalize_ready_docs(db_session, test_user.id, ready)
    ready = batcher.flush(force=True)
    drive_pipeline._finalize_ready_docs(db_session, test_user.id, ready)
    assert len(delete_calls) == 1
