from __future__ import annotations

from app.ingest import calendar_pipeline


def test_build_calendar_chunk_rows_prefixes_retrieval_metadata(test_user):
    rows = calendar_pipeline._build_chunk_rows(
        user_id=test_user.id,
        doc_id="cal-prefix",
        text=(
            "Event: Weekly Sync\n"
            "Start: 2026-06-08T10:00:00Z\n"
            "Location: Room 4B\n"
            "Description: Review launch blockers."
        ),
        content_hash="hash-prefix",
        doc_meta={
            "title": "Weekly Sync",
            "source": "calendar",
            "doc_id": "cal-prefix",
            "start": "2026-06-08T10:00:00Z",
            "end": "2026-06-08T11:00:00Z",
            "location": "Room 4B",
            "status": "confirmed",
        },
    )

    assert rows
    first = rows[0]
    assert first["id"] == f"{test_user.id}-cal-prefix-0"
    assert first["text"].startswith("Title: Weekly Sync\nSource: calendar\nStatus: confirmed\n\n")
    assert "Start: 2026-06-08T10:00:00Z" in first["text"]
    assert "Location: Room 4B" in first["text"]
    assert first["meta"]["source"] == "calendar"
    assert first["meta"]["doc_id"] == "cal-prefix"
    assert first["meta"]["title"] == "Weekly Sync"
    assert first["meta"]["start"] == "2026-06-08T10:00:00Z"
    assert first["meta"]["location"] == "Room 4B"
    assert first["meta"]["content_hash"] == "hash-prefix"


def test_calendar_prefix_skips_empty_fields():
    rows = calendar_pipeline._build_chunk_rows(
        user_id="user",
        doc_id="cal-empty",
        text="Event: Empty Fields",
        content_hash="hash",
        doc_meta={"title": "Empty Fields", "source": "calendar", "status": None},
    )

    assert "Status: None" not in rows[0]["text"]
    assert rows[0]["text"].startswith("Title: Empty Fields\nSource: calendar\n\n")
