from __future__ import annotations

import json

from app.logging_utils import log_event


def test_log_event_emits_json(caplog):
    caplog.set_level("INFO")
    log_event("test_event", foo="bar", none_field=None)
    assert caplog.records
    payload = json.loads(caplog.records[0].message)
    assert payload["event"] == "test_event"
    assert payload["foo"] == "bar"
    assert "none_field" not in payload
