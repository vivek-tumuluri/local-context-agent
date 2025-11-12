from __future__ import annotations

import logging

import pytest

from app.metrics import StageTimer


def test_stage_timer_emits_success_log(caplog):
    logger = logging.getLogger("perf.stage")
    with caplog.at_level(logging.INFO, logger=logger.name):
        with StageTimer("chunk", user_id="user-1"):
            pass
    assert any("chunk" in msg for msg in caplog.messages)


def test_stage_timer_logs_error_once(caplog):
    logger = logging.getLogger("perf.stage")
    with caplog.at_level(logging.ERROR, logger=logger.name):
        with pytest.raises(RuntimeError):
            with StageTimer("embed", doc_id="doc-1"):
                raise RuntimeError("boom")
    entries = [msg for msg in caplog.messages if "embed" in msg]
    assert len(entries) == 1
    assert "boom" in entries[0]


def test_stage_timer_log_helper(caplog):
    logger = logging.getLogger("perf.stage")
    timer = StageTimer("ingest", user_id="user-9")
    with caplog.at_level(logging.INFO, logger=logger.name):
        with timer:
            timer.log("progress", step=1)
    assert any("progress" in msg for msg in caplog.messages)
