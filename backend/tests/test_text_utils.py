from app.ingest.text_normalize import normalize_text, compute_content_hash


def test_normalize_text_cleans_whitespace_and_blank_lines():
    raw = "Hello\r\n\r\n\r\nWorld\t\tExample\u00A0"
    normalized = normalize_text(raw)
    assert normalized == "Hello\n\nWorld Example"


def test_normalize_text_removes_database_unsafe_control_chars():
    raw = "Alpha\x00Beta\x01\nGamma\tDelta"
    normalized = normalize_text(raw)
    assert normalized == "AlphaBeta\nGamma Delta"
    assert "\x00" not in normalized


def test_compute_content_hash_is_stable_for_identical_text():
    text = "Some text to hash"
    assert compute_content_hash(text) == compute_content_hash(text)
    assert compute_content_hash(text) != compute_content_hash(text + " extra")
