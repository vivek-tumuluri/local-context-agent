from app.rag.chunk import chunk_text


def test_chunk_text_generates_structured_chunks():
    text = "# Heading\nParagraph one sentences. Another sentence.\n\nParagraph two."
    meta = {"id": "doc-1", "title": "Doc", "source": "drive"}
    chunks = chunk_text(text, meta=meta, target_tokens=50, overlap_tokens=0)

    assert chunks, "expected at least one chunk"
    first = chunks[0]
    assert first["id"].startswith("doc-1:")
    assert first["meta"]["chunk_index"] == 0
    assert first["meta"]["source"] == "drive"
    assert "Heading" in first["text"]

    for chunk in chunks:
        assert "n_tokens" in chunk["meta"]
        assert chunk["meta"]["n_tokens"] > 0
