import random

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


def test_chunker_preserves_headings_across_random_sections():
    random.seed(42)
    sections = []
    for idx in range(6):
        sections.append(f"# Section {idx}")
        body = " ".join(random.choice(["alpha", "beta", "gamma", "delta"]) for _ in range(120))
        sections.append(body)
    text = "\n".join(sections)
    chunks = chunk_text(text, meta={"id": "doc-2", "title": "Doc", "source": "drive"}, target_tokens=80, overlap_tokens=20)
    for idx in range(6):
        assert any(f"Section {idx}" in chunk["text"] for chunk in chunks)
