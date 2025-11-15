from __future__ import annotations

import io
import zipfile

from app.ingest import parser


def _make_docx(text: str) -> bytes:
    xml = (
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        f"<w:p><w:r><w:t>{text}</w:t></w:r></w:p>"
        "</w:document>"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as doc:
        doc.writestr("word/document.xml", xml)
    return buf.getvalue()


def test_to_text_handles_docx_bytes():
    payload = _make_docx("Hello Docx")
    text = parser.to_text(payload, filename="report.docx")
    assert "Hello Docx" in text


def test_to_text_handles_csv_bytes():
    data = b"name,score\nAda,10\nGrace,9"
    text = parser.to_text(data, filename="scores.csv", mime="text/csv")
    lines = text.splitlines()
    assert "name, score" in lines[0]
    assert "Ada" in lines[1]

