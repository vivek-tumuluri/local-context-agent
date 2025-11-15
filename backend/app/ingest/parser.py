import io
import csv
import zipfile
from typing import Union
from xml.etree import ElementTree as ET
from pypdf import PdfReader


DOCX_MIMES = {
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
}

CSV_MIMES = {
    "text/csv",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}


def to_text(content: Union[str, bytes], filename: str, mime: str | None = None) -> str:
    if isinstance(content, str):
        return content

    if not isinstance(content, (bytes, bytearray)):
        return ""

    data = bytes(content)
    lower_name = (filename or "").lower()

    try:
        if lower_name.endswith(".pdf") or mime == "application/pdf":
            reader = PdfReader(io.BytesIO(data))
            return "\n".join((page.extract_text() or "") for page in reader.pages)

        if mime in DOCX_MIMES or lower_name.endswith(".docx"):
            return _docx_to_text(data)

        if mime in CSV_MIMES or lower_name.endswith(('.csv', '.tsv')):
            return _csv_to_text(data)

        if mime and mime.startswith("text/"):
            return data.decode("utf-8", errors="ignore")


        return data.decode("utf-8", errors="ignore")
    except Exception:
        return ""


def _docx_to_text(data: bytes) -> str:
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as doc:
            xml = doc.read("word/document.xml")
    except Exception:
        return ""

    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        return ""

    texts: list[str] = []
    namespace_sep = "}"
    for elem in root.iter():
        tag = elem.tag.split(namespace_sep)[-1]
        if tag == "t" and elem.text:
            texts.append(elem.text)
        elif tag in {"p", "br"}:
            texts.append("\n")

    return "".join(texts)


def _csv_to_text(data: bytes) -> str:
    text = data.decode("utf-8", errors="ignore")
    reader = csv.reader(io.StringIO(text))
    rows = [", ".join(col.strip() for col in row if col) for row in reader]
    return "\n".join(r for r in rows if r)
