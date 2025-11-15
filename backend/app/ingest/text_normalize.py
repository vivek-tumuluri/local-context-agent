import hashlib
import re
import unicodedata
from typing import Optional

_CRLF = re.compile(r"\r\n?")
_WHITESPACE_RUN = re.compile(r"[ \t]+")
_BLANK_LINES = re.compile(r"\n{3,}")
_TRAILING_WS = re.compile(r"[ \t]+\n")
_ZERO_WIDTH = re.compile(r"[\u200B-\u200D\uFEFF]")

def normalize_text(txt: Optional[str]) -> str:
    if not txt:
        return ""
    txt = unicodedata.normalize("NFC", txt)
    txt = _CRLF.sub("\n", txt)
    txt = txt.replace("\u00A0", " ")
    txt = _ZERO_WIDTH.sub("", txt)
    txt = _WHITESPACE_RUN.sub(" ", txt)
    txt = _TRAILING_WS.sub("\n", txt)
    txt = _BLANK_LINES.sub("\n\n", txt)
    return txt.strip()

def sha256_text(txt: str) -> str:
    return hashlib.sha256(txt.encode("utf-8")).hexdigest()

def compute_content_hash(raw_text: Optional[str]) -> str:
    return sha256_text(normalize_text(raw_text or ""))
