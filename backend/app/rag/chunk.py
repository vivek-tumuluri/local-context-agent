from __future__ import annotations
import re
from typing import Dict, List, Tuple, Optional



try:
    import tiktoken
    _ENC = tiktoken.get_encoding("cl100k_base")
    def _count_tokens(s: str) -> int:
        return len(_ENC.encode(s or ""))
except Exception:
    _ENC = None
    def _count_tokens(s: str) -> int:

        return max(1, (len(s or "") + 3) // 4)



_WS = re.compile(r"[ \t\f\v]+")
def _normalize(text: str) -> str:
    if not text:
        return ""

    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = _WS.sub(" ", text)

    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()



_HDR = re.compile(r"^(#{1,6})\s+(.*)$", re.MULTILINE)
_CODE_FENCE = re.compile(r"^```.*?$", re.MULTILINE)

def _split_markdown_sections(text: str) -> List[Tuple[str, str]]:
    """
    Split into (heading, body) sections. If no headings, returns one section ("", text).
    Keeps code blocks intact inside bodies.
    """
    if not text:
        return [("", "")]


    sections: List[Tuple[str, str]] = []
    matches = list(_HDR.finditer(text))
    if not matches:
        return [("", text)]


    for i, m in enumerate(matches):
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        heading = m.group(0).strip()
        body = text[start:end]

        body = body[len(heading):].lstrip("\n")
        sections.append((heading, body))

    return sections


def _split_paragraphs_preserving_code(body: str) -> List[str]:
    """
    Split a body into paragraphs while keeping fenced code blocks as atomic paragraphs.
    """
    if not body:
        return []

    parts: List[str] = []
    idx = 0
    lines = body.split("\n")
    in_code = False
    buf: List[str] = []

    for line in lines:
        if line.strip().startswith("```"):

            if not in_code:

                if buf and any(s.strip() for s in buf):
                    parts.append("\n".join(buf).strip())
                buf = [line]
                in_code = True
            else:

                buf.append(line)
                parts.append("\n".join(buf).strip())
                buf = []
                in_code = False
            continue

        if in_code:
            buf.append(line)
            continue


        if line.strip() == "":

            if buf and any(s.strip() for s in buf):
                parts.append("\n".join(buf).strip())
                buf = []
        else:
            buf.append(line)

    if buf and any(s.strip() for s in buf):
        parts.append("\n".join(buf).strip())


    return parts or [body.strip()]




_SENT_RE = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9\"'(\[])")

def _split_sentences(p: str) -> List[str]:
    p = p.strip()
    if not p:
        return []

    if p.startswith("```") and p.endswith("```"):
        return [p]
    sents = _SENT_RE.split(p)

    if not sents or sum(len(s) for s in sents) < len(p) * 0.6:
        return [p]
    return [s.strip() for s in sents if s.strip()]



def _pack_blocks(blocks: List[str], target_tokens: int, overlap_tokens: int) -> List[str]:
    """
    Greedy packer that groups small blocks (sentences/paras) up to target_tokens.
    Adds tail overlap to improve recall.
    """
    if target_tokens <= 0:
        target_tokens = 300
    if overlap_tokens < 0:
        overlap_tokens = 0

    chunks: List[str] = []
    cur: List[str] = []
    cur_tok = 0

    def flush_with_overlap():
        nonlocal cur, cur_tok
        if not cur:
            return
        chunk = "\n".join(cur).strip()
        chunks.append(chunk)
        if overlap_tokens > 0:


            tail = []
            tok_sum = 0
            for piece in reversed(cur):
                if tok_sum >= overlap_tokens:
                    break
                tail.append(piece)
                tok_sum += _count_tokens(piece)
            tail.reverse()
            cur = tail[:]
            cur_tok = sum(_count_tokens(x) for x in cur)
        else:
            cur = []
            cur_tok = 0

    for b in blocks:
        btok = _count_tokens(b)
        if btok > target_tokens * 1.1:

            text = b
            step_chars = max(500, int(len(text) * 0.25))
            s = 0
            while s < len(text):
                piece = text[s : s + step_chars]
                s += step_chars

                ptok = _count_tokens(piece)
                if cur_tok + ptok > target_tokens and cur:
                    flush_with_overlap()
                cur.append(piece)
                cur_tok += ptok
            continue

        if cur_tok + btok > target_tokens and cur:
            flush_with_overlap()
        cur.append(b)
        cur_tok += btok

    if cur:
        chunks.append("\n".join(cur).strip())

    return chunks



def chunk_text(
    text: str,
    meta: Dict,
    target_tokens: int = 350,
    overlap_tokens: int = 80,
    sentence_level: bool = True,
) -> List[Dict]:
    """
    Turn a document into token-aware, citation-friendly chunks.

    Args:
      text: raw document text
      meta: at least {"id": <doc_id>, "title": <title>, "source": "drive|calendar|..."}
      target_tokens: max tokens per chunk (approx)
      overlap_tokens: tokens to overlap between adjacent chunks (recall)
      sentence_level: if True, split paragraphs into sentences before packing

    Returns:
      [{"id": "<doc_id>:<idx>", "text": "...", "meta": { ... , "chunk_index": idx, "n_tokens": n}}]
    """
    text = _normalize(text)
    if not text:
        return []

    sections = _split_markdown_sections(text)


    blocks: List[str] = []
    for heading, body in sections:
        head_line = heading.strip()
        para_list = _split_paragraphs_preserving_code(body)


        first = True
        for p in para_list:
            unit_list = _split_sentences(p) if sentence_level else [p]
            if not unit_list:
                continue
            if head_line and first:

                unit_list[0] = f"{head_line}\n{unit_list[0]}"
                first = False
            blocks.extend(unit_list)


    packed = _pack_blocks(blocks, target_tokens=target_tokens, overlap_tokens=overlap_tokens)


    out: List[Dict] = []
    base_id = meta.get("id", "doc")
    for i, chunk in enumerate(packed):
        out.append({
            "id": f"{base_id}:{i}",
            "text": chunk,
            "meta": {
                **meta,
                "chunk_index": i,
                "n_tokens": _count_tokens(chunk),
            }
        })
    return out

