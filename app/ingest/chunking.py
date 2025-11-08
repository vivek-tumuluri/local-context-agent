from __future__ import annotations
from typing import List

def split_by_chars(text: str, max_chars: int = 1200, overlap: int = 120) -> List[str]:
    if not text:
        return []
    out, i, n = [], 0, len(text)
    while i < n:
        j = min(i + max_chars, n)
        out.append(text[i:j])
        if j == n:
            break
        i = max(0, j - overlap)
    return out