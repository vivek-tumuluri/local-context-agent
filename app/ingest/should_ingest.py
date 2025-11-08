from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional, Mapping, Any
from app.models import ContentIndex
from app.ingest.text_normalize import compute_content_hash

def _to_dt(val: Optional[str | datetime]) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    s = str(val).strip()


    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    else:

        if len(s) >= 5 and (s[-5] in "+-") and s[-3] != ":":
            s = s[:-2] + ":" + s[-2:]


    if "T" in s:
        date_part, time_part = s.split("T", 1)
        frac_idx = time_part.find(".")
        if frac_idx != -1:
            tz_idx = max(time_part.find("+", frac_idx), time_part.find("-", frac_idx))
            if tz_idx == -1:
                tz_idx = len(time_part)
            frac = time_part[frac_idx + 1 : tz_idx]
            if len(frac) > 6:
                time_part = time_part[: frac_idx + 1] + frac[:6] + time_part[tz_idx:]
        s = date_part + "T" + time_part

    try:

        if "T" not in s and len(s) == 10:
            return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(s)
    except Exception:
        return None

def should_reingest(
    stored: Optional[ContentIndex],
    incoming_meta: Mapping[str, Any],
    new_text: Optional[str] = None,
) -> bool:
    if stored is None:
        return True

    incoming_trashed = bool(incoming_meta.get("trashed") or incoming_meta.get("is_trashed"))
    if incoming_trashed != bool(stored.is_trashed):
        return True

    inc_mod = _to_dt(
        incoming_meta.get("modifiedTime") or incoming_meta.get("modified_time") or incoming_meta.get("updated")
    )
    if inc_mod:

        if not stored.modified_time or inc_mod > stored.modified_time:
            return True

    inc_ver = incoming_meta.get("version")
    if inc_ver and inc_ver != (stored.version or ""):
        return True

    inc_md5 = incoming_meta.get("md5Checksum") or incoming_meta.get("md5") or incoming_meta.get("etag")
    if inc_md5 and inc_md5 != (stored.md5 or ""):
        return True

    if new_text is not None:

        if compute_content_hash(new_text) != (stored.content_hash or ""):
            return True

    return False
