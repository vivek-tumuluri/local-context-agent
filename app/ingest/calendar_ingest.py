import datetime as dt
from fastapi import APIRouter, Depends
from googleapiclient.discovery import build
from sqlalchemy.orm import Session

from app.db import get_db
from ..auth import csrf_protect, get_current_user, get_google_credentials_for_user
from ..rag.chunk import chunk_text
from ..rag.vector import upsert as upsert_chunks

router = APIRouter(prefix="/ingest/calendar", tags=["ingest"])

@router.post("")
def ingest_calendar(
    months: int = 6,
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
    _csrf=Depends(csrf_protect),
):
    creds = get_google_credentials_for_user(db, user.user_id)
    svc = build("calendar", "v3", credentials=creds)

    now = dt.datetime.utcnow().isoformat() + "Z"
    until = (dt.datetime.utcnow() + dt.timedelta(days=30 * months)).isoformat() + "Z"

    events = svc.events().list(
        calendarId="primary",
        timeMin=now,
        timeMax=until,
        singleEvents=True,
        orderBy="startTime"
    ).execute().get("items", [])

    for e in events:
        title = e.get("summary", "(no title)")
        start = e.get("start", {}).get("dateTime") or e.get("start", {}).get("date")
        end = e.get("end", {}).get("dateTime") or e.get("end", {}).get("date")
        loc = e.get("location", "")
        desc = e.get("description", "") or ""

        text = (
            f"Event: {title}\n"
            f"Start: {start}\n"
            f"End: {end}\n"
            f"Location: {loc}\n"
            f"Description: {desc}"
        )


        meta = {"source": "calendar", "title": title, "id": e["id"], "user_id": user.user_id}
        chunks = chunk_text(text, meta=meta)
        upsert_chunks(chunks, user_id=user.user_id)

    return {"ingested": len(events)}
