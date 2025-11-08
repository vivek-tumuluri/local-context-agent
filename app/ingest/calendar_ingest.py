import datetime as dt
from fastapi import APIRouter
from googleapiclient.discovery import build
from ..auth import creds_from_session
from ..rag.chunk import chunk_text
from ..rag.vector import upsert as upsert_chunks

router = APIRouter(prefix="/ingest/calendar", tags=["ingest"])

@router.post("")
def ingest_calendar(session: str, months: int = 6):
    creds = creds_from_session(session)
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


        meta = {"source": "calendar", "title": title, "id": e["id"]}
        chunks = chunk_text(text, meta=meta)
        upsert_chunks(chunks)

    return {"ingested": len(events)}
