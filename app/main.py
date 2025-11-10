from fastapi import FastAPI
from dotenv import load_dotenv
from app.routes import jobs

load_dotenv()

app = FastAPI(title="Local Context Agent (Minimal)")

from .auth import router as auth_router
from .ingest.drive_ingest import router as drive_router
from .ingest.calendar_ingest import router as cal_router
from .ingest.routes import router as ingest_router
from .rag.routes import router as rag_router
from .routes.ingest_drive import router as legacy_drive_router

app.include_router(auth_router)
app.include_router(drive_router)
app.include_router(cal_router)
app.include_router(ingest_router)
app.include_router(rag_router)
app.include_router(legacy_drive_router)
app.include_router(jobs.router)

@app.get("/")
def root():
    return {"ok": True}
