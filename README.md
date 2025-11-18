# Local Context Agent

Production-ready personal RAG stack. FastAPI backend with pgvector, OpenAI embeddings/chat, RQ worker for ingestion, and a Vite/React frontend for Google auth, ingest, and Q&A.

## High-Level Overview
- Sign in with Google → obtain Drive/Calendar access tokens (stored encrypted).
- Ingest Google Drive/Calendar items → chunk text → embed with OpenAI → store chunks + vectors in Postgres (pgvector).
- Query via `/rag/search` and `/rag/answer` → fetch top-k similar chunks from pgvector → format answers with citations using OpenAI chat.
- Optional RQ worker handles ingest asynchronously; fallback inline ingest also available.

## Architecture
- **Backend:** FastAPI + SQLAlchemy. Routes for auth, ingest (Drive, Calendar), RAG search/answer, health/jobs.
- **Vector store:** Postgres + pgvector (DocChunk.embedding). ivfflat index created during init. No Chroma codepaths.
- **Ingestion:** Drive and Calendar pipelines → chunk ➜ embed (OpenAI) ➜ store chunks/metadata + vectors in Postgres.
- **Frontend:** Vite/React app for login, ingest controls, and chat retrieval UI.
- **Workers:** Optional RQ worker for async ingest; can also run inline.
- **Tests:** Pytest with fakes for OpenAI and Google; network blocked unless `ALLOW_NETWORK=1`.

## Prerequisites
- Python 3.10+, Node 18+
- Postgres with `vector` extension available
- OpenAI API key
- Google OAuth client (Drive read scope)
- Redis (for RQ worker)

## Environment (key vars)
- `DATABASE_URL` (Postgres URI)
- `OPENAI_API_KEY`
- `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `OAUTH_REDIRECT_URI`
- `SESSION_SECRET`, `REDIS_URL`
- Embedding controls: `EMBED_MODEL`, `EMBED_BATCH_SIZE`, `MAX_CHARS_PER_CHUNK`, `EMBED_MAX_RETRIES`, `EMBED_BASE_BACKOFF`

## Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
cd backend
pip install -r requirements.txt
python -m scripts.create_tables   # enables pgvector extension, creates tables, builds ivfflat index
```

## Run
Backend API:
```bash
cd backend
source ../.env
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Worker (optional, for queued ingest):
```bash
cd backend
source ../.env
python -m scripts.worker
```

Frontend:
```bash
cd frontend
npm install
npm run dev -- --host --port 5173
```
Visit http://localhost:5173/.

## Testing
```bash
cd backend
pytest
```
Network calls blocked unless `ALLOW_NETWORK=1`.

## Notes
- Vectors live in `doc_chunks.embedding` (pgvector). SQLite test runs skip index creation and use an in-Python distance fallback.
- Rebuild locally: drop/truncate Postgres tables as needed, rerun `python -m scripts.create_tables`, reingest.***
