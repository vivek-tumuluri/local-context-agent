# Local Context Agent (Azeryn)
A self-hostable RAG assistant for your Google data. The backend ingests Drive files and Calendar events, embeds them with OpenAI, stores vectors in Postgres/pgvector, and serves grounded Q&A. The Vite/React frontend handles Google auth, ingestion controls, chat with citations, and “Relevant now” suggestions for upcoming meetings.

## What it does
- Google OAuth with encrypted token storage and CSRF-protected session cookies.
- Drive ingest with incremental cursoring, content hashing, and chunked embeddings; Calendar ingest that turns upcoming events into searchable context.
- Retrieval endpoints for similarity search and grounded answers with citations; “Relevant now” pairs upcoming events with matching Drive docs.
- Ingestion jobs tracked in the database; optional Redis/RQ worker for background processing with inline fallback.
- Health, quota, and read-only switches for safer operations.

## Architecture (code map)
- Backend (`backend/app`): FastAPI app (`app/api/main.py`) with routers for auth, ingest (Drive + Calendar), RAG search/answer, relevant-now, health, and job status.
- Data layer: SQLAlchemy models (`app/core/models.py`), Postgres + `pgvector` for `doc_chunks.embedding`; `scripts/create_tables.py` enables the extension and IVFFlat index.
- Ingestion: Drive pipeline (`app/ingest/drive_pipeline.py`) handles listing, dedupe, chunking, batching embeddings, and metadata upserts; Calendar ingest (`app/ingest/calendar_ingest.py`) converts events into chunks. Job helpers (`app/ingest/job_helper.py`) persist progress; optional queue worker in `app/ingest/queue.py` / `scripts/worker.py`.
- Retrieval: `app/rag/vector_pg.py` for embedding + similarity search, `app/routes/rag_routes.py` for `/rag/search` and `/rag/answer`, and `app/routes/relevant_routes.py` for event-aware recommendations.
- Frontend (`frontend/src`): Vite/React app with `LoginPage` (Google sign-in via `/auth/google`) and `Dashboard` for ingest controls, chat UI, activity timeline, and relevant-now suggestions.

## Prerequisites
- Python 3.10+, Node 18+
- Postgres with the `vector` extension (production); SQLite works for local dev only when `ENVIRONMENT=local`
- OpenAI API key with embedding + chat access
- Google OAuth client (Drive read-only + Calendar read-only scopes)
- Redis (for quotas and the optional RQ worker)

## Environment variables (root `.env`)
Required:
- `DATABASE_URL` – Postgres URI (e.g., `postgresql://user:pass@localhost:5432/local_context`)
- `SESSION_SECRET` – strong random string (>=32 chars)
- `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `OAUTH_REDIRECT_URI`
- `OPENAI_API_KEY`
- `DRIVE_CREDENTIALS_KEY` – Fernet key to encrypt Google tokens (required when `ENVIRONMENT` is not `local`)

Common options:
- `ENVIRONMENT` (default `local`), `READ_ONLY_MODE` (block writes), `ALLOW_INLINE_INGEST` (allow ingest without Redis/RQ)
- `REDIS_URL` (default `redis://localhost:6379/0`) for quotas + worker
- Embedding/LLM tuning: `EMBED_MODEL`, `EMBED_DIM`, `EMBED_BATCH_SIZE`, `EMBED_MAX_RETRIES`, `EMBED_BASE_BACKOFF`, `ANSWER_MODEL`, `RAG_MAX_CTX_CHARS`, `RAG_DEFAULT_K`
- Quotas: `MAX_INGESTS_PER_USER_PER_DAY`, `MAX_RAG_REQUESTS_PER_DAY`
- Cookies (optional): `SESSION_COOKIE_NAME`, `SESSION_COOKIE_SECURE`, `SESSION_COOKIE_SAMESITE`, `CSRF_COOKIE_NAME`
- Frontend: `VITE_API_BASE_URL` (defaults to `http://localhost:8000`)

## Setup
1) Create and activate a virtualenv, install backend deps:
```bash
python3 -m venv .venv
source .venv/bin/activate
cd backend
pip install -r requirements.txt
```
2) Configure `.env` at the repo root with the variables above.
3) Initialize the database (creates tables, vector extension, IVFFlat index):
```bash
cd backend
python -m scripts.create_tables
```
4) Install frontend deps:
```bash
cd frontend
npm install
```

## Running locally
Backend API (FastAPI + Uvicorn):
```bash
cd backend
source ../.env
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Background ingestion worker (optional; needs Redis reachable via `REDIS_URL`):
```bash
cd backend
source ../.env
python -m scripts.worker
```

Frontend (Vite/React):
```bash
cd frontend
npm run dev -- --host --port 5173
```
Open http://localhost:5173 and sign in with Google.

## Typical workflow
1) Start backend (and worker if using queued ingest) and frontend.
2) Click “Sign in with Google” → OAuth flow issues a session cookie and CSRF token.
3) Run Drive ingest from the dashboard (or POST `/ingest/drive`); incremental cursors avoid reprocessing unchanged files, and content hashes skip duplicates unless `reembed_all` is set.
4) Optionally ingest Calendar (`POST /ingest/calendar`) to index future events.
5) Ask questions in the chat UI (calls `/rag/answer`) or hit `/rag/search` directly for raw similarity results. Responses include sources with Drive links and confidence.
6) Check “Relevant now” (or GET `/relevant/now`) to see Drive docs tied to upcoming events.
7) View ingestion history at `/ingest/jobs` (also shown in the Activity tab).

## Key endpoints
- Auth: `/auth/google`, `/auth/google/callback`, `/auth/me`, `/auth/csrf`, `/auth/disconnect`
- Ingest: `/ingest/drive`, `/ingest/drive/start` (queued), `/ingest/calendar`, `/ingest/jobs`, `/ingest/jobs/{job_id}`
- Retrieval: `/rag/search`, `/rag/answer`, `/relevant/now`
- Ops: `/healthz`, `/jobs/{job_id}` (legacy job fetch)

## Testing
Backend tests:
```bash
cd backend
pytest
```
Network calls are blocked in tests unless `ALLOW_NETWORK=1`.

## Notes
- Read-only mode (`READ_ONLY_MODE=1`) blocks ingest/auth writes.
- Without Redis, quotas are skipped and ingest falls back to inline execution when `ALLOW_INLINE_INGEST=true`.
- In production, ensure Postgres + `vector` is available and `DRIVE_CREDENTIALS_KEY` is set to keep Google tokens encrypted at rest.
