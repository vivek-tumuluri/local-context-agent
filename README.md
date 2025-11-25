# Local Context Agent (Azeryn)
Self-hosted RAG assistant for Google Drive and Calendar. The backend ingests your content, embeds it with OpenAI, stores vectors in Postgres/pgvector, and serves grounded answers and search. The Vite/React frontend handles Google auth, ingest controls, chat with citations, search, activity history, and “Relevant now” suggestions tied to upcoming events.

## Highlights
- Google OAuth2 with CSRF-protected cookies and encrypted token storage.
- Drive ingest with incremental cursors, content hashing, chunking, and batching; runs via Redis/RQ worker with an inline fallback allowed only in `ENV=local` when `ALLOW_INLINE_INGEST=1`.
- Calendar ingest pipeline that syncs a rolling window (30 days back, 180 forward), chunks events, embeds them, and cleans up cancelled items; can be forced to re-embed.
- RAG endpoints: `/rag/search` for similarity results and `/rag/answer` for grounded answers with reranking, per-doc caps, confidence scores, and optional source filters.
- “Relevant now” pairs upcoming calendar events with matching Drive documents.
- Ingestion jobs tracked in Postgres with progress, quotas, and a read-only safety switch.

## Components
- Backend: FastAPI app in `backend/app` (entrypoint `app.api.main:app`); SQLAlchemy models in `app/core/models.py`; pgvector-backed vector store in `app/rag/vector_pg.py`.
- Worker: RQ worker (`python -m scripts.worker`) consuming the `ingest` queue for Drive/Calendar jobs.
- Frontend: Vite + React (React 19) in `frontend` with Google login, ingest controls, chat/search UI, activity timeline, and relevant-now cards.
- Database: Postgres with the `vector` extension for production. SQLite is supported only for local/testing; pgvector is required when `ENV` is not `local`.

## Prerequisites
- Python 3.11+, Node 18+
- Postgres with `vector` extension (required outside local dev)
- Redis (for quotas and the RQ worker)
- Google OAuth client (Drive + Calendar scopes)
- OpenAI API key with embedding + chat access

## Configuration (`.env` at repo root)
Required
- `DATABASE_URL` – Postgres URI (`postgresql://user:pass@host:5432/local_context`)
- `SESSION_SECRET` – strong random string (>=32 chars)
- `DRIVE_CREDENTIALS_KEY` – Fernet key used to encrypt Google tokens (required when `ENV`/`ENVIRONMENT` is not `local`)
- `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `OAUTH_REDIRECT_URI`
- `OPENAI_API_KEY`

Common options
- `ENV` / `ENVIRONMENT` (default `local`), `READ_ONLY_MODE` (blocks writes), `ALLOW_INLINE_INGEST` (inline ingest when queue unavailable)
- `REDIS_URL` (default `redis://localhost:6379/0`) for quotas + worker
- Models/tuning: `ANSWER_MODEL`, `EMBED_MODEL`, `EMBED_DIM`, `EMBED_BATCH_SIZE`, `EMBED_MAX_RETRIES`, `EMBED_BASE_BACKOFF`, `RAG_RETRIEVAL_K`, `RAG_MAX_CHUNKS_PER_DOC`, `RAG_MIN_CONFIDENCE`
- Chunking: `DRIVE_CHUNK_TARGET_TOKENS`, `DRIVE_CHUNK_OVERLAP_TOKENS`, `RAG_MAX_CTX_CHARS`, `RAG_SEARCH_MIN_CHARS`, `RAG_SEARCH_SKIP_TRASHED`
- Quotas: `MAX_INGESTS_PER_USER_PER_DAY`, `MAX_RAG_REQUESTS_PER_DAY`
- Cookies: `SESSION_COOKIE_NAME`, `CSRF_COOKIE_NAME`, `CSRF_HEADER_NAME`, `SESSION_COOKIE_SECURE`, `SESSION_COOKIE_SAMESITE`
- Frontend: `VITE_API_BASE_URL` (defaults to `http://localhost:8000`)

Sample templates: `.env.example` (local) and `.env.production.example`.

## Local setup (without Docker)
1) Backend deps
```bash
python3 -m venv .venv
source .venv/bin/activate
cd backend
pip install -r requirements.txt
```
2) Copy `.env.example` to `.env` in the repo root and fill secrets.
3) Create tables and indexes (enables pgvector when Postgres is used):
```bash
cd backend
python -m scripts.create_tables
```
4) Run the API
```bash
cd backend
source ../.env
uvicorn app.api.main:app --host 0.0.0.0 --port 8000
```
5) Run the worker (needs Redis) if you want queued ingest:
```bash
cd backend
source ../.env
python -m scripts.worker
```
Inline ingest is only available in local mode when `ALLOW_INLINE_INGEST=1`.
6) Frontend
```bash
cd frontend
npm install
npm run dev -- --host --port 5173
```
Open http://localhost:5173 and sign in with Google.

## Docker Compose (API + worker + Postgres + Redis)
```bash
cp .env.example .env  # or .env.production.example
docker compose up -d db redis
docker compose up --build api worker
docker compose exec api python -m scripts.create_tables  # or: alembic upgrade head
curl http://localhost:8000/healthz
```
The compose file builds from `backend/Dockerfile` and reuses the API image for the worker. Default ports: API 8000, Postgres 5432, Redis 6379.

## How ingestion works
- Drive: `/ingest/drive/start` creates a job, queues it to Redis/RQ when available, and falls back to inline ingest only in local mode. Listing uses Google Drive API with incremental cursors (`source_state` table), skips duplicates via content hashes, chunks text, batches embeddings, and stores metadata in `content_index` plus vectors in `doc_chunks`.
- Calendar: `/ingest/calendar/start` enqueues a rolling sync over recent/pending events. Events are chunked, embedded, written to `content_index`/`doc_chunks`, and cancelled events purge their chunks. The inline `/ingest/calendar` endpoint exists for local/manual use but the queue path is the primary flow.
- Jobs: Progress stored in `ingestion_jobs` with `/ingest/jobs` and `/ingest/jobs/{job_id}` for status. Quotas enforced via Redis; `READ_ONLY_MODE=1` blocks writes.

## Retrieval + recommendations
- `/rag/search`: similarity search with reranking, per-doc chunk caps, and optional source filter (`drive` or `calendar`).
- `/rag/answer`: grounded answers using retrieved context and OpenAI chat completions; includes citations and confidence.
- `/relevant/now`: pulls upcoming calendar events (next 24h) and returns top Drive docs per event.

## Auth + session flow
- `/auth/google` → `/auth/google/callback` completes OAuth, encrypts/persists credentials, and issues a session cookie plus CSRF token.
- `/auth/me` returns the user profile and CSRF token for the frontend; `/auth/disconnect` deletes data and revokes credentials.
- CSRF header defaults to `X-CSRF-Token`; CORS allows `http://localhost:5173` by default.

## API quick reference
- Auth: `/auth/google`, `/auth/google/callback`, `/auth/me`, `/auth/csrf`, `/auth/disconnect`
- Ingest: `/ingest/drive/start`, `/ingest/calendar/start`, `/ingest/jobs`, `/ingest/jobs/{job_id}` (inline `/ingest/drive` and `/ingest/calendar` exist for local)
- Retrieval: `/rag/search`, `/rag/answer`, `/relevant/now`
- Ops: `/healthz`

## Testing
Backend tests:
```bash
cd backend
pytest
```
Tests default to SQLite and block network access; set `ALLOW_NETWORK=1` to permit network calls during testing.
