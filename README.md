# Local Context Agent

A full-stack RAG application that connects to a user's Google Drive and Calendar, indexes private context into PostgreSQL/pgvector, and answers questions with retrieved source evidence.

## Overview

Local Context Agent is a personal knowledge assistant built around private user data. It handles Google OAuth, encrypted credential storage, Drive and Calendar ingestion, document parsing, chunking, OpenAI embeddings, pgvector storage, hybrid retrieval, and grounded answer generation.

The project is designed as a realistic full-stack system rather than a single notebook demo: ingestion runs through background jobs, documents are tracked in relational metadata tables, retrieval uses both vector and lexical signals, and the frontend exposes auth, ingest status, search, answer, and "Relevant now" workflows.

## Features

- Google OAuth login with session cookies and CSRF protection.
- Encrypted Google credential storage using Fernet.
- Google Drive ingestion with MIME filtering, file-size guardrails, content hashing, chunking, embedding, and job progress tracking.
- Google Calendar ingestion for recent/upcoming events.
- PostgreSQL + pgvector storage for document chunks and embeddings.
- Hybrid retrieval that combines vector search with lexical ranking signals.
- RAG answer endpoint with source-aware context packing, reranking, per-document caps, confidence metadata, and citation handling.
- "Relevant now" suggestions that match upcoming calendar events to related indexed documents.
- React/Vite dashboard for connecting Google, starting ingests, monitoring jobs, searching, and asking questions.
- Backend test suite with isolated test database safeguards and opt-in pgvector integration tests.

## Architecture

```text
React / Vite frontend
        |
        v
FastAPI backend
  |     |      |
  |     |      +--> OpenAI embeddings + chat completions
  |     +---------> Google OAuth / Drive / Calendar APIs
  |
  +--> PostgreSQL + pgvector
  |
  +--> Redis / RQ worker for ingestion jobs
```

Major components:

- `frontend/`: React dashboard and API client.
- `backend/app/api/`: FastAPI app construction and middleware.
- `backend/app/core/`: settings, auth/session handling, database models, limits, logging, and runtime guards.
- `backend/app/ingest/`: Drive and Calendar ingestion pipelines, parsing, chunking, job queue integration, and text normalization.
- `backend/app/rag/`: pgvector storage, retrieval, hybrid ranking, and prompt/context utilities.
- `backend/app/routes/`: auth, ingest, jobs, health, RAG, and relevant-now routes.
- `backend/tests/`: pytest suite covering auth, ingestion, RAG, queues, settings, multitenancy, and API routes.
- `migrations/`: Alembic migrations for the relational schema and `doc_chunks` vector table.

## How It Works

1. A user signs in through `/auth/google`.
2. The backend completes OAuth at `/auth/google/callback`, stores encrypted Google credentials, creates a server-side session, and sets a CSRF token.
3. The user starts Drive or Calendar ingestion from the dashboard.
4. The backend creates an `ingestion_jobs` row and queues work through Redis/RQ.
5. The worker lists source items, filters unsupported or oversized files, parses supported documents, normalizes text, chunks content, embeds chunks with OpenAI, and stores metadata in `content_index` plus vectors in `doc_chunks`.
6. The user searches or asks a question through `/rag/search` or `/rag/answer`.
7. The backend embeds the query, retrieves relevant chunks from pgvector, merges lexical and vector candidates, reranks results, builds a grounded context, and calls the answer model.
8. The frontend displays the answer, source metadata, job history, and relevant context cards.

## Tech Stack

| Layer | Technology |
|---|---|
| Frontend | React 19, Vite |
| Backend API | FastAPI, Pydantic |
| Database | PostgreSQL, pgvector, SQLAlchemy |
| Jobs | Redis, RQ |
| AI | OpenAI embeddings and chat completions |
| Auth | Google OAuth, server-side sessions, CSRF cookies |
| Parsing | pypdf, DOCX XML parsing, CSV/text parsing |
| Testing | pytest, pytest-asyncio |
| DevOps | Docker Compose, Alembic |

## Repository Structure

```text
backend/
  app/
    api/          FastAPI app setup
    core/         settings, auth, DB models, limits, logging
    ingest/       Drive/Calendar ingest, parsers, chunking, queue logic
    rag/          pgvector storage, retrieval, ranking, prompt helpers
    routes/       HTTP route modules
  scripts/        table creation, worker entrypoint, debugging helpers
  tests/          backend test suite
frontend/
  src/            React dashboard and API client
migrations/
  versions/       Alembic migrations
docker-compose.yml
```

## Prerequisites

- Docker and Docker Compose.
- Node.js 18+ for local frontend development.
- Google Cloud OAuth credentials.
- OpenAI API key.
- PostgreSQL with the `vector` extension if running outside Docker.
- Redis if running the worker outside Docker.

## Environment Variables

Create a `.env` file in the repository root. Start from `.env.example`, then adjust the database host depending on how you run the app.

Required variables:

```env
ENV=local
DATABASE_URL=postgresql://postgres:postgres@db:5432/local_context
REDIS_URL=redis://redis:6379/0

GOOGLE_CLIENT_ID=
GOOGLE_CLIENT_SECRET=
OAUTH_REDIRECT_URI=http://localhost:8000/auth/google/callback

OPENAI_API_KEY=

SESSION_SECRET=
DRIVE_CREDENTIALS_KEY=
```

Important notes:

- For Docker Compose, use `db` and `redis` as hosts, as shown above.
- For running the backend directly on your host machine, use `localhost`:
  - `DATABASE_URL=postgresql://postgres:postgres@localhost:5432/local_context`
  - `REDIS_URL=redis://localhost:6379/0`
- `SESSION_SECRET` must be at least 32 characters in production-like environments.
- `DRIVE_CREDENTIALS_KEY` must be a valid Fernet key because Google credentials are encrypted before storage.

Generate local secrets:

```bash
python - <<'PY'
import secrets
from cryptography.fernet import Fernet

print("SESSION_SECRET=" + secrets.token_urlsafe(48))
print("DRIVE_CREDENTIALS_KEY=" + Fernet.generate_key().decode())
PY
```

Common optional variables:

- `ANSWER_MODEL`, `EMBED_MODEL`, `EMBED_DIM`
- `EMBED_BATCH_SIZE`, `EMBED_MAX_RETRIES`, `EMBED_BASE_BACKOFF`
- `RAG_RETRIEVAL_K`, `RAG_MAX_CHUNKS_PER_DOC`, `RAG_MIN_CONFIDENCE`
- `DRIVE_CHUNK_TARGET_TOKENS`, `DRIVE_CHUNK_OVERLAP_TOKENS`
- `MAX_INGESTS_PER_USER_PER_DAY`, `MAX_RAG_REQUESTS_PER_DAY`
- `AZERYN_MAX_FILE_BYTES`, `AZERYN_MAX_CHUNKS_PER_FILE`, `AZERYN_MAX_TOKENS_PER_JOB`
- `READ_ONLY_MODE`
- `ALLOW_INLINE_INGEST`
- `VITE_API_BASE_URL` for the frontend, defaulting to `http://localhost:8000`

## Google OAuth Setup

1. Create a Google Cloud project.
2. Enable these APIs:
   - Google Drive API
   - Google Calendar API
3. Create an OAuth 2.0 Web Client.
4. Add this authorized redirect URI for local development:

```text
http://localhost:8000/auth/google/callback
```

5. Put the client ID and secret into `.env`.

The app requests these scopes:

- `openid`
- `https://www.googleapis.com/auth/userinfo.email`
- `https://www.googleapis.com/auth/userinfo.profile`
- `https://www.googleapis.com/auth/drive.readonly`
- `https://www.googleapis.com/auth/calendar.readonly`

## Running Locally With Docker

1. Create `.env`.

```bash
cp .env.example .env
```

2. For Docker Compose, make sure `.env` uses container hosts:

```env
DATABASE_URL=postgresql://postgres:postgres@db:5432/local_context
REDIS_URL=redis://redis:6379/0
```

3. Start Postgres, Redis, the API, and the worker.

```bash
docker compose up --build -d
```

4. Create tables and pgvector indexes.

```bash
docker compose exec api python -m scripts.create_tables
```

Alternatively, run Alembic migrations:

```bash
docker compose exec api alembic upgrade head
```

5. Confirm the API is healthy.

```bash
curl http://localhost:8000/healthz
```

6. Start the frontend.

```bash
cd frontend
npm install
npm run dev
```

7. Open:

```text
http://localhost:5173
```

## Running Without Docker

Use this path if you want to run the backend directly on your host machine.

1. Install backend dependencies.

```bash
python3 -m venv .venv
source .venv/bin/activate
cd backend
pip install -r requirements.txt
```

2. Run local Postgres with pgvector and Redis.

3. Use host-based URLs in `.env`.

```env
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/local_context
REDIS_URL=redis://localhost:6379/0
```

4. Create tables.

```bash
cd backend
set -a
source ../.env
set +a
python -m scripts.create_tables
```

5. Run the API.

```bash
cd backend
set -a
source ../.env
set +a
uvicorn app.api.main:app --host 0.0.0.0 --port 8000
```

6. Run the worker in another terminal.

```bash
cd backend
set -a
source ../.env
set +a
python -m scripts.worker
```

7. Run the frontend.

```bash
cd frontend
npm install
npm run dev
```

## Database and Storage

The main tables are:

- `users`: Google user profile records.
- `user_sessions`: hashed session tokens and expiration metadata.
- `drive_sessions`: encrypted Google OAuth credentials.
- `content_index`: source document/event metadata, hashes, modified times, and ingestion metadata.
- `doc_chunks`: chunk text, metadata, and pgvector embeddings.
- `ingestion_jobs`: queued/running/completed ingest jobs and metrics.
- `source_state`: per-source cursor and sync state.

`doc_chunks.embedding` uses pgvector with the configured embedding dimension. The default embedding model is `text-embedding-3-small` with `EMBED_DIM=1536`.

## Ingesting Documents

Use the dashboard after signing in with Google:

1. Connect your Google account.
2. Start a Drive ingest.
3. Monitor the ingest job from the dashboard.
4. Ask questions after the job succeeds.

Drive ingestion currently supports the default allowlist from `backend/app/core/settings.py`:

- Google Docs
- Google Sheets
- Google Slides
- PDF
- DOC/DOCX
- plain text
- Markdown
- CSV/TSV

Unsupported MIME types are skipped. Files above `AZERYN_MAX_FILE_BYTES` are skipped before parsing. Incremental ingest uses metadata and content hashes to avoid reprocessing unchanged files. Use `reembed_all` when you intentionally need to rebuild vectors for already-indexed documents.

Calendar ingestion is available through the dashboard/API and indexes calendar events as searchable context.

## Asking Questions

After ingestion, use the dashboard search and answer UI.

Relevant backend endpoints:

- `POST /rag/search`: returns retrieved chunks and source metadata.
- `POST /rag/answer`: retrieves chunks, builds grounded context, calls the answer model, and returns an answer with sources.
- `GET /relevant/now`: finds upcoming calendar events and related Drive documents.

If `content_index` has records but `doc_chunks` is empty, the app may show that it cannot find anything in your docs. In that case, rebuild vectors with a forced re-embed.

## API Reference

Auth:

- `GET /auth/google`
- `GET /auth/google/callback`
- `GET /auth/me`
- `GET /auth/csrf`
- `POST /auth/disconnect`

Ingestion:

- `POST /ingest/drive/start`
- `POST /ingest/calendar/start`
- `GET /ingest/jobs`
- `GET /ingest/jobs/{job_id}`

Retrieval:

- `POST /rag/search`
- `POST /rag/answer`
- `GET /relevant/now`

Ops:

- `GET /healthz`

Local-only inline routes also exist for development when the app is running in local mode.

## Testing

Run backend tests:

```bash
docker compose run --rm api pytest
```

or, from a local backend environment:

```bash
cd backend
pytest
```

Default test behavior:

- Forces an isolated SQLite database at `/tmp/local_context_agent_test.db`.
- Blocks network access unless `ALLOW_NETWORK=1` is set.
- Skips pgvector integration tests unless explicitly enabled.
- Refuses known live database URLs, including Supabase pooler URLs.

Run the pgvector integration test only against a disposable test database:

```bash
TEST_DATABASE_URL=postgresql://localhost:5432/local_context_test \
ALLOW_PGVECTOR_INTEGRATION_TESTS=1 \
pytest tests/rag/test_vector_pg.py
```

Never point tests at a real app database.

## Security Notes

- Google OAuth credentials are encrypted before storage using `DRIVE_CREDENTIALS_KEY`.
- Session tokens are stored server-side as hashes.
- Cookie-based requests use CSRF protection with the `X-CSRF-Token` header by default.
- `READ_ONLY_MODE=1` blocks write operations.
- `/auth/disconnect` deletes user content metadata, source state, sessions, jobs, credentials, and vector chunks for that user.
- `.env` contains secrets and should never be committed.
- The test suite includes safeguards to prevent destructive tests from running against live databases.

## Known Limitations

- Drive ingestion handles common document formats, but many media and application-specific MIME types are intentionally skipped.
- Oversized files are skipped based on configured byte limits.
- Calendar ingestion exists, but Drive ingestion is the more mature pipeline.
- Observability is mostly structured logs and job metrics; full tracing/metrics dashboards are not included.
- Production deployment would need stronger operational hardening around migrations, backups, secret management, rate limits, monitoring, and incident recovery.
- Ingestion resilience is still an area for improvement: the desired direction is per-document/per-chunk fallback and quarantine when a batch contains poisoned input.

## Roadmap

- Add production-style ingestion failure isolation with document/chunk quarantine.
- Improve skipped-file reporting in the dashboard.
- Add health diagnostics for `content_index` versus `doc_chunks` mismatches.
- Add more end-to-end tests around real worker retries and batch failure handling.
- Expand retrieval evaluation and ranking diagnostics.
- Add deployment documentation for a real hosted environment.

## Engineering Highlights

This project demonstrates:

- Full-stack product development with React and FastAPI.
- OAuth integration, encrypted credential handling, sessions, and CSRF protection.
- Background ingestion with Redis/RQ and persistent job state.
- Document ingestion, parsing, chunking, embeddings, and pgvector retrieval.
- Hybrid lexical/vector retrieval and grounded answer generation.
- Multi-user data scoping across auth, retrieval, and ingestion.
- Practical test safety work to prevent destructive database operations during automated tests.
