# Local Context Agent

A FastAPI application that authenticates with Google on behalf of a user, ingests Drive files and Calendar events, and serves retrieval-augmented answers backed by Chroma + OpenAI embeddings. The goal is to provide a minimal but production-minded reference you can extend with your own auth, worker, or LLM stack.

## What You Get
- **Hardened auth** – `/auth` completes the Google OAuth flow, stores refresh + access tokens in SQL (`users`, `user_sessions`, `drive_sessions`), and exposes only HttpOnly cookies / bearer tokens to clients.
- **Unified ingestion pipeline** – Drive and Calendar routes normalize text, hash deduped content, update `ContentIndex`, and upsert embeddings into per-user Chroma collections. Quick runs, background jobs, and legacy wrappers all call the same code paths.
- **Job orchestration** – `/ingest/drive/start` creates `IngestionJob` rows, runs ingest work in the background, and surfaces progress/logs via `/ingest/jobs/*`.
- **RAG endpoints** – `/rag/search` returns ranked chunks with confidence scores; `/rag/answer` uses OpenAI Chat with inline citations.
- **Test coverage** – Pytest suite exercises auth/session helpers, text normalization, chunking, and re-ingest logic so refactors stay safe.

## Architecture at a Glance
```
FastAPI (app/main.py)
├── Auth: app/auth.py + app/google_clients.py
├── Ingestion
│   ├── app/ingest/drive_ingest.py   (Google adapters + unified pipeline entrypoints)
│   ├── app/ingest/calendar_ingest.py
│   ├── app/ingest/drive_pipeline.py (normalize → dedupe → embed → persist)
│   ├── app/ingest/routes.py         (`/ingest/drive/start`, job helpers)
│   └── app/routes/ingest_drive.py   (legacy scripting route; not production ready)
├── Retrieval: app/rag/routes.py + app/rag/vector.py + app/rag/chunk.py
├── Persistence: app/models.py + app/db.py + scripts/create_tables.py
└── Jobs API: app/routes/jobs.py
```

## Prerequisites
- Python 3.10+
- SQLite (default) or any SQL database supported by SQLAlchemy
- Google Cloud OAuth credentials with Drive + Calendar read scopes
- OpenAI API key (embeddings + answers)
- Local filesystem access for the Chroma store (default `./.chroma`)

## Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
python -m scripts.create_tables
```

## Configuration
Put secrets in `.env` (never commit it). Key variables:

| Variable | Description |
| --- | --- |
| `DATABASE_URL` | SQLAlchemy URL; defaults to `sqlite:///./local_context.db`. |
| `SESSION_SECRET` | Used to sign session payloads and OAuth state. |
| `GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET` | OAuth credentials from Google Cloud. |
| `OAUTH_REDIRECT_URI` | Must match the authorized redirect (e.g. `http://localhost:8000/auth/google/callback`). |
| `OPENAI_API_KEY` | Used for embeddings + Chat completions. |
| `COLLECTION_PREFIX`, `CHROMA_DIR`, `EMBED_MODEL`, etc. | Optional tunables for vector storage. |

Load the env file automatically via `python-dotenv` (already called in `app/main.py`).

## Running the App
```bash
uvicorn app.main:app --reload
# Docs: http://localhost:8000/docs
```

## Auth Flow
1. `GET /auth/google` → returns an `authorization_url`.
2. Complete the Google consent screen.
3. `/auth/google/callback` exchanges the code, persists tokens, creates/updates the user record, and redirects to `/auth/debug/authed` showing your bearer token (also stored as an HttpOnly cookie `lc_session`).
4. Use `/auth/me` to confirm the session. All other routes depend on `get_current_user`, so requests without the cookie or `Authorization: Bearer <token>` are rejected.

## Drive Ingestion
| Endpoint | Best for | Notes |
| --- | --- | --- |
| `POST /ingest/drive` | Quick, synchronous runs (≤50 files). | Returns `{found, ingested_chunks, errors}` immediately. |
| `POST /ingest/drive/start` | Production workloads. | Creates an `IngestionJob`, runs in the background, poll `/ingest/jobs/{id}` for progress/logs. |
| `POST /ingest/drive/run` | Legacy scripting helper. | Currently not production-ready; prefer the routes above. |

Under the hood all three call `drive_pipeline.process_drive_file`, which:
- Lists Drive files via Google SDK
- Downloads/exports (Docs/Sheets/Slides → text/CSV)
- Parses bytes (`app/ingest/parser.py`), normalizes text (`text_normalize.py`)
- Computes content hashes + `should_reingest` decision
- Updates `ContentIndex` and deletes/recreates embeddings in Chroma

## Calendar Ingestion
`POST /ingest/calendar` pulls events for the next `months` (default 6), formats them as text blocks, and pushes them through the same chunk/embedding pipeline so `/rag` queries can surface calendar context alongside Drive docs.

## Retrieval APIs
- `POST /rag/search` → `{ query, k, source? }` → ranked chunks with confidence scores.
- `POST /rag/answer` → `{ query, k, max_ctx_chars, allow_partial }` → OpenAI-powered answer with `[n]` citations and aggregate confidence.

Both endpoints embed the query via OpenAI, query the user’s Chroma collection, and rely on `app/rag/chunk.py` for context formatting.

## Data Stores
- **SQL (default SQLite)** – tracks users, sessions, job state, content metadata. Managed via SQLAlchemy models in `app/models.py`. Run `python -m scripts.create_tables` anytime you bootstrap a new environment.
- **Chroma** – persistent vector store under `.chroma/` (configurable). Safe to wipe in dev; rerun ingestion to rebuild.

## Testing
```bash
source .venv/bin/activate
pytest -q
```
The suite runs deterministically with network access disabled by default (set `ALLOW_NETWORK=1` to opt in). It enforces coverage on `app/` via `pytest.ini`, provides @perf markers for optional synthetic benchmarks, and includes golden retrieval datasets plus in-memory fakes for OpenAI, Google APIs, and Chroma. Use `pytest -q -k "not perf"` in CI for the fast path.

## Roadmap Ideas
- Swap FastAPI background tasks for a real worker (Celery/RQ) to handle long-running jobs and retries.
- Add observability (structured logs, metrics, tracing) around ingest + RAG usage.
- Plug in additional sources (Notion, Slack, email) by following the same normalize → dedupe → chunk → embed pattern.
- Replace or extend OpenAI usage with your preferred embedding/LLM provider.

## License
MIT.
