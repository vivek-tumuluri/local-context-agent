# Local Context Agent

FastAPI service that signs into Google on behalf of a user, ingests Drive files and Calendar events, and exposes a retrieval-augmented generation (RAG) API backed by a local Chroma vector store plus OpenAI models. The project is intentionally minimal so you can swap in your own auth layer, job runner, or LLM provider while keeping the ingestion and retrieval plumbing in place.

## Feature Highlights
- Google OAuth flow (`/auth`) stores refreshable sessions per user (`DriveSession` table + in-memory cache).
- Drive and Calendar ingestion pipelines convert documents to normalized text, chunk, embed with OpenAI, and upsert into a Chroma collection namespaced per user.
- Background job helper (`app/ingest/job_helper.py`) tracks ingestion progress/status in SQL (SQLite by default).
- RAG endpoints (`/rag/search`, `/rag/answer`) return ranked chunks with confidence scores or full answers with inline citations.
- Scriptable schema management (`scripts/create_tables.py`), pluggable vector store, and configurable chunking/tokenization utilities.

## High-Level Architecture
```
FastAPI (app/main.py)
├── Auth: app/auth.py + app/google_clients.py
├── Ingestion APIs:
│   ├── app/ingest/drive_ingest.py  (token mgmt + background callable)
│   ├── app/ingest/calendar_ingest.py
│   ├── app/ingest/routes.py        (job orchestration)
│   └── app/routes/ingest_drive.py  (drive_pipeline convenience route)
├── RAG APIs: app/rag/routes.py
├── Vector store: app/rag/vector.py (Chroma + OpenAI embeddings)
└── Persistence: app/db.py, app/models.py, scripts/create_tables.py
```

### Data flow (Drive example)
1. User completes Google OAuth → `/auth/google/callback` returns a signed session token.
2. `POST /ingest/drive/start` creates an `IngestionJob` row and enqueues a FastAPI `BackgroundTask`.
3. Background worker loads the session (`DriveSession`), lists Drive files, downloads/export as needed, runs parsers (`app/ingest/parser.py`), normalizes text, chunks (`app/rag/chunk.py`), embeds + upserts to Chroma, and updates job progress/logs through `job_helper`.
4. Chroma stores per-user collections; `/rag/*` queries reuse the same user namespace so Drive + Calendar context can be referenced later by `OpenAI` when generating answers.

## Repository Layout
```
app/
├── auth.py                Google OAuth endpoints + session signing
├── db.py                  SQLAlchemy engine + session factory
├── main.py                FastAPI app wiring
├── models.py              ORM models (IngestionJob, ContentIndex, etc.)
├── google_clients.py      OAuth Flow builder + scopes
├── ingest/                Ingestion pipelines, helpers, job routes
│   ├── drive_ingest.py    Drive token cache + ingest callable + API
│   ├── drive_pipeline.py  Metadata-aware dedupe + vector upsert helper
│   ├── routes.py          `/ingest` job endpoints + background runner
│   ├── calendar_ingest.py Calendar ingestion
│   ├── parser.py          PDF/Docx/CSV/text extraction helpers
│   ├── text_normalize.py  Unicode cleanup + hashing utilities
│   ├── should_ingest.py   Delta detection to skip unchanged files
│   ├── chunking.py        Simple char-splitter (used by drive_pipeline)
│   └── job_helper.py      CRUD + log helpers for `IngestionJob`
├── rag/
│   ├── chunk.py           Markdown-aware chunker with token budgets
│   ├── vector.py          Chroma persistence + OpenAI embedding logic
│   └── routes.py          `/rag/search` + `/rag/answer`
├── routes/ingest_drive.py Legacy/experimental Drive ingestion entrypoint
└── routes/jobs.py         Simple job create/status endpoints
scripts/create_tables.py   Creates SQL tables defined in app/models.py
requirements.txt           Runtime dependencies
```

## Prerequisites
- Python 3.10+ (3.11 recommended).
- SQLite (bundled with Python) for metadata; change `DATABASE_URL` for Postgres/MySQL.
- Local filesystem access for the Chroma store (default `./.chroma`).
- Google Cloud project with OAuth credentials + Drive/Calendar scopes enabled.
- OpenAI API key (used both for embeddings and answer generation).

## Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Configure your environment (see next section) then create tables:
python scripts/create_tables.py
```

Run the API:
```bash
uvicorn app.main:app --reload
```
The FastAPI docs live at `http://localhost:8000/docs`.

## Configuration
Create a `.env` (never commit secrets). Key variables:

| Variable | Purpose |
| --- | --- |
| `DATABASE_URL` | SQLAlchemy URL (defaults to `sqlite:///./local_context.db`). |
| `SESSION_SECRET` | Secret for signing OAuth session payloads. |
| `GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET` | Credentials from Google Cloud Console. |
| `OAUTH_REDIRECT_URI` | Must match the Authorized redirect URI in Google console (e.g. `http://localhost:8000/auth/google/callback`). |
| `OPENAI_API_KEY` | Used by `app/rag/vector.py` (embeddings) and `app/rag/routes.py` (answers). |
| `EMBED_MODEL` | OpenAI embedding model (default `text-embedding-3-small`). |
| `ANSWER_MODEL` | Chat model for `/rag/answer` (default `gpt-4o-mini`). |
| `CHROMA_DIR` | On-disk location of the Chroma DB (default `.chroma`). |
| `COLLECTION_PREFIX` | Prefix for user-scoped Chroma collections. |
| `INGEST_DRIVE_DEFAULT_MAX`, `INGEST_DRIVE_PAGE_SIZE`, `INGEST_DRIVE_LIST_RETRIES`, `INGEST_DRIVE_BACKOFF_BASE` | Tune Drive ingestion batch sizes + retries. |
| `DRIVE_SESSION_TOKEN` / `DRIVE_SESSION_<USER>` | Optional fallback tokens if you want to seed Drive sessions without hitting `/ingest/drive` first. |
| `RAG_MAX_CTX_CHARS`, `RAG_DEFAULT_K` | Retrieval context sizing defaults. |
| `CHROMA_RESET_ON_CORRUPTION` | Set to `1` only in dev if you want auto-resets when metadata mismatches. |

Load variables via `python-dotenv` (already invoked in `app/main.py`).

## OAuth & Session Flow
1. `GET /auth/google` → returns `authorization_url` + `state`.
2. Visit the URL, approve scopes (Drive read-only + Calendar read-only).
3. Google redirects to `/auth/google/callback`; the handler exchanges the code, serializes the refresh token bundle, and redirects to `/auth/debug/authed?session=<signed-token>` so you can copy the token locally.
4. Use this `session` token in subsequent ingestion calls (`X-Session` header, query string, or request body depending on the route). Tokens are stored/reused via the `DriveSession` table.

## Ingestion APIs

### Quick Drive ingest (`app/ingest/drive_ingest.py`)
- `POST /ingest/drive?session=...&limit=20&name_contains=foo` – attaches the Drive session to the user, lists files, downloads/export (supports Docs/Sheets/Slides conversion), parses (`app/ingest/parser.py`), chunks with the markdown-aware chunker, embeds, and upserts into the user’s Chroma collection. Returns `{found, ingested, errors}` synchronously.
- Background entry point `ingest_drive(...)` (same module) is what the job route calls. It accepts an `on_progress(done, total, message)` callback so job rows can be updated incrementally.

### Job-based Drive ingest (`app/ingest/routes.py`)
- `POST /ingest/drive/start` – body: `{"query": "...", "max_files": 50, "reembed_all": false}`. Requires a stored session (`ensure_drive_session`) and creates an `IngestionJob` row before launching `_run_drive_job` as a FastAPI `BackgroundTask`.
- `GET /ingest/jobs/{job_id}` – returns a single row, enforcing ownership via the placeholder `fake_user`.
- `GET /ingest/jobs` – lists recent jobs for the user.
This route relies on `app/ingest/job_helper.py` and the SQLAlchemy `IngestionJob` model. Replace `fake_user` with your auth dependency to multi-tenant it properly.

### Drive pipeline helper (`app/routes/ingest_drive.py`)
`POST /ingest/drive/run` gives a more prescriptive loop that:
1. Lists pages via the Google SDK client.
2. Downloads/export files chunk-by-chunk.
3. Parses bytes (you can swap `parse_bytes`) and sends normalized text into `app/ingest/drive_pipeline.run_drive_ingest_once`.
4. Dedupe logic (`should_reingest`) skips files whose MD5/version/hash match stored metadata in `ContentIndex`.
It expects the Drive session via header/query (`X-Session`/`?session=`) and writes richer metadata rows per document (`ContentIndex`, `SourceState`). Use this if you want precise control over pagination tokens.

### Calendar ingest (`app/ingest/calendar_ingest.py`)
- `POST /ingest/calendar` with `session` and `months` (default 6). Pulls upcoming events, formats them into text blocks, chunks, and upserts. These chunks show up alongside Drive docs in `/rag` results.

## Retrieval APIs
- `POST /rag/search` – body `{ "query": "...", "k": 6, "source": "drive" }`. Returns top-k chunks from the user’s Chroma collection plus per-hit confidence scores.
- `POST /rag/answer` – body `{ "query": "...", "k": 6, "max_ctx_chars": 7000, "allow_partial": true }`. Retrieves chunks, builds a context window with numbered blocks, and calls OpenAI Chat to produce an answer with `[n]` citations and a combined confidence estimate. Requires `OPENAI_API_KEY`.

Both endpoints rely on:
- `app/rag/vector.py` for Chroma access, embeddings with exponential backoff, collection namespacing (`<COLLECTION_PREFIX>-<user_id>`), and optional dev-only corruption resets.
- `app/rag/chunk.py` for markdown-aware chunking used in Drive/Calendar ingest.

## Persistence
- **SQL Database** (`app/db.py`): defaults to SQLite `./local_context.db`. Tables live in `app/models.py` (`IngestionJob`, `SourceState`, `ContentIndex`, `DriveSession`). Run `python scripts/create_tables.py` whenever you add migrations or bootstrap a new environment.
- **Vector Store**: Chroma persistent client rooted at `CHROMA_DIR`. You can safely delete the directory to force a rebuild (all vectors will be lost; rerun ingestion).

## Development Notes & Tips
- Replace all `fake_user()` dependencies with your auth of choice (e.g., verify JWT, map to `user_id`). Until then, everything is hard-coded to `demo_user`.
- When running ingestion outside FastAPI (e.g., in a real worker), import and call `app/ingest/drive_ingest.ingest_drive(...)` directly; pass your own `on_progress` to integrate with a queue system.
- `app/ingest/parser.py` currently supports PDF, DOCX, CSV/TSV, and plain text. Extend it for other MIME types (Slides, images, etc.) as needed.
- To reset Chroma in development: delete `.chroma/` or set `CHROMA_RESET_ON_CORRUPTION=1` temporarily (never in production).
- The repo ships without tests; consider adding unit tests around `chunk.py`, `should_ingest.py`, and the vector helpers before extending.
- Keep secrets like `.env` and `local_context.db` out of source control; `.gitignore` already excludes `.env`, but add `.chroma` and database files if you plan to commit back.

## Next Steps
- Swap the placeholder auth dependencies for your identity provider so each API call can derive `user.user_id` securely.
- Move the background ingestion worker into a true task runner (Celery, RQ, etc.) if you expect long-running jobs or multiple workers.
- Add observability (structured logs, metrics) around job execution, embedding usage, and OpenAI spend.
- Expand ingestion sources (Notion, Slack, email) by following the same pattern: extract text → chunk → embed → upsert with source metadata.

