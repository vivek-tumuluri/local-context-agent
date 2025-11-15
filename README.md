# Local Context Agent

FastAPI app + RQ worker that authenticates to Google on behalf of a user, ingests Drive + Calendar data, stores normalized metadata in SQL + per-user Chroma collections, and exposes retrieval/search/answering endpoints backed by OpenAI embeddings. The focus is reliable ingest and retrieval: throttled job progress logging, transactional page-level commits, and token-aware embedding batching that keeps large documents within OpenAI limits.

---

## Highlights

**Battle-tested auth/session stack**  
`app/auth.py` handles Google OAuth, stores refresh/access tokens plus Drive session scopes in SQL, issues signed HttpOnly session cookies, and exposes simple helpers (`get_current_user`, `csrf_protect`) used across every route.

**Unified Drive pipeline**  
`app/ingest/drive_pipeline.py` contains the canonical “normalize → dedupe → chunk → embed → persist” flow. Every entrypoint (`/ingest/drive`, `/ingest/drive/start`, legacy scripts, worker jobs) funnels through this module so bug fixes land everywhere.

**EmbeddingBatcher**  
A token-aware batching layer slices chunk uploads across multiple OpenAI requests. It monitors cumulative tokens + chunk counts per batch, flushes automatically inside `enqueue_doc`, and only deletes stale chunks after a document’s entire chunk set is safely upserted to stay under OpenAI request limits.

**Throttled job progress**  
Both inline and RQ workers buffer progress/log updates and commit Drive ingest work per Drive page to keep database writes predictable on large runs.

**RAG-ready APIs**  
`/rag/search` returns ranked chunks with confidence scores, while `/rag/answer` feeds those chunks into OpenAI Chat, streaming answers with inline citations.

**Solid test bed**  
Pytest suite includes fake Google clients, fake OpenAI embeddings, golden Drive docs, and coverage for chunking, re-ingest, token batching, and worker progress throttling. Network calls are blocked by default so tests are deterministic.

---

## Repository Layout

```
backend/
├── app/
│   ├── api/              # FastAPI factory + wiring
│   ├── core/             # auth/db/models/settings/logging/etc.
│   ├── routes/           # auth, ingest, rag, health, jobs routers
│   ├── ingest/           # pipeline, queue, helpers
│   ├── rag/              # chunk/vector utilities
│   └── main.py           # shim exposing app.api.main.app
├── scripts/              # create_tables.py, worker.py
├── tests/                # pytest suite (ingest, rag, auth, metrics...)
├── requirements.txt
└── pytest.ini
.env stays at repo root.
```

---

## Prerequisites

* Python 3.10+
* SQLite (default) or any SQLAlchemy-compatible DB
* Google Cloud OAuth credentials with Drive + Calendar read scopes
* OpenAI API key (embeddings + chat)
* Redis (cloud Upstash URL or local redis-server) for RQ jobs
* Local filesystem access for Chroma (default `backend/.chroma/`)

---

## Setup

```bash
git clone https://github.com/your-org/local-context-agent.git
cd local-context-agent
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
cd backend
pip install -r requirements.txt
python -m scripts.create_tables
```

Create `.env` (never commit) with:

| Var | Description |
| --- | --- |
| `DATABASE_URL` | e.g. `sqlite:///./local_context.db` |
| `SESSION_SECRET` | long random string |
| `GOOGLE_CLIENT_ID`/`GOOGLE_CLIENT_SECRET` | from Google Cloud |
| `OAUTH_REDIRECT_URI` | e.g. `http://localhost:8000/auth/google/callback` |
| `OPENAI_API_KEY` | Embeddings + answer generation |
| `REDIS_URL` | `redis://localhost:6379/0` or Upstash `rediss://` URL |
| `CHROMA_DIR`, `COLLECTION_PREFIX`, `EMBED_MODEL` | optional overrides |
| `INGEST_PROGRESS_FLUSH_INTERVAL` | how often job progress is flushed (default 10) |
| `EMBED_BATCH_SIZE`, `EMBED_TOKEN_LIMIT` | embedding batch knobs (default 48 chunks / 120k tokens) |

The app autoloads `.env` via `python-dotenv`.

---

## Running Everything

Open 3 terminals (all with `.venv` activated). Run everything from `backend/`:

1. **API**
   ```bash
   cd backend
   source ../.venv/bin/activate
   set -a; source ../.env; set +a
   uvicorn app.main:app --reload
   # Swagger: http://localhost:8000/docs
   ```

2. **Worker**
   ```bash
   cd backend
   source ../.venv/bin/activate
   set -a; source ../.env; set +a
   export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES  # macOS fork guard
   python -m scripts.worker
   ```

3. **Client (curl / httpie)**
   ```bash
    cd backend
    source ../.venv/bin/activate
    set -a; source ../.env; set +a
    export BASE_URL="http://127.0.0.1:8000"
   ```

---

## Auth & Session Flow

1. `GET /auth/google` → returns `{authorization_url}`.
2. Visit the URL, consent with Drive + Calendar scopes.
3. `/auth/google/callback` persists tokens, upserts your user, issues an HttpOnly `lc_session` cookie plus bearer token (available at `/auth/debug/authed` during dev).
4. Use `GET /auth/me` to verify the session. Every other route depends on `get_current_user` and `csrf_protect`.

---

## Ingestion Workflows

### Sync Drive ingest (small batches)
```bash
curl -X POST "$BASE_URL/ingest/drive" \
  -H "Content-Type: application/json" \
  -H "X-CSRF-Token: $CSRF" \
  --cookie "$COOKIES" \
  -d '{"limit":20}'
```

### Background Drive ingest
```bash
curl -X POST "$BASE_URL/ingest/drive/start" \
  -H "Content-Type: application/json" \
  -H "X-CSRF-Token: $CSRF" \
  --cookie "$COOKIES" \
  -d '{"query":"","max_files":100,"reembed_all":false}' \
  | tee /tmp/ingest_start.json
export JOB_ID=$(jq -r '.job_id' /tmp/ingest_start.json)
curl "$BASE_URL/ingest/jobs/$JOB_ID" \
  -H "X-CSRF-Token: $CSRF" \
  --cookie "$COOKIES" \
  | jq .
```

Behind the scenes:

1. `routes.py` creates an `IngestionJob` row, enqueues work via RQ (`queue.enqueue_drive_job`) if Redis is configured, or runs inline fallback.
2. Worker executes `_run_ingest`, loads Google Drive credentials, and calls `drive_ingest.ingest_drive`.
3. `drive_pipeline.run_drive_ingest_once` processes Drive pages, buffering chunk uploads per doc via `EmbeddingBatcher`.
4. `_finalize_ready_docs` deletes stale chunk IDs only after new chunks are safely embedded and persisted.
5. Job progress/log updates are throttled and flushed every `INGEST_PROGRESS_FLUSH_INTERVAL` files plus a forced flush at the end.

### Calendar ingest
```bash
curl -X POST "$BASE_URL/ingest/calendar" \
  -H "Content-Type: application/json" \
  -H "X-CSRF-Token: $CSRF" \
  --cookie "$COOKIES" \
  -d '{"months":3}'
```

---

## Retrieval (RAG)

```bash
# search chunks
curl -X POST "$BASE_URL/rag/search" \
  -H "Content-Type: application/json" \
  -H "X-CSRF-Token: $CSRF" \
  --cookie "$COOKIES" \
  -d '{"query":"montreal itinerary","k":6}' \
  | jq .

# answer with citations
curl -X POST "$BASE_URL/rag/answer" \
  -H "Content-Type: application/json" \
  -H "X-CSRF-Token: $CSRF" \
  --cookie "$COOKIES" \
  -d '{"query":"montreal itinerary","k":6,"max_ctx_chars":4000}' \
  | jq .
```

Both endpoints embed the query, query the user’s Chroma collection, and (for `/rag/answer`) send selected chunks to OpenAI Chat with inline `[n]` citations that match the chunk metadata.

---

## Manual Testing (curl/Postman)

Swagger does not automatically send the session and CSRF cookies required by protected routes. To exercise the API manually:

1. **Authenticate once via browser**
   * Start the API + worker as described above.
   * Visit `http://127.0.0.1:8000/auth/google`, complete the Google consent flow, and land on `/auth/me`.
   * Inspect your browser cookies for the domain (e.g., `127.0.0.1`) and note the values of `lc_session` and `lc_csrf`.

2. **Export helpers in your shell**
   ```bash
   export BASE_URL="http://127.0.0.1:8000"
   export SESSION="paste lc_session value here"
   export CSRF_COOKIE="paste lc_csrf value here"
   ```

3. **Fetch a matching CSRF header token**
   ```bash
   CSRF=$(curl -s "$BASE_URL/auth/me" \
     --cookie "lc_session=$SESSION; lc_csrf=$CSRF_COOKIE" \
     | jq -r '.csrf_token')
   ```

4. **Call any stateful endpoint (ingest, rag, etc.)**
   ```bash
   curl -s -X POST "$BASE_URL/rag/search" \
     -H "Content-Type: application/json" \
     -H "X-CSRF-Token: $CSRF" \
     --cookie "lc_session=$SESSION; lc_csrf=$CSRF_COOKIE" \
     -d '{"query":"montreal itinerary","k":6}' \
     | jq .
   ```

Postman users can add the same cookies under the “Cookies” tab and include `X-CSRF-Token` in headers. This mirrors what the production frontend would do automatically (browser keeps the cookies; client adds the CSRF header).

---
## Worker & Queue Tips

* **Clearing the queue** (pending + failed):
```bash
source .venv/bin/activate
set -a; source .env; set +a
cd backend
python - <<'PY'
import os
from redis import from_url
from rq import Queue
from rq.registry import FailedJobRegistry

conn = from_url(os.environ["REDIS_URL"])
q = Queue("ingest", connection=conn)
q.empty()
failed = FailedJobRegistry(queue=q)
for job_id in failed.get_job_ids():
    failed.remove(job_id, delete_job=True)
print("Queue and failed registry cleared.")
PY
```

* **Retry behavior** – RQ automatically retries transient failures (`RETRY_POLICY` in `queue.py`). Token-limit errors count as permanent; batching fixes should prevent them.

* **Telemetry** – `app/logging_utils.log_event` emits structured JSON logs for every stage (drive_list_page, drive_process_file, job start/completion). These logs include `duration_ms`, doc IDs, and user IDs; pipe them into your preferred log aggregator in prod.

---

## Testing

```bash
source .venv/bin/activate
cd backend
pytest tests/ingest/test_drive_ingest.py
pytest tests/ingest/test_drive_pipeline.py -k drive
pytest -q                     # entire suite
pytest -q -m "not perf"       # skip optional perf benchmarks
```

* Network access is blocked unless you set `ALLOW_NETWORK=1`.
* Fake clients simulate Google Drive, OpenAI embeddings, and Chroma so tests run fast.
* New tests include embedding batch split coverage and worker throttling assertions.

---

## Operational Notes

* **Large docs** – With batching enabled, even multi-MB Drive docs should finish; they just span multiple embedding calls. Stale chunks are deleted only after the doc’s entire chunk set succeeds.
* **Unsupported formats** – Binary-only files (images, raw videos) fail gracefully: `process_drive_file` returns `processed=1, embedded=0`, leaving existing chunks untouched.
* **Deleting data** – `POST /auth/disconnect` deletes Chroma entries (`content_index` rows), source state, Drive sessions, ingestion jobs, and user sessions for the current user.

---

## Roadmap

* Prune obvious non-text MIME types during Drive listing to avoid wasted downloads.
* Surface ingest metrics in `/jobs` (batch counts, token totals).
* Add Notion/Slack/email sources by reusing the DocWork + EmbeddingBatcher pattern.
* Swap OpenAI embeddings for an open-source model or Azure-hosted variant.

---

## License

MIT License. Use at your own risk; contributions welcome. Pull requests should include tests for new ingest behaviors (especially around batching, job progress, and RAG responses).
