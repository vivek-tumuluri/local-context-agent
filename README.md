# Local Context Agent

End-to-end personal RAG stack with:
- FastAPI backend + RQ worker for Google auth, Drive ingest, and retrieval.
- Dual vector backends: Chroma (default) or Postgres pgvector (set `VECTOR_BACKEND=pgvector`).
- OpenAI embeddings + chat for answers.
- Vite/React frontend for login, ingest controls, and Q&A.

---

## What it does
- OAuth with Google, stores tokens in Postgres, issues HttpOnly session cookies + CSRF.
- Ingests Google Drive files: normalize → chunk → embed → persist. Calendar ingest exists but Drive is primary.
- Stores metadata in SQL; stores embeddings in Chroma or pgvector.
- `/rag/search` and `/rag/answer` retrieve relevant chunks and generate answers with citations.
- Background ingest via RQ worker (Redis) or inline fallback.

---

## Repo layout
```
backend/
  app/
    api/            # FastAPI factory
    core/           # auth, db, models, settings, logging
    ingest/         # Drive pipeline, queue, chunking
    rag/            # embeddings, vector backends (chroma + pgvector)
    routes/         # auth, ingest, rag, health, jobs
    main.py         # ASGI entry
  scripts/          # create_tables.py, worker.py
  tests/            # pytest suite (fakes for Drive, OpenAI, Chroma)
frontend/
  src/              # Vite + React UI (login, ingest panel, Q&A)
.env                # loaded by python-dotenv
```

---

## Prerequisites
- Python 3.10+
- Postgres (for pgvector) or SQLite (dev/tests; pgvector tests are skipped)
- Redis (for RQ worker)
- Google OAuth creds with Drive read scope
- OpenAI API key
- Node 18+ (for the frontend)

---

## Environment variables (key ones)
```
DATABASE_URL=postgresql://user:pass@host:5432/dbname   # use Postgres for pgvector
SESSION_SECRET=<long-random>
GOOGLE_CLIENT_ID=...
GOOGLE_CLIENT_SECRET=...
OAUTH_REDIRECT_URI=http://localhost:8000/auth/google/callback
OPENAI_API_KEY=...
REDIS_URL=redis://localhost:6379/0
VECTOR_BACKEND=chroma | pgvector   # default: chroma
CHROMA_DIR=backend/.chroma         # optional override
EMBED_MODEL=text-embedding-3-small # default
```

For pgvector: connect to Postgres and run `CREATE EXTENSION IF NOT EXISTS vector;` once.

---

## Setup (backend)
```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
cd backend
pip install -r requirements.txt
# ensure tables (and pgvector index if on Postgres)
python -m scripts.create_tables
```

---

## Running (3 terminals)
Backend API:
```bash
cd backend
source ../.venv/bin/activate
set -a; source ../.env; set +a
export VECTOR_BACKEND=pgvector   # or chroma
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES  # macOS
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Worker:
```bash
cd backend
source ../.venv/bin/activate
set -a; source ../.env; set +a
export VECTOR_BACKEND=pgvector   # match API
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
python -m scripts.worker
```

Frontend:
```bash
cd frontend
npm install
npm run dev -- --host --port 5173
```
Visit http://localhost:5173/.

---

## Using the app
1) **Login** via Google (button in the frontend) → sets session + CSRF.  
2) **Run Drive ingest** from the dashboard (defaults `max_files: 100` in UI; backend limit is env/route controlled).  
3) **Ask questions** in the chat area → `/rag/answer` retrieves chunks (via selected backend) and calls OpenAI chat.  
4) **Disconnect** to clear user data + vector entries.

---

## Vector backends
- **Chroma** (default): on-disk store under `CHROMA_DIR`.  
- **pgvector**: embeddings stored in `doc_chunks` table with ivfflat index. Enable with `VECTOR_BACKEND=pgvector` and Postgres + `vector` extension.

---

## Testing
```bash
cd backend
pytest -q          # pgvector test skipped on SQLite
```
Network calls are blocked in tests unless `ALLOW_NETWORK=1`. Fakes cover Google Drive, OpenAI embeddings, and Chroma.

---

## Notes
- Job progress is throttled; chunking respects max chars per chunk and OpenAI limits.
- Unsupported/binary files are skipped without failing ingest.
- The frontend only posts to the existing APIs; backend behavior and signatures are unchanged aside from the vector backend toggle.

---

## License
MIT
