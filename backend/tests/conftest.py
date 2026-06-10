from __future__ import annotations

import base64
import os
import socket
from pathlib import Path
from types import SimpleNamespace
from typing import Generator, Iterable

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from tests.db_safety import configure_test_database

# Configure deterministic env before app modules import.
os.environ.setdefault("ENVIRONMENT", "local")
os.environ.setdefault("APP_ENV", "test")
os.environ.setdefault("SESSION_COOKIE_SECURE", "1")
os.environ.setdefault("SESSION_COOKIE_SAMESITE", "strict")
os.environ.setdefault("SESSION_SECRET", "test-session-secret-value-that-is-long-enough-12345")

configure_test_database(os.environ)
if "DRIVE_CREDENTIALS_KEY" not in os.environ:
    os.environ["DRIVE_CREDENTIALS_KEY"] = "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA="
os.environ.setdefault("OPENAI_API_KEY", "test-openai-key")

from app.core import auth as auth_module
from app.core import db as app_db
from app.main import app as fastapi_app
from app.core.models import Base, User
from app.routes import ingest_routes
from app.ingest import drive_ingest
from app.routes import jobs as jobs_routes
from app.rag import vector_store as vector_module
from app.rag import vector_pg
from app.routes import rag_routes
from tests.fakes import FakeEmbeddingsClient, FakeChatCompletions

_ORIGINAL_GET_CURRENT_USER = auth_module.get_current_user


@pytest.fixture(autouse=True)
def block_network(monkeypatch):
    """Prevent accidental external network access."""
    if os.getenv("ALLOW_NETWORK") == "1":
        return

    real_socket = socket.socket

    class GuardedSocket(real_socket):  # type: ignore[misc,valid-type]
        def connect(self, *args, **kwargs):
            raise RuntimeError("Network access disabled during tests. Set ALLOW_NETWORK=1 to override.")

        def connect_ex(self, *args, **kwargs):
            raise RuntimeError("Network access disabled during tests. Set ALLOW_NETWORK=1 to override.")

    def guard(*args, **kwargs):
        raise RuntimeError("Network access disabled during tests. Set ALLOW_NETWORK=1 to override.")

    monkeypatch.setattr(socket, "socket", GuardedSocket)
    monkeypatch.setattr(socket, "create_connection", guard)


@pytest.fixture()
def session_factory(monkeypatch, tmp_path) -> Generator[sessionmaker, None, None]:
    """Provide a fresh SQLite DB per test and wire FastAPI deps to it."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}", future=True)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

    def _get_db() -> Iterable[Session]:
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    modules = [
        app_db,
        auth_module,
        ingest_routes,
        drive_ingest,
        jobs_routes,
    ]
    for mod in modules:
        if hasattr(mod, "SessionLocal"):
            monkeypatch.setattr(mod, "SessionLocal", SessionLocal, raising=False)
        if hasattr(mod, "get_db"):
            monkeypatch.setattr(mod, "get_db", _get_db, raising=False)
    if hasattr(ingest_routes, "_db_dependency"):
        monkeypatch.setattr(ingest_routes, "_db_dependency", _get_db, raising=False)

    yield SessionLocal

    Base.metadata.drop_all(engine)
    engine.dispose()


@pytest.fixture()
def db_session(session_factory) -> Generator[Session, None, None]:
    db = session_factory()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture()
def user_factory(db_session):
    created = []

    def _make_user(email: str = "user@example.com") -> User:
        user = User(google_sub=email, email=email, full_name="QA User")
        db_session.add(user)
        db_session.commit()
        db_session.refresh(user)
        created.append(user)
        return user

    return _make_user


@pytest.fixture()
def test_user(user_factory) -> User:
    return user_factory()


@pytest_asyncio.fixture()
async def api_client(session_factory, test_user):
    override = lambda: test_user
    fastapi_app.dependency_overrides[_ORIGINAL_GET_CURRENT_USER] = override
    fastapi_app.dependency_overrides[auth_module.get_current_user] = override
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
    fastapi_app.dependency_overrides.pop(auth_module.get_current_user, None)
    fastapi_app.dependency_overrides.pop(_ORIGINAL_GET_CURRENT_USER, None)


@pytest.fixture()
def fake_vector_env(monkeypatch, session_factory) -> FakeEmbeddingsClient:
    embeddings = FakeEmbeddingsClient()
    dim = vector_pg.EMBED_DIM

    def _fake_embed(texts):
        while True:
            try:
                resp = embeddings.create(texts, model=vector_pg.EMBED_MODEL)
                break
            except Exception as exc:
                s = str(exc).lower()
                if "rate limit" in s or "429" in s:
                    continue
                raise
        out = []
        for item in resp.data:
            vec = list(item.embedding)[:dim]
            if len(vec) < dim:
                vec.extend([0.0] * (dim - len(vec)))
            out.append(vec)
        return out

    for backend_impl in (vector_pg, vector_module.BACKEND):
        monkeypatch.setattr(backend_impl, "_client", SimpleNamespace(embeddings=embeddings), raising=False)
        monkeypatch.setattr(backend_impl, "_embed_with_retry", _fake_embed, raising=False)
        monkeypatch.setattr(backend_impl, "SessionLocal", session_factory, raising=False)
        monkeypatch.setattr(backend_impl.time, "sleep", lambda *_: None, raising=False)
    monkeypatch.setattr(vector_module, "_embed_with_retry", _fake_embed, raising=False)

    # ensure clean table
    session = session_factory()
    session.execute(text("DELETE FROM doc_chunks"))
    session.commit()
    session.close()

    yield embeddings

    session = session_factory()
    session.execute(text("DELETE FROM doc_chunks"))
    session.commit()
    session.close()


@pytest.fixture()
def fake_chat_client(monkeypatch) -> FakeChatCompletions:
    chat = FakeChatCompletions()
    fake_oai = SimpleNamespace(chat=SimpleNamespace(completions=chat))
    monkeypatch.setattr(rag_routes, "oai", fake_oai, raising=False)
    return chat


@pytest.fixture()
def golden_drive_docs():
    """Load the canonical golden documents used for RAG assertions."""
    base = Path(__file__).resolve().parent / "data" / "golden_docs"
    docs = []
    for path in sorted(base.glob("*")):
        if path.is_file():
            docs.append((path.stem.replace("_", "-"), path.read_text(encoding="utf-8").strip()))
    return docs
