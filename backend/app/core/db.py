import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.settings import ENVIRONMENT


DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required.")

if ENVIRONMENT != "local" and not DATABASE_URL.lower().startswith("postgresql"):
    raise RuntimeError("Postgres with pgvector is required when ENVIRONMENT is not 'local'.")


connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}


engine = create_engine(
    DATABASE_URL,
    connect_args=connect_args,
    pool_pre_ping=True,
    future=True,
)


SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    future=True,
)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
