from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.settings import settings


DATABASE_URL = settings.database_url
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required.")

if settings.is_prod_like and not DATABASE_URL.lower().startswith("postgresql"):
    raise RuntimeError("Postgres with pgvector is required when ENV is not 'local'.")


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
