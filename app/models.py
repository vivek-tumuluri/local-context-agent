from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import (
    Column, String, Integer, DateTime, Boolean, JSON, UniqueConstraint, Index, ForeignKey
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()
utcnow = lambda: datetime.now(timezone.utc)

class User(Base):
    __tablename__ = "users"

    id = Column(String, primary_key=True, default=lambda: str(uuid4()))
    google_sub = Column(String, unique=True, nullable=False, index=True)
    email = Column(String, unique=True, nullable=False, index=True)
    full_name = Column(String, nullable=True)
    picture = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)

    @property
    def user_id(self) -> str:
        return self.id


class UserSession(Base):
    __tablename__ = "user_sessions"

    id = Column(String, primary_key=True, default=lambda: str(uuid4()))
    user_id = Column(String, ForeignKey("users.id"), index=True, nullable=False)
    token_hash = Column(String, unique=True, nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    last_used_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)

class IngestionJob(Base):
    __tablename__ = "ingestion_jobs"
    id = Column(String, primary_key=True, default=lambda: str(uuid4()))
    user_id = Column(String, index=True, nullable=False)
    source = Column(String, default="drive", index=True)
    kind = Column(String, default="drive_ingest", index=True)
    payload = Column(JSON, default=dict)
    status = Column(String, default="queued", index=True)
    total_files = Column(Integer, default=0)
    processed_files = Column(Integer, default=0)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    error_summary = Column(String, nullable=True)
    metrics = Column(JSON, default=dict)

    __table_args__ = (Index("ix_job_user_source_status", "user_id", "source", "status"),)

class SourceState(Base):
    __tablename__ = "source_state"
    user_id = Column(String, primary_key=True)
    source = Column(String, primary_key=True)
    cursor_token = Column(String, nullable=True)
    last_sync = Column(DateTime(timezone=True), nullable=True)
    extra = Column(JSON, default=dict)
    updated_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)

class ContentIndex(Base):
    __tablename__ = "content_index"

    id = Column(String, primary_key=True)
    user_id = Column(String, index=True, nullable=False)
    source = Column(String, index=True, nullable=False)
    external_id = Column(String, nullable=True)
    name = Column(String, nullable=True)
    path = Column(String, nullable=True)
    mime_type = Column(String, nullable=True)
    md5 = Column(String, nullable=True)
    modified_time = Column(DateTime(timezone=True), nullable=True)
    size_bytes = Column(Integer, nullable=True)
    version = Column(String, nullable=True)
    is_trashed = Column(Boolean, default=False, nullable=False)
    content_hash = Column(String, nullable=True)
    last_ingested_at = Column(DateTime(timezone=True), nullable=True)
    extra = Column(JSON, default=dict)

    __table_args__ = (
        UniqueConstraint('user_id', 'source', 'id', name='u_user_source_id'),
        Index("ix_content_user_source_modified", "user_id", "source", "modified_time"),
        Index("ix_content_user_source_trashed", "user_id", "source", "is_trashed"),
    )


class DriveSession(Base):
    __tablename__ = "drive_sessions"
    user_id = Column(String, primary_key=True)
    session_token = Column(String, nullable=True)
    credentials = Column(JSON, default=dict)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)
