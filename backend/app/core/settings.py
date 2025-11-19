from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()


ENVIRONMENT = os.getenv("ENVIRONMENT", "local").lower()
READ_ONLY_MODE = os.getenv("READ_ONLY_MODE", "0").lower() in {"1", "true", "yes"}

SESSION_SECRET = os.getenv("SESSION_SECRET", "")
if len(SESSION_SECRET) < 32 or SESSION_SECRET == "dev-secret":
    raise RuntimeError("SESSION_SECRET must be a strong random string (>= 32 chars and not 'dev-secret').")

DRIVE_CREDENTIALS_KEY = os.getenv("DRIVE_CREDENTIALS_KEY")
if ENVIRONMENT != "local" and not DRIVE_CREDENTIALS_KEY:
    raise RuntimeError("DRIVE_CREDENTIALS_KEY is required when ENVIRONMENT is not 'local'.")

ALLOW_INLINE_INGEST = os.getenv("ALLOW_INLINE_INGEST", "false").lower() == "true"
