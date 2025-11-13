from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()


READ_ONLY_MODE = os.getenv("READ_ONLY_MODE", "0").lower() in {"1", "true", "yes"}
