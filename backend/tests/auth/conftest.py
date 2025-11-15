from __future__ import annotations

import base64
import importlib
import os

import pytest

import app.core.auth as auth_module


@pytest.fixture()
def reload_auth():
    original_env = os.environ.copy()

    def _reload(**env):
        for key, value in env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        importlib.reload(auth_module)
        return auth_module

    yield _reload

    os.environ.clear()
    os.environ.update(original_env)
    importlib.reload(auth_module)


@pytest.fixture()
def fernet_key() -> str:
    return "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA="
