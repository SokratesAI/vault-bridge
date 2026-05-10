"""
E2E fixtures for vault-bridge tests.

Requires real CouchDB and NATS — both run as service containers in CI.
Git push/pull and GitHub API calls are mocked so no real repos are touched.

Environment variables (with CI defaults):
  COUCHDB_URL       (default: http://localhost:5984)
  COUCHDB_USER      (default: admin)
  COUCHDB_PASSWORD  (default: password)
  NATS_URL          (default: nats://localhost:4222)
"""
import asyncio
import os
import sys
import types
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import aiohttp
import nats as nats_lib
import nats.js.api
import pytest
import xxhash
from urllib.parse import quote as _quote

# ── Load vault-bridge/main.py (same stub trick as unit tests) ─────────────────
_STUB_NAMES = ["git", "git.remote", "github"]
_saved_modules = {name: sys.modules.get(name) for name in _STUB_NAMES}


def _stub_module(name: str, **attrs):
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


_stub_module("git", InvalidGitRepositoryError=Exception, Repo=MagicMock())
_stub_module(
    "git.remote",
    PushInfo=MagicMock(ERROR=1, REJECTED=2, REMOTE_REJECTED=4, REMOTE_FAILURE=8),
)

class _FakeGithubException(Exception):
    def __init__(self, status=500, msg=""):
        super().__init__(msg)
        self.status = status

_stub_module("github", Github=MagicMock(), GithubException=_FakeGithubException)

os.environ.setdefault("NATS_URL", "nats://localhost:4222")
os.environ.setdefault("COUCHDB_URL", "http://localhost:5984")
os.environ.setdefault("COUCHDB_USER", "admin")
os.environ.setdefault("COUCHDB_PASSWORD", "password")
os.environ.setdefault("VAULT_REPO", "SokratesAI/vault")
os.environ.setdefault("CLAUDE_REPO", "SokratesAI/.claude")
os.environ.setdefault("GITHUB_TOKEN", "ghp_test")

import importlib.util as _ilu
_MAIN = Path(__file__).parent.parent.parent / "main.py"
_spec = _ilu.spec_from_file_location("vault_bridge_main_e2e", _MAIN)
M = _ilu.module_from_spec(_spec)
sys.modules["vault_bridge_main_e2e"] = M
_spec.loader.exec_module(M)

for _name, _original in _saved_modules.items():
    if _original is None:
        sys.modules.pop(_name, None)
    else:
        sys.modules[_name] = _original

# ── Helpers ───────────────────────────────────────────────────────────────────

COUCHDB_URL  = os.environ["COUCHDB_URL"]
COUCHDB_USER = os.environ["COUCHDB_USER"]
COUCHDB_PASS = os.environ["COUCHDB_PASSWORD"]
NATS_URL     = os.environ["NATS_URL"]

_AUTH = aiohttp.BasicAuth(COUCHDB_USER, COUCHDB_PASS)


async def _couch(session: aiohttp.ClientSession, method: str, path: str, **kwargs):
    url = f"{COUCHDB_URL}{path}"
    async with session.request(method, url, auth=_AUTH, **kwargs) as r:
        return r.status, await r.json()


async def write_vault_note(session: aiohttp.ClientSession, db: str, path: str, content: str):
    """Write a LiveSync v0.25+ chunked note directly to CouchDB."""
    import time
    content_bytes = content.encode("utf-8")
    chunk_id = f"h:{xxhash.xxh64(content_bytes).hexdigest()}"
    now_ms = int(time.time() * 1000)

    # Upsert chunk
    s, ex = await _couch(session, "GET", f"/{db}/{_quote(chunk_id, safe='')}")
    chunk = {"_id": chunk_id, "data": content, "type": "leaf", "children": []}
    if s == 200:
        chunk["_rev"] = ex["_rev"]
    await _couch(session, "PUT", f"/{db}/{_quote(chunk_id, safe='')}", json=chunk)

    # Upsert main doc
    lower_id = path.lower()
    enc_id = _quote(lower_id, safe='')
    s, ex = await _couch(session, "GET", f"/{db}/{enc_id}")
    doc = {
        "_id": lower_id, "path": path, "data": "",
        "children": [chunk_id], "size": len(content_bytes),
        "ctime": now_ms, "mtime": now_ms, "type": "plain", "eden": {},
    }
    if s == 200:
        doc["_rev"] = ex["_rev"]
        doc["ctime"] = ex.get("ctime", now_ms)
    await _couch(session, "PUT", f"/{db}/{enc_id}", json=doc)
    return chunk_id


# ── Session-scoped NATS connection ────────────────────────────────────────────

@pytest.fixture
async def nc():
    try:
        conn = await nats_lib.connect(NATS_URL, connect_timeout=5)
    except Exception as e:
        pytest.skip(f"NATS unreachable at {NATS_URL}: {e}")
    yield conn
    await conn.drain()


@pytest.fixture
async def js(nc):
    return nc.jetstream()


@pytest.fixture
async def kv(js):
    """Per-test KV bucket in the main vault-bridge namespace."""
    bucket = f"test-vb-{uuid4().hex[:8]}"
    store = await js.create_key_value(nats_lib.js.api.KeyValueConfig(bucket=bucket))
    yield store
    try:
        await js.delete_key_value(bucket)
    except Exception:
        pass


@pytest.fixture
async def kv_b_seeded(js):
    """Per-test short-lived KV bucket simulating vault-bridge-b-seeded."""
    bucket = f"test-b-seeded-{uuid4().hex[:8]}"
    store = await js.create_key_value(
        nats_lib.js.api.KeyValueConfig(bucket=bucket, ttl=M.KV_B_SEEDED_TTL_S)
    )
    yield store
    try:
        await js.delete_key_value(bucket)
    except Exception:
        pass


# ── Per-test CouchDB databases ─────────────────────────────────────────────────

@pytest.fixture
async def http():
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(COUCHDB_URL, auth=_AUTH,
                                   timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status not in (200, 401):
                    pytest.skip(f"CouchDB unreachable at {COUCHDB_URL}: {r.status}")
        except Exception as e:
            pytest.skip(f"CouchDB unreachable at {COUCHDB_URL}: {e}")
        yield session


@pytest.fixture
async def obsidian_db(http):
    """Fresh CouchDB database for each test, dropped on teardown."""
    db = f"test-obsidian-{uuid4().hex[:8]}"
    await _couch(http, "PUT", f"/{db}")
    yield db
    await _couch(http, "DELETE", f"/{db}")


@pytest.fixture
async def claude_config_db(http):
    """Fresh CouchDB database for claude-config tests."""
    db = f"test-cc-{uuid4().hex[:8]}"
    await _couch(http, "PUT", f"/{db}")
    yield db
    await _couch(http, "DELETE", f"/{db}")


# ── Task runner helper ─────────────────────────────────────────────────────────

async def run_task(coro, *, timeout: float = 8.0):
    """Run a task coroutine with a shutdown event; cancel after timeout."""
    shutdown = asyncio.Event()

    async def _run():
        await asyncio.wait_for(coro, timeout=timeout)

    task = asyncio.create_task(coro)
    await asyncio.sleep(timeout)
    shutdown.set()
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, asyncio.TimeoutError):
        pass
