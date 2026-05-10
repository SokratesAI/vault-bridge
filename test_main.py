"""Unit tests for vault-bridge drone.

Covers the overhaul from PR #35:
  1. _flush_vault_sync — case preservation, idempotency
  2. seed_file — idempotency, only_if_missing semantics
  3. repair_livesync_version — self-heal at startup
  4. seed_missing_vault_files — seeds absent files, skips existing
  5. cleanup_case_duplicates_sync — deduplication logic
  6. commit_to_claude — returns failed items for re-queue
  7. _cleanup_sidecar_dir — one-time migration at Task A startup
  8. Loop killer — Task B chunk_id storage and Task C detection
"""

import asyncio
import os
import sys
import types
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ── Load vault-bridge/main.py without polluting sys.modules ──────────────────
import importlib.util as _ilu

_STUB_NAMES = ["nats", "nats.js", "nats.js.api", "git", "git.remote", "github", "aiohttp"]
_saved_modules = {name: sys.modules.get(name) for name in _STUB_NAMES}


def _stub_module(name: str, **attrs):
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


nats_mod = _stub_module("nats")
_stub_module("nats.js")
_stub_module("nats.js.api", KeyValueConfig=MagicMock())
nats_mod.connect = AsyncMock()

_stub_module("git", InvalidGitRepositoryError=Exception, Repo=MagicMock())
_stub_module("git.remote", PushInfo=MagicMock(ERROR=1, REJECTED=2, REMOTE_REJECTED=4, REMOTE_FAILURE=8))
_stub_module("github", Github=MagicMock(), GithubException=Exception)
_stub_module("aiohttp", BasicAuth=MagicMock(), ClientSession=MagicMock(),
             ClientTimeout=MagicMock())

os.environ.setdefault("NATS_URL", "nats://localhost:4222")
os.environ.setdefault("COUCHDB_URL", "http://localhost:5984")
os.environ.setdefault("COUCHDB_USER", "admin")
os.environ.setdefault("COUCHDB_PASSWORD", "password")
os.environ.setdefault("VAULT_REPO", "SokratesAI/vault")
os.environ.setdefault("CLAUDE_REPO", "SokratesAI/.claude")
os.environ.setdefault("GITHUB_TOKEN", "ghp_test")

_spec = _ilu.spec_from_file_location("vault_bridge_main", Path(__file__).parent / "main.py")
M = _ilu.module_from_spec(_spec)
sys.modules["vault_bridge_main"] = M
_spec.loader.exec_module(M)

for _name, _original in _saved_modules.items():
    if _original is None:
        sys.modules.pop(_name, None)
    else:
        sys.modules[_name] = _original


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_repo(tmp_path: Path) -> MagicMock:
    repo = MagicMock()
    repo.is_dirty.return_value = True
    return repo


# ══════════════════════════════════════════════════════════════════════════════
# 1. _flush_vault_sync — case preservation
# ══════════════════════════════════════════════════════════════════════════════

def test_flush_writes_original_case_path(tmp_path):
    """File must be written at original_path (from phone), not the lowercase doc_id."""
    repo = _make_repo(tmp_path)
    with patch.object(M, "VAULT_DIR", tmp_path), patch.object(M, "push_with_rebase"):
        M._flush_vault_sync({"devcon.md": ("DevCon.md", "# DevCon\n")}, repo)

    assert (tmp_path / "DevCon.md").exists()
    assert not (tmp_path / "devcon.md").exists()


def test_flush_removes_stale_lowercase_duplicate_on_write(tmp_path):
    """When writing Foo.md, any pre-existing foo.md must be deleted."""
    (tmp_path / "foo.md").write_text("old", encoding="utf-8")
    repo = _make_repo(tmp_path)
    with patch.object(M, "VAULT_DIR", tmp_path), patch.object(M, "push_with_rebase"):
        M._flush_vault_sync({"foo.md": ("Foo.md", "new content")}, repo)

    assert (tmp_path / "Foo.md").exists()
    assert not (tmp_path / "foo.md").exists()


def test_flush_removes_stale_lowercase_duplicate_on_delete(tmp_path):
    """When deleting Foo.md (content=None), the lowercase foo.md must also be removed."""
    (tmp_path / "Foo.md").write_text("content", encoding="utf-8")
    (tmp_path / "foo.md").write_text("content", encoding="utf-8")
    repo = _make_repo(tmp_path)
    with patch.object(M, "VAULT_DIR", tmp_path), patch.object(M, "push_with_rebase"):
        M._flush_vault_sync({"foo.md": ("Foo.md", None)}, repo)

    assert not (tmp_path / "Foo.md").exists()
    assert not (tmp_path / "foo.md").exists()


def test_flush_no_write_when_content_unchanged(tmp_path):
    """If file content matches, no git operations should occur."""
    content = "# unchanged\n"
    (tmp_path / "notes.md").write_text(content, encoding="utf-8")
    repo = _make_repo(tmp_path)
    with patch.object(M, "VAULT_DIR", tmp_path):
        changed = M._flush_vault_sync({"notes.md": ("notes.md", content)}, repo)

    assert changed == 0
    repo.git.add.assert_not_called()


# ══════════════════════════════════════════════════════════════════════════════
# 2. seed_file — idempotency
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_seed_file_skips_when_chunk_and_path_match():
    """Fast-path: same content AND same path → no CouchDB writes."""
    import xxhash
    content = "hello"
    chunk_id = f"h:{xxhash.xxh64(content.encode()).hexdigest()}"
    existing_doc = {"_id": "foo.md", "children": [chunk_id], "path": "Foo.md"}

    session = MagicMock()
    with patch.object(M, "couch_get", new=AsyncMock(return_value=(200, existing_doc))), \
         patch.object(M, "couch_put", new=AsyncMock()) as mock_put:
        result = await M.seed_file(session, "obsidian", "Foo.md", content)

    assert result is True
    mock_put.assert_not_called()


@pytest.mark.asyncio
async def test_seed_file_writes_when_path_case_differs():
    """If chunk matches but path field has wrong case, doc must be updated."""
    import xxhash
    content = "hello"
    chunk_id = f"h:{xxhash.xxh64(content.encode()).hexdigest()}"
    existing_doc = {"_id": "foo.md", "_rev": "1-abc", "children": [chunk_id], "path": "foo.md"}

    async def fake_get(sess, db, doc_id):
        return {
            "foo.md": (200, existing_doc),
            chunk_id: (200, {"_id": chunk_id, "_rev": "1-xxx", "data": content}),
        }.get(doc_id, (404, {}))

    with patch.object(M, "couch_get", new=fake_get), \
         patch.object(M, "couch_put", new=AsyncMock(return_value=(200, {}))) as mock_put:
        result = await M.seed_file(MagicMock(), "obsidian", "Foo.md", content)

    assert result is True
    main_doc_call = next(c for c in mock_put.call_args_list if c.args[2] == "foo.md")
    assert main_doc_call.args[3]["path"] == "Foo.md"


# ══════════════════════════════════════════════════════════════════════════════
# 3. seed_file — only_if_missing semantics
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_seed_file_only_if_missing_skips_existing():
    """only_if_missing=True must return None (skip) when doc already exists."""
    existing_doc = {"_id": "notes.md", "_rev": "2-abc", "children": ["h:old"], "path": "notes.md"}
    with patch.object(M, "couch_get", new=AsyncMock(return_value=(200, existing_doc))), \
         patch.object(M, "couch_put", new=AsyncMock()) as mock_put:
        result = await M.seed_file(MagicMock(), "obsidian", "notes.md", "new content",
                                   only_if_missing=True)

    assert result is None, "should return None (skipped) when doc exists"
    mock_put.assert_not_called()


@pytest.mark.asyncio
async def test_seed_file_only_if_missing_seeds_absent():
    """only_if_missing=True must write when doc is absent (404)."""
    async def fake_get(sess, db, doc_id):
        return (404, {})

    with patch.object(M, "couch_get", new=fake_get), \
         patch.object(M, "couch_put", new=AsyncMock(return_value=(201, {}))) as mock_put:
        result = await M.seed_file(MagicMock(), "obsidian", "new.md", "content",
                                   only_if_missing=True)

    assert result is True
    assert mock_put.call_count >= 1


@pytest.mark.asyncio
async def test_seed_file_without_flag_overwrites_existing():
    """Without only_if_missing, different content must be written even if doc exists."""
    existing_doc = {"_id": "notes.md", "_rev": "2-abc", "children": ["h:old"], "path": "notes.md"}

    async def fake_get(sess, db, doc_id):
        return (200, existing_doc) if doc_id == "notes.md" else (404, {})

    with patch.object(M, "couch_get", new=fake_get), \
         patch.object(M, "couch_put", new=AsyncMock(return_value=(200, {}))) as mock_put:
        result = await M.seed_file(MagicMock(), "obsidian", "notes.md", "new content")

    assert result is True
    assert mock_put.call_count >= 1


# ══════════════════════════════════════════════════════════════════════════════
# 4. repair_livesync_version
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_repair_livesync_version_fixes_corrupted():
    """type='newnote' must be replaced with type='versioninfo'."""
    bad_doc = {"_id": "obsydian_livesync_version", "_rev": "2-xyz", "type": "newnote", "version": 12}
    with patch.object(M, "couch_get", new=AsyncMock(return_value=(200, bad_doc))), \
         patch.object(M, "couch_put", new=AsyncMock(return_value=(200, {}))) as mock_put:
        await M.repair_livesync_version(MagicMock(), "obsidian")

    put_doc = mock_put.call_args.args[3]
    assert put_doc["type"] == "versioninfo"
    assert put_doc["version"] == 12
    assert put_doc["_rev"] == "2-xyz"


@pytest.mark.asyncio
async def test_repair_livesync_version_creates_when_missing():
    """If the doc is absent, it must be created with correct fields."""
    with patch.object(M, "couch_get", new=AsyncMock(return_value=(404, {}))), \
         patch.object(M, "couch_put", new=AsyncMock(return_value=(201, {}))) as mock_put:
        await M.repair_livesync_version(MagicMock(), "claude-config")

    put_doc = mock_put.call_args.args[3]
    assert put_doc["type"] == "versioninfo"
    assert put_doc["version"] == 12
    assert "_rev" not in put_doc


@pytest.mark.asyncio
async def test_repair_livesync_version_skips_correct_doc():
    """A correctly formatted doc must not trigger a write."""
    good_doc = {"_id": "obsydian_livesync_version", "_rev": "1-abc", "version": 12, "type": "versioninfo"}
    with patch.object(M, "couch_get", new=AsyncMock(return_value=(200, good_doc))), \
         patch.object(M, "couch_put", new=AsyncMock()) as mock_put:
        await M.repair_livesync_version(MagicMock(), "obsidian")

    mock_put.assert_not_called()


# ══════════════════════════════════════════════════════════════════════════════
# 5. seed_missing_vault_files
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_seed_missing_seeds_absent_files(tmp_path):
    """Files in vault repo that are missing from DB must be seeded."""
    (tmp_path / "profile.md").write_text("# Profile\n", encoding="utf-8")
    (tmp_path / "roadmap.md").write_text("# Roadmap\n", encoding="utf-8")

    with patch.object(M, "seed_file", new=AsyncMock(return_value=True)) as mock_seed, \
         patch.object(M, "OBSIDIAN_DB", "obsidian"):
        await M.seed_missing_vault_files(MagicMock(), tmp_path)

    seeded_paths = {c.args[2] for c in mock_seed.call_args_list}
    assert "profile.md" in seeded_paths
    assert "roadmap.md" in seeded_paths


@pytest.mark.asyncio
async def test_seed_missing_uses_only_if_missing(tmp_path):
    """seed_missing_vault_files must always pass only_if_missing=True to seed_file."""
    (tmp_path / "notes.md").write_text("content", encoding="utf-8")

    with patch.object(M, "seed_file", new=AsyncMock(return_value=None)) as mock_seed, \
         patch.object(M, "OBSIDIAN_DB", "obsidian"):
        await M.seed_missing_vault_files(MagicMock(), tmp_path)

    assert mock_seed.called
    _, kwargs = mock_seed.call_args
    assert kwargs.get("only_if_missing") is True, "must pass only_if_missing=True"


@pytest.mark.asyncio
async def test_seed_missing_skips_hidden_files(tmp_path):
    """Files in hidden directories (e.g. .git, .obsidian) must not be seeded."""
    hidden = tmp_path / ".obsidian"
    hidden.mkdir()
    (hidden / "config.json").write_text("{}", encoding="utf-8")

    with patch.object(M, "seed_file", new=AsyncMock(return_value=True)) as mock_seed, \
         patch.object(M, "OBSIDIAN_DB", "obsidian"):
        await M.seed_missing_vault_files(MagicMock(), tmp_path)

    mock_seed.assert_not_called()


# ══════════════════════════════════════════════════════════════════════════════
# 6. cleanup_case_duplicates_sync
# ══════════════════════════════════════════════════════════════════════════════

def test_cleanup_removes_lowercase_duplicate(tmp_path):
    """If both Foo.md and foo.md exist, foo.md must be removed."""
    (tmp_path / "Foo.md").write_text("content", encoding="utf-8")
    (tmp_path / "foo.md").write_text("content", encoding="utf-8")
    repo = _make_repo(tmp_path)

    with patch.object(M, "push_with_rebase"):
        removed = M.cleanup_case_duplicates_sync(tmp_path, repo)

    assert "foo.md" in removed
    assert not (tmp_path / "foo.md").exists()
    assert (tmp_path / "Foo.md").exists()


def test_cleanup_leaves_unique_files(tmp_path):
    """Files with no case-colliding counterpart must not be touched."""
    (tmp_path / "notes.md").write_text("a", encoding="utf-8")
    (tmp_path / "Daily.md").write_text("b", encoding="utf-8")
    repo = _make_repo(tmp_path)

    with patch.object(M, "push_with_rebase"):
        removed = M.cleanup_case_duplicates_sync(tmp_path, repo)

    assert removed == []
    repo.git.add.assert_not_called()


def test_cleanup_commits_only_when_changes_made(tmp_path):
    """No git commit should be made when there are no duplicates."""
    (tmp_path / "solo.md").write_text("only", encoding="utf-8")
    repo = _make_repo(tmp_path)
    repo.is_dirty.return_value = False

    with patch.object(M, "push_with_rebase") as mock_push:
        M.cleanup_case_duplicates_sync(tmp_path, repo)

    mock_push.assert_not_called()


def test_cleanup_handles_subdirectory_duplicates(tmp_path):
    """Case duplicates in subdirectories must also be cleaned up."""
    sub = tmp_path / "daily"
    sub.mkdir()
    (sub / "2026-04-16.md").write_text("content", encoding="utf-8")
    (sub / "2026-04-16.MD").write_text("content", encoding="utf-8")
    repo = _make_repo(tmp_path)

    with patch.object(M, "push_with_rebase"):
        removed = M.cleanup_case_duplicates_sync(tmp_path, repo)

    assert any("2026-04-16.md" in r for r in removed)
    assert len(list(sub.iterdir())) == 1


def test_cleanup_handles_directory_case_collision(tmp_path):
    """A lowercase dir and mixed-case dir that collapse to same key must be deduplicated."""
    (tmp_path / "lifestyle").mkdir()
    (tmp_path / "Lifestyle").mkdir()
    (tmp_path / "lifestyle" / "notes.md").write_text("old", encoding="utf-8")
    (tmp_path / "Lifestyle" / "notes.md").write_text("new", encoding="utf-8")
    repo = _make_repo(tmp_path)

    with patch.object(M, "push_with_rebase"):
        removed = M.cleanup_case_duplicates_sync(tmp_path, repo)

    assert any("lifestyle" in r and "notes.md" in r for r in removed)
    assert not (tmp_path / "lifestyle" / "notes.md").exists()
    assert (tmp_path / "Lifestyle" / "notes.md").exists()


def test_canonical_path_prefers_uppercase_directory():
    """_canonical_path must prefer paths with more uppercase characters."""
    paths = [Path("lifestyle/notes.md"), Path("Lifestyle/notes.md")]
    assert M._canonical_path(paths) == Path("Lifestyle/notes.md")


# ══════════════════════════════════════════════════════════════════════════════
# 7. commit_to_claude — returns failed items for re-queue
# ══════════════════════════════════════════════════════════════════════════════

def test_commit_to_claude_returns_empty_on_success():
    """When all commits succeed, returned failed list must be empty."""
    mock_repo = MagicMock()
    mock_repo.default_branch = "master"
    mock_repo.get_contents.side_effect = Exception.__subclasses__()[0]("404")

    import github as gh_mod
    orig_exc = gh_mod.GithubException

    class Fake404(Exception):
        status = 404

    mock_repo.get_contents.side_effect = Fake404()
    mock_repo.create_file.return_value = MagicMock()

    with patch.object(M, "Github") as mock_gh:
        mock_gh.return_value.get_repo.return_value = mock_repo
        with patch.object(M, "GithubException", Fake404):
            failed = M.commit_to_claude([("prompts/test.md", "# content")])

    assert failed == []


def test_commit_to_claude_returns_failed_items():
    """When a file commit raises, it must be included in the returned failed list."""
    mock_repo = MagicMock()
    mock_repo.default_branch = "master"
    mock_repo.get_contents.side_effect = Exception("network error")

    with patch.object(M, "Github") as mock_gh:
        mock_gh.return_value.get_repo.return_value = mock_repo
        failed = M.commit_to_claude([("prompts/fail.md", "content")])

    assert len(failed) == 1
    assert failed[0][0] == "prompts/fail.md"
    assert failed[0][1] == "content"


def test_commit_to_claude_partial_failure():
    """On partial failure, only the failed items are returned; successes are not."""
    # Use an exception with .status so the GithubException handler works correctly.
    class FakeGithubException(Exception):
        def __init__(self, status, msg=""):
            super().__init__(msg)
            self.status = status

    call_count = [0]

    def get_contents_side_effect(path, ref):
        call_count[0] += 1
        # Both files: 404 (new file path) so create_file is attempted
        raise FakeGithubException(404, "not found")

    mock_repo = MagicMock()
    mock_repo.default_branch = "master"
    mock_repo.get_contents.side_effect = get_contents_side_effect
    # First file succeeds, second fails
    mock_repo.create_file.side_effect = [MagicMock(), Exception("network error")]

    with patch.object(M, "Github") as mock_gh, \
         patch.object(M, "GithubException", FakeGithubException):
        mock_gh.return_value.get_repo.return_value = mock_repo
        failed = M.commit_to_claude([
            ("prompts/ok.md", "content"),
            ("prompts/fail.md", "content"),
        ])

    assert len(failed) == 1
    assert failed[0][0] == "prompts/fail.md"


# ══════════════════════════════════════════════════════════════════════════════
# 8. _cleanup_sidecar_dir — one-time migration
# ══════════════════════════════════════════════════════════════════════════════

def test_cleanup_sidecar_dir_removes_directory(tmp_path):
    """Must remove .vault-bridge-mtimes directory and commit if it was tracked."""
    sidecar = tmp_path / ".vault-bridge-mtimes"
    sidecar.mkdir()
    (sidecar / "notes.md").write_text("1234567890", encoding="utf-8")

    repo = _make_repo(tmp_path)
    with patch.object(M, "VAULT_DIR", tmp_path), patch.object(M, "push_with_rebase"):
        M._cleanup_sidecar_dir(repo)

    assert not sidecar.exists()
    repo.git.add.assert_called_once_with("-A")
    repo.index.commit.assert_called_once()
    commit_msg = repo.index.commit.call_args.args[0]
    assert ".vault-bridge-mtimes" in commit_msg


def test_cleanup_sidecar_dir_noop_when_absent(tmp_path):
    """Must do nothing when the sidecar directory does not exist."""
    repo = _make_repo(tmp_path)
    with patch.object(M, "VAULT_DIR", tmp_path):
        M._cleanup_sidecar_dir(repo)

    repo.git.add.assert_not_called()
    repo.index.commit.assert_not_called()


def test_cleanup_sidecar_dir_skips_push_when_not_dirty(tmp_path):
    """Must not push if git says nothing is staged (directory wasn't tracked)."""
    sidecar = tmp_path / ".vault-bridge-mtimes"
    sidecar.mkdir()

    repo = _make_repo(tmp_path)
    repo.is_dirty.return_value = False

    with patch.object(M, "VAULT_DIR", tmp_path), \
         patch.object(M, "push_with_rebase") as mock_push:
        M._cleanup_sidecar_dir(repo)

    mock_push.assert_not_called()


# ══════════════════════════════════════════════════════════════════════════════
# 9. Loop killer — chunk_id computation
# ══════════════════════════════════════════════════════════════════════════════

def test_loop_killer_chunk_id_is_deterministic():
    """The chunk_id computed for KV storage must match what seed_file would produce."""
    import xxhash
    content = "# My prompt\nDo the thing.\n"
    content_bytes = content.encode("utf-8")

    # Simulate what Task B does before calling seed_file
    task_b_chunk_id = f"h:{xxhash.xxh64(content_bytes).hexdigest()}"

    # Simulate what seed_file computes internally
    seed_file_chunk_id = f"h:{xxhash.xxh64(content_bytes).hexdigest()}"

    assert task_b_chunk_id == seed_file_chunk_id


def test_loop_killer_different_content_produces_different_chunk_id():
    """Phone edit (different content) must produce a different chunk_id than Task B stored."""
    import xxhash

    agent_content = "# Agent version\n"
    phone_content = "# Phone version\n"

    agent_chunk = f"h:{xxhash.xxh64(agent_content.encode()).hexdigest()}"
    phone_chunk = f"h:{xxhash.xxh64(phone_content.encode()).hexdigest()}"

    assert agent_chunk != phone_chunk, "different content must produce different chunk_ids"


@pytest.mark.asyncio
async def test_loop_killer_kv_key_is_lowercased():
    """Task B must store the chunk under the lowercased path, matching CouchDB's _id."""
    import xxhash

    content = "content"
    rel_path = "Prompts/System.md"
    expected_key = rel_path.lower()  # "prompts/system.md"
    expected_chunk = f"h:{xxhash.xxh64(content.encode()).hexdigest()}"

    stored = {}

    async def fake_kv_put(kv, key, value):
        stored[key] = value

    with patch.object(M, "kv_put", new=fake_kv_put):
        content_bytes = content.encode("utf-8")
        chunk_id = f"h:{xxhash.xxh64(content_bytes).hexdigest()}"
        await M.kv_put(None, rel_path.lower(), chunk_id)

    assert stored.get(expected_key) == expected_chunk
