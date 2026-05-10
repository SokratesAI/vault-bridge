"""
E2E tests for vault-bridge — run against real CouchDB and NATS.
Git push/pull and GitHub API are mocked; all CouchDB/NATS I/O is real.

Success criteria from PR #35:
  1. Task A: CouchDB obsidian change → git commit to vault repo
  2. Task B: .claude commit → files seeded to claude-config DB
  3. Task C loop killer: Task B writes are not echoed back to .claude
  4. Task C phone edit: genuine phone edits ARE committed to .claude
  5. Task D: vault commit → files seeded to obsidian DB (no phone overwrite)
  6. Task D conflict: agent write lands in CouchDB; existing phone edit
     triggers LiveSync conflict (not silently dropped)
  7. Startup repair: corrupted obsydian_livesync_version is self-healed
"""
import asyncio
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call, patch

import aiohttp
import pytest
from urllib.parse import quote as _quote

from .conftest import M, _AUTH, _couch, write_vault_note


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

async def _poll(condition_coro, *, interval=0.25, timeout=8.0):
    """Poll condition_coro until it returns truthy or timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await condition_coro():
            return True
        await asyncio.sleep(interval)
    return False


async def _couch_doc(http, db, doc_id):
    s, doc = await _couch(http, "GET", f"/{db}/{_quote(doc_id.lower(), safe='')}")
    return doc if s == 200 else None


# ══════════════════════════════════════════════════════════════════════════════
# 1. Startup repair — obsydian_livesync_version self-heal
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_repair_livesync_version_e2e(http, obsidian_db):
    """repair_livesync_version must fix a corrupted version doc in a real CouchDB DB."""
    # Plant a corrupted version doc
    bad = {"_id": "obsydian_livesync_version", "type": "newnote", "version": 12}
    await _couch(http, "PUT", f"/{obsidian_db}/obsydian_livesync_version", json=bad)

    with patch.object(M, "OBSIDIAN_DB", obsidian_db):
        await M.repair_livesync_version(http, obsidian_db)

    doc = await _couch_doc(http, obsidian_db, "obsydian_livesync_version")
    assert doc is not None
    assert doc["type"] == "versioninfo"
    assert doc["version"] == 12


@pytest.mark.asyncio
async def test_repair_creates_missing_version_doc(http, obsidian_db):
    """repair_livesync_version must create the version doc when absent."""
    with patch.object(M, "OBSIDIAN_DB", obsidian_db):
        await M.repair_livesync_version(http, obsidian_db)

    doc = await _couch_doc(http, obsidian_db, "obsydian_livesync_version")
    assert doc is not None
    assert doc["type"] == "versioninfo"


# ══════════════════════════════════════════════════════════════════════════════
# 2. seed_file — real CouchDB round-trip
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_seed_file_writes_to_real_couchdb(http, obsidian_db):
    """seed_file must create a properly chunked LiveSync doc in CouchDB."""
    content = "# Hello\nThis is a test note.\n"
    with patch.object(M, "COUCHDB_URL", M.COUCHDB_URL):
        result = await M.seed_file(http, obsidian_db, "daily/test.md", content)

    assert result is True

    doc = await _couch_doc(http, obsidian_db, "daily/test.md")
    assert doc is not None
    assert doc["path"] == "daily/test.md"
    assert len(doc["children"]) == 1
    assert doc["data"] == ""  # content must be in chunk, not main doc

    # Verify chunk is retrievable
    chunk_id = doc["children"][0]
    s, chunk = await _couch(http, "GET", f"/{obsidian_db}/{chunk_id}")
    assert s == 200
    assert chunk["data"] == content


@pytest.mark.asyncio
async def test_seed_file_idempotent_on_real_couchdb(http, obsidian_db):
    """Seeding the same content twice must not create duplicate docs."""
    content = "# Idempotent\n"
    await M.seed_file(http, obsidian_db, "notes.md", content)
    result = await M.seed_file(http, obsidian_db, "notes.md", content)

    assert result is True  # fast-path hit

    # Verify only one doc exists
    s, all_docs = await _couch(http, "GET", f"/{obsidian_db}/_all_docs",
                                 params={"include_docs": "false"})
    # notes.md + its chunk = 2 docs total (plus any version doc)
    note_rows = [r for r in all_docs.get("rows", [])
                 if not r["id"].startswith("_") and not r["id"].startswith("h:")]
    assert len(note_rows) == 1


@pytest.mark.asyncio
async def test_seed_file_only_if_missing_preserves_phone_edit(http, obsidian_db):
    """only_if_missing=True must not overwrite an existing phone edit."""
    phone_content = "# Written on phone\n"
    agent_content = "# Written by agent\n"

    # Simulate phone writing to CouchDB first
    await write_vault_note(http, obsidian_db, "notes.md", phone_content)

    # Agent tries to seed — must skip
    result = await M.seed_file(http, obsidian_db, "notes.md", agent_content,
                                only_if_missing=True)
    assert result is None

    # Phone version must be intact
    doc = await _couch_doc(http, obsidian_db, "notes.md")
    chunk_id = doc["children"][0]
    s, chunk = await _couch(http, "GET", f"/{obsidian_db}/{chunk_id}")
    assert chunk["data"] == phone_content


@pytest.mark.asyncio
async def test_seed_file_without_flag_overwrites_for_conflict(http, obsidian_db):
    """Without only_if_missing, agent write lands in CouchDB — LiveSync detects conflict."""
    phone_content = "# Phone version\n"
    agent_content = "# Agent version\n"

    await write_vault_note(http, obsidian_db, "notes.md", phone_content)

    # Task D incremental sync: no only_if_missing → agent write goes through
    result = await M.seed_file(http, obsidian_db, "notes.md", agent_content)
    assert result is True

    # CouchDB now holds agent's version — LiveSync will detect divergence from
    # local Obsidian storage and surface a conflict dialog.
    doc = await _couch_doc(http, obsidian_db, "notes.md")
    chunk_id = doc["children"][0]
    s, chunk = await _couch(http, "GET", f"/{obsidian_db}/{chunk_id}")
    assert chunk["data"] == agent_content


# ══════════════════════════════════════════════════════════════════════════════
# 3. Task B — .claude repo → claude-config DB
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_task_b_seeds_files_to_couchdb(tmp_path, http, claude_config_db, kv, kv_b_seeded):
    """Task B must seed .claude repo files to the claude-config DB."""
    # Set up a fake .claude repo on disk
    (tmp_path / "prompts").mkdir()
    (tmp_path / "prompts" / "system.md").write_text("# System prompt\n", encoding="utf-8")
    (tmp_path / "skills" / "interview.md").parent.mkdir()
    (tmp_path / "skills" / "interview.md").write_text("# Interview skill\n", encoding="utf-8")

    fake_repo = MagicMock()
    fake_repo.head.commit.hexsha = "abc123def456"
    fake_repo.git.diff.return_value = ""  # no diff — triggers first-run full scan

    shutdown = asyncio.Event()

    async def _run():
        with patch.object(M, "CLAUDE_DIR", tmp_path), \
             patch.object(M, "CLAUDE_CONFIG_DB", claude_config_db), \
             patch.object(M, "_pull_or_clone_claude", return_value=fake_repo), \
             patch.object(M, "CLAUDE_POLL_INTERVAL", 1):
            task = asyncio.create_task(
                M.task_b_claude_to_couchdb(kv, kv_b_seeded, shutdown)
            )
            # Wait until SHA is advanced (means at least one successful sync)
            async def _sha_written():
                try:
                    return await kv.get(M.KV_LAST_CLAUDE_SHA)
                except Exception:
                    return None

            seeded = await _poll(_sha_written, timeout=10)
            shutdown.set()
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
            return seeded

    seeded = await _run()
    assert seeded, "Task B did not advance KV last-claude-sha within timeout"

    # Verify files landed in claude-config DB
    doc = await _couch_doc(http, claude_config_db, "prompts/system.md")
    assert doc is not None, "prompts/system.md must be seeded to claude-config DB"
    assert doc["path"] == "prompts/system.md"

    skill_doc = await _couch_doc(http, claude_config_db, "skills/interview.md")
    assert skill_doc is not None, "skills/interview.md must be seeded to claude-config DB"


@pytest.mark.asyncio
async def test_task_b_stores_chunk_id_in_kv_before_seeding(tmp_path, http, claude_config_db, kv, kv_b_seeded):
    """Task B must store chunk_id in kv_b_seeded before seeding each file."""
    import xxhash
    content = "# Prompt\n"
    (tmp_path / "prompts").mkdir()
    (tmp_path / "prompts" / "test.md").write_text(content, encoding="utf-8")

    fake_repo = MagicMock()
    fake_repo.head.commit.hexsha = "deadbeef1234"
    fake_repo.git.diff.return_value = ""

    expected_chunk_id = f"h:{xxhash.xxh64(content.encode()).hexdigest()}"
    shutdown = asyncio.Event()

    async def _run():
        with patch.object(M, "CLAUDE_DIR", tmp_path), \
             patch.object(M, "CLAUDE_CONFIG_DB", claude_config_db), \
             patch.object(M, "_pull_or_clone_claude", return_value=fake_repo), \
             patch.object(M, "CLAUDE_POLL_INTERVAL", 1):
            task = asyncio.create_task(
                M.task_b_claude_to_couchdb(kv, kv_b_seeded, shutdown)
            )
            # Poll until chunk_id appears in kv_b_seeded
            async def _chunk_written():
                try:
                    return await kv_b_seeded.get("prompts/test.md")
                except Exception:
                    return None

            found = await _poll(_chunk_written, timeout=10)
            shutdown.set()
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
            return found

    found = await _run()
    assert found, "Task B did not store chunk_id in kv_b_seeded within timeout"

    entry = await kv_b_seeded.get("prompts/test.md")
    assert entry is not None
    assert entry.value.decode() == expected_chunk_id


# ══════════════════════════════════════════════════════════════════════════════
# 4. Task C — loop killer and phone-edit detection
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_task_c_loop_killer_ignores_task_b_writes(http, claude_config_db, kv, kv_b_seeded):
    """Task C must NOT call commit_to_claude for writes Task B seeded."""
    content = "# Agent prompt\n"
    path = "prompts/agent.md"

    # Simulate Task B: store chunk_id in kv_b_seeded BEFORE writing to CouchDB
    import xxhash
    chunk_id = f"h:{xxhash.xxh64(content.encode()).hexdigest()}"
    await kv_b_seeded.put(path.lower(), chunk_id.encode())

    # Write the doc to CouchDB (as Task B would)
    await write_vault_note(http, claude_config_db, path, content)

    # Seed seq=0 so Task C reads from the beginning and sees the pre-written doc
    await kv.put(M.KV_LAST_CC_SEQ, b"0")

    commit_calls = []
    shutdown = asyncio.Event()

    async def _run():
        with patch.object(M, "CLAUDE_CONFIG_DB", claude_config_db), \
             patch.object(M, "PR_DEBOUNCE", 1), \
             patch.object(M, "CHANGES_HEARTBEAT_MS", 500), \
             patch.object(M, "commit_to_claude",
                          side_effect=lambda items: commit_calls.extend(items) or []):
            task = asyncio.create_task(
                M.task_c_couchdb_to_claude_pr(kv, kv_b_seeded, shutdown)
            )
            # Give Task C enough time to process the change and any debounce
            await asyncio.sleep(4)
            shutdown.set()
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    await _run()
    assert len(commit_calls) == 0, (
        f"Task C should not commit Task B's own writes, but committed: {commit_calls}"
    )


@pytest.mark.asyncio
async def test_task_c_commits_genuine_phone_edit(http, claude_config_db, kv, kv_b_seeded):
    """Task C must call commit_to_claude for phone edits not in kv_b_seeded."""
    content = "# Phone-edited prompt\n"
    path = "prompts/phone.md"

    # Write directly to CouchDB WITHOUT storing chunk_id in kv_b_seeded
    await write_vault_note(http, claude_config_db, path, content)

    # Seed seq=0 so Task C reads from the beginning and sees the pre-written doc
    await kv.put(M.KV_LAST_CC_SEQ, b"0")

    committed_items = []
    shutdown = asyncio.Event()

    def fake_commit(items):
        committed_items.extend(items)
        return []  # no failures

    async def _run():
        with patch.object(M, "CLAUDE_CONFIG_DB", claude_config_db), \
             patch.object(M, "PR_DEBOUNCE", 1), \
             patch.object(M, "CHANGES_HEARTBEAT_MS", 500), \
             patch.object(M, "commit_to_claude", side_effect=fake_commit):
            task = asyncio.create_task(
                M.task_c_couchdb_to_claude_pr(kv, kv_b_seeded, shutdown)
            )
            committed = await _poll(
                lambda: _committed_items_not_empty(committed_items),
                timeout=10
            )
            shutdown.set()
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
            return committed

    await _run()
    assert any(p == path for p, _ in committed_items), (
        f"Expected {path} in committed items, got: {committed_items}"
    )
    committed_content = next(c for p, c in committed_items if p == path)
    assert committed_content == content


async def _committed_items_not_empty(committed_items):
    return bool(committed_items)


# ══════════════════════════════════════════════════════════════════════════════
# 5. Task D — vault commit → obsidian DB
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_task_d_seeds_new_vault_file(tmp_path, http, obsidian_db, kv):
    """Task D must seed a new vault file to CouchDB obsidian DB."""
    (tmp_path / "profile.md").write_text("# Profile\n", encoding="utf-8")

    fake_repo = MagicMock()
    fake_repo.head.commit.hexsha = "vaultsha001"
    fake_repo.git.diff.return_value = ""  # first run: full scan

    shutdown = asyncio.Event()

    async def _run():
        with patch.object(M, "VAULT_POLL_DIR", tmp_path), \
             patch.object(M, "OBSIDIAN_DB", obsidian_db), \
             patch.object(M, "_pull_or_clone_vault_poll", return_value=fake_repo), \
             patch.object(M, "VAULT_POLL_INTERVAL", 1), \
             patch.object(M, "cleanup_case_duplicates_sync", return_value=[]):
            task = asyncio.create_task(
                M.task_d_vault_to_couchdb(kv, shutdown)
            )
            async def _vault_sha_written():
                try:
                    return await kv.get(M.KV_LAST_VAULT_SHA)
                except Exception:
                    return None

            seeded = await _poll(_vault_sha_written, timeout=10)
            shutdown.set()
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
            return seeded

    seeded = await _run()
    assert seeded, "Task D did not advance KV last-vault-sha"

    doc = await _couch_doc(http, obsidian_db, "profile.md")
    assert doc is not None, "profile.md must be seeded to obsidian DB"


@pytest.mark.asyncio
async def test_task_d_agent_write_lands_in_couchdb_for_conflict(tmp_path, http, obsidian_db, kv):
    """Agent writes must land in CouchDB so LiveSync can surface conflict dialog."""
    phone_content = "# Phone version\n"
    agent_content = "# Agent updated this\n"

    # Phone already has a version in CouchDB
    await write_vault_note(http, obsidian_db, "roadmap.md", phone_content)

    # Agent updates the file in vault repo
    (tmp_path / "roadmap.md").write_text(agent_content, encoding="utf-8")

    fake_repo = MagicMock()
    fake_repo.head.commit.hexsha = "vaultsha002"
    # Incremental diff: roadmap.md was modified by an agent (not [obsidian])
    fake_repo.git.diff.return_value = "M\troadmap.md"
    fake_repo.git.log.return_value = "feat: agent updated roadmap"

    shutdown = asyncio.Event()

    async def _sha_advanced():
        entry = await kv.get(M.KV_LAST_VAULT_SHA)
        if not entry:
            return False
        return entry.value.decode() != "prevsha000"

    async def _run():
        with patch.object(M, "VAULT_POLL_DIR", tmp_path), \
             patch.object(M, "OBSIDIAN_DB", obsidian_db), \
             patch.object(M, "_pull_or_clone_vault_poll", return_value=fake_repo), \
             patch.object(M, "VAULT_POLL_INTERVAL", 1), \
             patch.object(M, "cleanup_case_duplicates_sync", return_value=[]):
            # Pre-set last SHA so Task D uses the incremental diff path
            await kv.put(M.KV_LAST_VAULT_SHA, b"prevsha000")
            task = asyncio.create_task(
                M.task_d_vault_to_couchdb(kv, shutdown)
            )
            advanced = await _poll(_sha_advanced, timeout=10)
            shutdown.set()
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
            return advanced

    advanced = await _run()
    assert advanced, "Task D did not advance SHA"

    # Agent's version must be in CouchDB — LiveSync will detect conflict
    doc = await _couch_doc(http, obsidian_db, "roadmap.md")
    assert doc is not None
    chunk_id = doc["children"][0]
    s, chunk = await _couch(http, "GET", f"/{obsidian_db}/{chunk_id}")
    assert chunk["data"] == agent_content, (
        "Agent version must land in CouchDB; LiveSync handles conflict resolution"
    )


# ══════════════════════════════════════════════════════════════════════════════
# 6. [obsidian] sentinel — loop-back prevention
# ══════════════════════════════════════════════════════════════════════════════

def test_is_obsidian_commit_true():
    """_is_obsidian_commit returns True when last commit subject starts with [obsidian]."""
    repo = MagicMock()
    repo.git.log.return_value = "[obsidian] 1 file(s) [skip ci]"
    assert M._is_obsidian_commit(repo, "notes/meeting.md", "abc", "def") is True


def test_is_obsidian_commit_false_human_message():
    """_is_obsidian_commit returns False for a human commit message."""
    repo = MagicMock()
    repo.git.log.return_value = "feat: add meeting notes"
    assert M._is_obsidian_commit(repo, "notes/meeting.md", "abc", "def") is False


def test_is_obsidian_commit_false_empty():
    """_is_obsidian_commit returns False when git log returns nothing (no commits in range)."""
    repo = MagicMock()
    repo.git.log.return_value = ""
    assert M._is_obsidian_commit(repo, "notes/meeting.md", "abc", "def") is False


def test_is_obsidian_commit_false_on_exception():
    """_is_obsidian_commit returns False on git error so we over-seed rather than miss."""
    repo = MagicMock()
    repo.git.log.side_effect = Exception("git failure")
    assert M._is_obsidian_commit(repo, "notes/meeting.md", "abc", "def") is False


@pytest.mark.asyncio
async def test_task_d_skips_obsidian_committed_file(tmp_path, http, obsidian_db, kv):
    """Task D must not re-seed a file whose last commit was [obsidian] (phone round-trip)."""
    phone_content = "# Written by phone\n"
    agent_content = "# Written by agent — should NOT overwrite phone\n"

    # Phone version already in CouchDB
    await write_vault_note(http, obsidian_db, "notes/phone.md", phone_content)

    (tmp_path / "notes").mkdir()
    (tmp_path / "notes" / "phone.md").write_text(agent_content, encoding="utf-8")

    fake_repo = MagicMock()
    fake_repo.head.commit.hexsha = "newsha002"
    fake_repo.git.diff.return_value = "M\tnotes/phone.md"
    # Last commit on this file was by vault-bridge
    fake_repo.git.log.return_value = "[obsidian] 1 file(s) [skip ci]"

    shutdown = asyncio.Event()

    async def _run():
        with patch.object(M, "VAULT_POLL_DIR", tmp_path), \
             patch.object(M, "OBSIDIAN_DB", obsidian_db), \
             patch.object(M, "_pull_or_clone_vault_poll", return_value=fake_repo), \
             patch.object(M, "VAULT_POLL_INTERVAL", 1), \
             patch.object(M, "cleanup_case_duplicates_sync", return_value=[]):
            await kv.put(M.KV_LAST_VAULT_SHA, b"oldsha001")

            async def _sha_advanced():
                try:
                    e = await kv.get(M.KV_LAST_VAULT_SHA)
                    return e and e.value.decode() == "newsha002"
                except Exception:
                    return None

            task = asyncio.create_task(M.task_d_vault_to_couchdb(kv, shutdown))
            await _poll(_sha_advanced, timeout=10)
            shutdown.set()
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    await _run()

    doc = await _couch_doc(http, obsidian_db, "notes/phone.md")
    chunk_id = doc["children"][0]
    _, chunk = await _couch(http, "GET", f"/{obsidian_db}/{chunk_id}")
    assert chunk["data"] == phone_content, (
        "Task D must not overwrite phone content for [obsidian]-committed files"
    )


@pytest.mark.asyncio
async def test_task_d_seeds_human_committed_file(tmp_path, http, obsidian_db, kv):
    """Task D must seed a file whose last commit was by a human (not [obsidian])."""
    agent_content = "# Written by human on desktop\n"

    (tmp_path / "notes").mkdir()
    (tmp_path / "notes" / "desktop.md").write_text(agent_content, encoding="utf-8")

    fake_repo = MagicMock()
    fake_repo.head.commit.hexsha = "newsha003"
    fake_repo.git.diff.return_value = "M\tnotes/desktop.md"
    # Last commit was by a human
    fake_repo.git.log.return_value = "feat: add desktop notes"

    shutdown = asyncio.Event()

    async def _run():
        with patch.object(M, "VAULT_POLL_DIR", tmp_path), \
             patch.object(M, "OBSIDIAN_DB", obsidian_db), \
             patch.object(M, "_pull_or_clone_vault_poll", return_value=fake_repo), \
             patch.object(M, "VAULT_POLL_INTERVAL", 1), \
             patch.object(M, "cleanup_case_duplicates_sync", return_value=[]):
            await kv.put(M.KV_LAST_VAULT_SHA, b"oldsha002")

            async def _sha_advanced():
                try:
                    e = await kv.get(M.KV_LAST_VAULT_SHA)
                    return e and e.value.decode() == "newsha003"
                except Exception:
                    return None

            task = asyncio.create_task(M.task_d_vault_to_couchdb(kv, shutdown))
            await _poll(_sha_advanced, timeout=10)
            shutdown.set()
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    await _run()

    doc = await _couch_doc(http, obsidian_db, "notes/desktop.md")
    assert doc is not None, "Human-committed file must be seeded to CouchDB"
    chunk_id = doc["children"][0]
    _, chunk = await _couch(http, "GET", f"/{obsidian_db}/{chunk_id}")
    assert chunk["data"] == agent_content


@pytest.mark.asyncio
async def test_task_b_skips_obsidian_committed_file(tmp_path, http, claude_config_db, kv, kv_b_seeded):
    """Task B must not re-seed a .claude file whose last commit was [obsidian] (phone round-trip)."""
    phone_content = "# Prompt edited on phone\n"
    disk_content = "# Disk version — should NOT overwrite phone\n"

    # Phone version already in CouchDB
    await write_vault_note(http, claude_config_db, "prompts/system.md", phone_content)

    (tmp_path / "prompts").mkdir()
    (tmp_path / "prompts" / "system.md").write_text(disk_content, encoding="utf-8")

    fake_repo = MagicMock()
    fake_repo.head.commit.hexsha = "claudesha002"
    fake_repo.git.diff.return_value = "M\tprompts/system.md"
    # Last commit on this file was by vault-bridge (Task C committed phone edit)
    fake_repo.git.log.return_value = "[obsidian] update prompts/system.md"

    shutdown = asyncio.Event()

    async def _run():
        with patch.object(M, "CLAUDE_DIR", tmp_path), \
             patch.object(M, "CLAUDE_CONFIG_DB", claude_config_db), \
             patch.object(M, "_pull_or_clone_claude", return_value=fake_repo), \
             patch.object(M, "CLAUDE_POLL_INTERVAL", 1):
            await kv.put(M.KV_LAST_CLAUDE_SHA, b"oldclaudesha")

            async def _sha_advanced():
                try:
                    e = await kv.get(M.KV_LAST_CLAUDE_SHA)
                    return e and e.value.decode() == "claudesha002"
                except Exception:
                    return None

            task = asyncio.create_task(
                M.task_b_claude_to_couchdb(kv, kv_b_seeded, shutdown)
            )
            await _poll(_sha_advanced, timeout=10)
            shutdown.set()
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    await _run()

    doc = await _couch_doc(http, claude_config_db, "prompts/system.md")
    chunk_id = doc["children"][0]
    _, chunk = await _couch(http, "GET", f"/{claude_config_db}/{chunk_id}")
    assert chunk["data"] == phone_content, (
        "Task B must not overwrite phone content for [obsidian]-committed files"
    )
