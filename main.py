"""
vault-bridge — bidirectional sync between two GitHub repos and two CouchDB databases.
Runs as a persistent Deployment. Four concurrent async tasks:

Task A — obsidian DB → vault repo
  Streams CouchDB `obsidian` _changes feed. Batch-commits changed vault files
  to SokratesAI/vault every FLUSH_INTERVAL seconds.

Task B — .claude repo → claude-config DB
  Polls SokratesAI/.claude every CLAUDE_POLL_INTERVAL seconds.
  On new commit: seeds changed files to CouchDB `claude-config` database using
  LiveSync v0.25+ chunked format. Before each seed, stores the chunk ID in a
  short-lived NATS KV bucket so Task C can identify Task B's own writes.

Task C — claude-config DB → .claude commit
  Streams CouchDB `claude-config` _changes feed.
  Loop killer: checks NATS KV to see if the incoming chunk was seeded by Task B.
  If not, it's a genuine phone edit: debounce PR_DEBOUNCE seconds, then commit
  directly to SokratesAI/.claude master. Failed per-file commits are re-queued.

Task D — vault repo → obsidian DB
  Polls SokratesAI/vault every VAULT_POLL_INTERVAL seconds.
  Seeds new vault files to CouchDB `obsidian` so agent-written notes appear on
  the phone. Phone is always source of truth: existing CouchDB docs are never
  overwritten (only_if_missing semantics).

State in NATS KV bucket "vault-bridge":
  last-couchdb-seq       — obsidian DB sequence (Task A)
  last-claude-config-seq — claude-config DB sequence (Task C)
  last-claude-sha        — last .claude commit synced to claude-config DB (Task B)
  last-vault-sha         — last vault commit synced to obsidian DB (Task D)

State in NATS KV bucket "vault-bridge-b-seeded" (TTL=5min):
  {lower_path} → chunk_id  — chunk IDs recently seeded by Task B (loop killer)
"""

import asyncio
import json
import logging
import os
import signal
import stat
import time
from pathlib import Path
from urllib.parse import quote

import aiohttp
import nats
import nats.js.api
import xxhash
from git import InvalidGitRepositoryError, Repo
from git.remote import PushInfo
from github import Github, GithubException

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Environment ───────────────────────────────────────────────────────────────

NATS_URL             = os.environ["NATS_URL"]
COUCHDB_URL          = os.environ["COUCHDB_URL"].rstrip("/")
COUCHDB_USER         = os.environ["COUCHDB_USER"]
COUCHDB_PASSWORD     = os.environ["COUCHDB_PASSWORD"]

OBSIDIAN_DB          = os.environ.get("COUCHDB_DB", "obsidian")
CLAUDE_CONFIG_DB     = os.environ.get("CLAUDE_CONFIG_DB", "claude-config")

VAULT_REPO           = os.environ["VAULT_REPO"]
CLAUDE_REPO          = os.environ["CLAUDE_REPO"]
CLAUDE_BRANCH        = "master"
GITHUB_TOKEN         = os.environ["GITHUB_TOKEN"]

FLUSH_INTERVAL       = int(os.environ.get("FLUSH_INTERVAL", "30"))
CLAUDE_POLL_INTERVAL   = int(os.environ.get("CLAUDE_POLL_INTERVAL", "60"))
VAULT_POLL_INTERVAL    = int(os.environ.get("VAULT_POLL_INTERVAL", "60"))
PR_DEBOUNCE            = int(os.environ.get("PR_DEBOUNCE", "120"))
CHANGES_HEARTBEAT_MS   = int(os.environ.get("CHANGES_HEARTBEAT_MS", "10000"))

VAULT_DIR            = Path("/tmp/vault-repo")
VAULT_POLL_DIR       = Path("/tmp/vault-repo-poll")   # Task D read-only clone
CLAUDE_DIR           = Path("/tmp/claude-repo")

KV_BUCKET            = "vault-bridge"
KV_LAST_SEQ          = "last-couchdb-seq"
KV_LAST_CC_SEQ       = "last-claude-config-seq"
KV_LAST_CLAUDE_SHA   = "last-claude-sha"
KV_LAST_VAULT_SHA    = "last-vault-sha"

# Short-lived KV bucket used by Task B to record seeded chunk IDs so Task C
# can distinguish Task B's own writes from genuine phone edits (loop killer).
# TTL = 5 minutes; entries expire long after CouchDB propagates the change.
KV_B_SEEDED_BUCKET   = "vault-bridge-b-seeded"
KV_B_SEEDED_TTL_S    = 5 * 60  # 5 min in seconds (KeyValueConfig.ttl unit)

# Sentinel prefix on every commit made by vault-bridge (Task A and Task C).
# Task B and Task D check the last commit touching each file; if it starts with
# this sentinel, they skip re-seeding so phone edits don't loop back as conflicts.
OBSIDIAN_COMMIT_SENTINEL = "[obsidian]"

PUSH_ERROR_FLAGS = (
    PushInfo.ERROR | PushInfo.REJECTED | PushInfo.REMOTE_REJECTED | PushInfo.REMOTE_FAILURE
)

# LiveSync internal docs — skip these in the obsidian feed.
# Also skip "claude-config/" which now lives in its own database.
OBSIDIAN_SKIP_PREFIXES     = ("_", "h:", "f:", "i:", "v:", "claude-config/")
OBSIDIAN_SKIP_EXACT        = {"obsydian_livesync_version", "settings.local.json"}
# LiveSync internal docs — skip in the claude-config feed.
CLAUDE_CONFIG_SKIP_PREFIXES = ("_", "h:", "f:", "i:", "v:")
CLAUDE_CONFIG_SKIP_EXACT = {"obsydian_livesync_version", "settings.local.json"}


# ── Shared utilities ──────────────────────────────────────────────────────────

def configure_git_credentials() -> None:
    netrc = Path.home() / ".netrc"
    netrc.write_text(
        f"machine github.com login x-access-token password {GITHUB_TOKEN}\n",
        encoding="utf-8",
    )
    netrc.chmod(stat.S_IRUSR | stat.S_IWUSR)


_AUTH = aiohttp.BasicAuth(COUCHDB_USER, COUCHDB_PASSWORD)


def auth() -> aiohttp.BasicAuth:
    return _AUTH


def safe_path(base: Path, rel: str) -> Path | None:
    candidate = (base / rel).resolve()
    try:
        candidate.relative_to(base.resolve())
        return candidate
    except ValueError:
        log.warning("path traversal attempt blocked: %r", rel)
        return None



async def kv_get(kv, key: str) -> str | None:
    try:
        entry = await kv.get(key)
        return entry.value.decode() if entry and entry.value else None
    except Exception:
        return None


async def kv_put(kv, key: str, value: str) -> None:
    try:
        await kv.put(key, value.encode())
    except Exception as e:
        log.warning("KV put failed for %s: %s", key, e)


def _is_obsidian_commit(repo: Repo, rel_path: str, since_sha: str, until_sha: str) -> bool:
    """True if the most recent commit in since..until touching rel_path was by vault-bridge.

    Used by Task B and Task D to skip re-seeding files that vault-bridge itself
    committed from a phone edit, preventing the phone→GitHub→CouchDB conflict loop.
    On error (e.g. git failure) returns False so we over-seed rather than miss a change.
    """
    try:
        subject = repo.git.log(
            "--max-count=1", "--format=%s",
            f"{since_sha}..{until_sha}", "--", rel_path,
        ).strip()
        return subject.startswith(OBSIDIAN_COMMIT_SENTINEL)
    except Exception:
        return False


async def connect_nats():
    while True:
        try:
            return await nats.connect(NATS_URL, max_reconnect_attempts=-1, reconnect_time_wait=2)
        except Exception as e:
            log.warning("NATS connect failed, retrying in 5s: %s", e)
            await asyncio.sleep(5)


# ── Git helpers ───────────────────────────────────────────────────────────────

def _github_url(repo_name: str) -> str:
    """Build an authenticated GitHub HTTPS URL using GITHUB_TOKEN / GH_TOKEN."""
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN", "")
    if token:
        return f"https://x-access-token:{token}@github.com/{repo_name}.git"
    return f"https://github.com/{repo_name}.git"


def clone_repo(repo_dir: Path, repo_name: str) -> Repo:
    """Clone repo_name into repo_dir using the authenticated URL."""
    import shutil
    if repo_dir.exists():
        shutil.rmtree(repo_dir)
    log.info("cloning %s", repo_name)
    repo = Repo.clone_from(_github_url(repo_name), repo_dir)
    repo.config_writer().set_value("user", "name", "vault-bridge").release()
    repo.config_writer().set_value("user", "email", "vault-bridge@sokratesai").release()
    return repo


def ensure_vault_repo(repo_dir: Path) -> Repo:
    """Open or clone the vault repo, always on main."""
    import shutil
    if repo_dir.exists():
        try:
            repo = Repo(repo_dir)
            repo.remotes.origin.set_url(_github_url(VAULT_REPO))
            repo.git.fetch("origin")
            repo.git.checkout("main")
            repo.git.reset("--hard", "origin/main")
            return repo
        except InvalidGitRepositoryError:
            shutil.rmtree(repo_dir)
        except Exception as e:
            log.warning("ensure_vault_repo: fetch failed (%s) — using last known state", e)
            try:
                return Repo(repo_dir)
            except Exception:
                shutil.rmtree(repo_dir)
    return clone_repo(repo_dir, VAULT_REPO)


def push_or_raise(repo: Repo) -> None:
    result = repo.remotes.origin.push()
    failed = [i for i in result if i.flags & PUSH_ERROR_FLAGS]
    if failed:
        raise RuntimeError(f"git push rejected: {'; '.join(i.summary.strip() for i in failed)}")


def push_with_rebase(repo: Repo, max_attempts: int = 3) -> None:
    for attempt in range(1, max_attempts + 1):
        try:
            push_or_raise(repo)
            return
        except RuntimeError as e:
            if attempt == max_attempts:
                raise
            log.warning("push failed (attempt %d/%d): %s — rebasing", attempt, max_attempts, e)
            repo.remotes.origin.fetch()
            try:
                repo.git.rebase("origin/HEAD")
            except Exception:
                conflicted = repo.git.diff("--name-only", "--diff-filter=U").splitlines()
                if not conflicted:
                    raise
                log.warning("conflicts on %d file(s), keeping CouchDB version", len(conflicted))
                for f in conflicted:
                    repo.git.checkout("--theirs", f)
                repo.git.add("-A")
                repo.git.rebase("--continue", env={"GIT_EDITOR": "true"})


# ── CouchDB helpers ───────────────────────────────────────────────────────────

async def couch_get(session: aiohttp.ClientSession, db: str, doc_id: str) -> tuple[int, dict]:
    url = f"{COUCHDB_URL}/{db}/{quote(doc_id, safe='')}"
    async with session.get(url, auth=auth()) as r:
        return r.status, await r.json()


async def couch_put(session: aiohttp.ClientSession, db: str, doc_id: str, body: dict) -> tuple[int, dict]:
    url = f"{COUCHDB_URL}/{db}/{quote(doc_id, safe='')}"
    async with session.put(url, json=body, auth=auth()) as r:
        return r.status, await r.json()


async def couch_ensure_db(session: aiohttp.ClientSession, db: str) -> None:
    """Create the CouchDB database if it doesn't exist."""
    url = f"{COUCHDB_URL}/{db}"
    async with session.head(url, auth=auth()) as r:
        if r.status == 200:
            return
    async with session.put(url, auth=auth()) as r:
        if r.status not in (201, 412):
            raise RuntimeError(f"Failed to create CouchDB db '{db}': {r.status}")
    log.info("created CouchDB database '%s'", db)


async def stream_changes(session: aiohttp.ClientSession, db: str, since: str):
    """Yield (seq, row) from a CouchDB continuous _changes feed.
    Yields (None, {}) on each 10s heartbeat."""
    url = f"{COUCHDB_URL}/{db}/_changes"
    params = {"feed": "continuous", "since": since, "include_docs": "true", "heartbeat": str(CHANGES_HEARTBEAT_MS)}
    async with session.get(url, params=params, auth=auth()) as resp:
        resp.raise_for_status()
        buf = b""
        async for chunk in resp.content.iter_any():
            buf += chunk
            while b"\n" in buf:
                raw, buf = buf.split(b"\n", 1)
                line = raw.strip()
                if not line:
                    yield None, {}
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    log.warning("unparseable changes line: %r", line[:120])
                    continue
                if "seq" not in row:
                    continue
                yield row["seq"], row


async def assemble_content(session: aiohttp.ClientSession, db: str, doc: dict) -> str:
    """Assemble file content from a LiveSync v0.25+ chunked doc."""
    children = doc.get("children") or []
    if children:
        parts = []
        for chunk_id in children:
            s, cd = await couch_get(session, db, chunk_id)
            if s == 200:
                parts.append(cd.get("data", ""))
            else:
                log.warning("chunk %s missing (%d) for %s", chunk_id, s, doc.get("_id"))
        return "".join(parts)
    return doc.get("data", "")


async def seed_file(
    session: aiohttp.ClientSession,
    db: str,
    vault_path: str,
    content: str,
    *,
    only_if_missing: bool = False,
) -> bool | None:
    """Write one file to CouchDB using LiveSync v0.25+ chunked format.

    Returns:
      True  — file was written or was already up-to-date
      None  — skipped because only_if_missing=True and doc already exists
              (phone is source of truth — caller should count as skipped)
      False — write failed

    only_if_missing: when True, skip if the doc already exists in CouchDB.
      Used by Task D so that phone edits are never overwritten by vault commits.
      Task B never sets this (agent .claude files always update the phone).
    """
    now_ms = int(time.time() * 1000)
    content_bytes = content.encode("utf-8")
    chunk_id = f"h:{xxhash.xxh64(content_bytes).hexdigest()}"
    lower_id = vault_path.lower()

    s, ex = await couch_get(session, db, lower_id)

    # Phone is source of truth: if doc exists and caller says don't overwrite, skip.
    if only_if_missing and s == 200:
        return None

    # Fast-path: already up-to-date (same content and correct path case).
    if s == 200 and ex.get("children") == [chunk_id] and ex.get("path") == vault_path:
        return True

    # Upsert chunk doc
    sc, ec = await couch_get(session, db, chunk_id)
    chunk = {"_id": chunk_id, "data": content, "type": "leaf", "children": []}
    if sc == 200:
        chunk["_rev"] = ec["_rev"]
    await couch_put(session, db, chunk_id, chunk)

    # Upsert main doc
    doc = {
        "_id": lower_id,
        "path": vault_path,
        "data": "",
        "children": [chunk_id],
        "size": len(content_bytes),
        "ctime": now_ms,
        "mtime": now_ms,
        "type": "plain",
        "eden": {},
    }
    if s == 200:
        doc["_rev"] = ex["_rev"]
        doc["ctime"] = ex.get("ctime", now_ms)

    s, _ = await couch_put(session, db, lower_id, doc)
    return s in (200, 201)


async def delete_couch_doc(
    session: aiohttp.ClientSession, db: str, doc_id: str, caller: str = "task"
) -> bool:
    """Delete a CouchDB document by ID (fetches rev first)."""
    lower_id = doc_id.lower()
    s, ex = await couch_get(session, db, lower_id)
    if s == 404:
        return True  # Already gone
    if s != 200:
        log.warning("[%s] could not GET %s for deletion: %d", caller, lower_id, s)
        return False
    url = f"{COUCHDB_URL}/{db}/{quote(lower_id, safe='')}?rev={ex['_rev']}"
    async with session.delete(url, auth=auth()) as r:
        if r.status not in (200, 202):
            log.warning("[%s] DELETE %s returned %d", caller, lower_id, r.status)
            return False
    return True


# ── Startup repair ───────────────────────────────────────────────────────────

async def repair_livesync_version(session: aiohttp.ClientSession, db: str) -> None:
    """Ensure obsydian_livesync_version has the correct format in the given DB.

    The correct doc is: {"_id": "obsydian_livesync_version", "version": 12, "type": "versioninfo"}
    Any other format (e.g. type="newnote") causes LiveSync to report "remote database is corrupted".
    This runs at startup so we self-heal after any bad write.
    """
    doc_id = "obsydian_livesync_version"
    s, ex = await couch_get(session, db, doc_id)
    needs_repair = False
    if s == 404:
        needs_repair = True
        doc: dict = {"_id": doc_id, "version": 12, "type": "versioninfo"}
    elif s == 200:
        if ex.get("version") != 12 or ex.get("type") != "versioninfo":
            needs_repair = True
            doc = {"_id": doc_id, "_rev": ex["_rev"], "version": 12, "type": "versioninfo"}
        else:
            return  # Already correct
    else:
        log.warning("[repair] unexpected status %d reading %s from %s", s, doc_id, db)
        return

    if needs_repair:
        s2, _ = await couch_put(session, db, doc_id, doc)
        if s2 in (200, 201):
            log.info("[repair] fixed %s in %s (was type=%r)", doc_id, db, ex.get("type") if s == 200 else "missing")
        else:
            log.warning("[repair] failed to fix %s in %s: %d", doc_id, db, s2)


async def seed_missing_vault_files(session: aiohttp.ClientSession, repo_dir: Path) -> None:
    """Seed vault repo files that are absent from the obsidian DB.

    Task D's incremental diff only picks up files changed since last-vault-sha.
    After a DB wipe, existing vault files (profile.md, roadmap.md, etc.) will
    never be re-seeded unless we explicitly check for gaps.
    Called once at Task D startup. Uses only_if_missing so phone edits are never
    overwritten — only truly absent files are seeded.
    """
    seeded = 0
    for abs_path in sorted(repo_dir.rglob("*")):
        if not abs_path.is_file():
            continue
        rel = abs_path.relative_to(repo_dir)
        if any(p.startswith(".") for p in rel.parts):
            continue
        rel_path = str(rel)
        if any(rel_path.startswith(p) for p in OBSIDIAN_SKIP_PREFIXES) or rel_path in OBSIDIAN_SKIP_EXACT:
            continue

        try:
            content = abs_path.read_text(encoding="utf-8")
            result = await seed_file(session, OBSIDIAN_DB, rel_path, content, only_if_missing=True)
            if result is True:
                seeded += 1
                log.info("[task-d] seeded missing file: %s", rel_path)
            elif result is False:
                log.warning("[task-d] failed to seed missing: %s", rel_path)
            # result is None → already exists on phone, skip (phone is source of truth)
        except Exception as e:
            log.warning("[task-d] error seeding missing %s: %s", rel_path, e)

    if seeded:
        log.info("[task-d] completeness check: seeded %d missing file(s)", seeded)
    else:
        log.debug("[task-d] completeness check: obsidian DB is up-to-date")


def _canonical_path(paths: list[Path]) -> Path:
    """Pick the canonical path from a collision group.

    Strategy: prefer the path whose directory components have the most uppercase
    characters — this selects phone-created mixed-case directories (e.g. Lifestyle/)
    over old vault-bridge lowercase directories (e.g. lifestyle/).
    Ties broken alphabetically so the result is deterministic.
    """
    def uppercase_score(p: Path) -> int:
        return sum(1 for c in str(p) if c.isupper())

    return max(paths, key=lambda p: (uppercase_score(p), str(p)))


def cleanup_case_duplicates_sync(repo_dir: Path, repo: Repo) -> list[str]:
    """Remove files that are case-duplicates of another file in the same repo.

    Groups all files by their fully-lowercased relative path. When multiple files
    share the same lowercase key (e.g. lifestyle/Uke-16.md and Lifestyle/Uke-16.md),
    one canonical path is kept and the rest are removed.

    Canonical path selection: the path with the most uppercase characters in its
    directory components wins (prefers phone-created mixed-case directories over
    old vault-bridge lowercase directories). Ties are broken alphabetically.

    Returns list of removed relative paths. Commits+pushes if anything was removed.
    Runs in an executor — no asyncio primitives.
    """
    from collections import defaultdict

    lower_to_paths: dict[str, list[Path]] = defaultdict(list)
    for abs_path in repo_dir.rglob("*"):
        if not abs_path.is_file():
            continue
        rel = abs_path.relative_to(repo_dir)
        if any(p.startswith(".") for p in rel.parts):
            continue
        lower_to_paths[str(rel).lower()].append(rel)

    removed = []
    for lower_key, paths in lower_to_paths.items():
        if len(paths) <= 1:
            continue
        keep = _canonical_path(paths)
        for path in paths:
            if path == keep:
                continue
            (repo_dir / path).unlink()
            removed.append(str(path))
            log.info("[task-d] removed duplicate: %s (kept %s)", path, keep)

    if removed:
        repo.git.add("-A")
        if repo.is_dirty(index=True):
            repo.index.commit(f"[obsidian] remove {len(removed)} case duplicate(s) [skip ci]")
            push_with_rebase(repo)
            log.info("[task-d] pushed duplicate cleanup (%d file(s) removed)", len(removed))

    return removed


# ── GitHub PR helper (Task C) ─────────────────────────────────────────────────

def commit_to_claude(
    changed_files: list[tuple[str, str | None]],
) -> list[tuple[str, str | None]]:
    """Commit phone-edited files directly to SokratesAI/.claude master branch.

    Returns list of (path, content) tuples that failed — callers should re-queue
    these so they are retried on the next debounce cycle.
    """
    gh = Github(GITHUB_TOKEN)
    claude_gh = gh.get_repo(CLAUDE_REPO)
    default_branch = claude_gh.default_branch

    committed: list[str] = []
    failed: list[tuple[str, str | None]] = []
    for path, content in changed_files:
        try:
            existing = None
            try:
                existing = claude_gh.get_contents(path, ref=default_branch)
            except GithubException as e:
                if e.status != 404:
                    raise
            if content is None:
                if existing:
                    claude_gh.delete_file(
                        path, f"[obsidian] remove {path}",
                        existing.sha, branch=default_branch,
                    )
                    committed.append(f"{path} (deleted)")
            elif existing:
                claude_gh.update_file(
                    path, f"[obsidian] update {path}",
                    content, existing.sha, branch=default_branch,
                )
                committed.append(path)
            else:
                claude_gh.create_file(
                    path, f"[obsidian] add {path}",
                    content, branch=default_branch,
                )
                committed.append(path)
        except Exception as e:
            log.warning("[task-c] failed to commit %s: %s", path, e)
            failed.append((path, content))

    if committed:
        log.info("[task-c] committed directly to %s: %s", default_branch, committed)
    return failed


# ── Task A: obsidian DB → vault repo ─────────────────────────────────────────

def _cleanup_sidecar_dir(repo: Repo) -> None:
    """Remove the legacy .vault-bridge-mtimes sidecar directory if it exists.

    Called once at Task A startup so the cleanup happens even when the vault
    is idle and _flush_vault_sync never runs. Commits+pushes if the directory
    was tracked in git.
    Runs in an executor — must not call any asyncio primitives.
    """
    import shutil
    sidecar_root = VAULT_DIR / ".vault-bridge-mtimes"
    if not sidecar_root.exists():
        return
    shutil.rmtree(sidecar_root)
    log.info("[task-a] removed legacy .vault-bridge-mtimes directory")
    repo.git.add("-A")
    if repo.is_dirty(index=True):
        repo.index.commit("[obsidian] remove legacy .vault-bridge-mtimes [skip ci]")
        push_with_rebase(repo)


def _flush_vault_sync(pending: dict, repo: Repo) -> int:
    """Write pending files to disk and git-commit+push. Returns number of changed files.

    pending maps doc_id (lowercase CouchDB _id) → (original_path, content|None).
    Files are written using original_path to preserve case from the phone.
    Runs in an executor — must not call any asyncio primitives.
    """
    changed = 0

    for doc_id, (original_path, content) in pending.items():
        file_path = safe_path(VAULT_DIR, original_path)
        if not file_path:
            continue
        if content is None:
            if file_path.exists():
                file_path.unlink()
                changed += 1
            # Also clean up any stale lowercase version (from old vault-bridge behaviour)
            lower_path = safe_path(VAULT_DIR, original_path.lower())
            if lower_path and lower_path != file_path and lower_path.exists():
                lower_path.unlink()
                changed += 1
        else:
            current = file_path.read_text(encoding="utf-8") if file_path.exists() else None
            if current == content:
                continue
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(content, encoding="utf-8")
            # Remove stale lowercase duplicate if a differently-cased version now exists
            lower_path = safe_path(VAULT_DIR, original_path.lower())
            if lower_path and lower_path != file_path and lower_path.exists():
                lower_path.unlink()
            changed += 1

    if changed:
        repo.git.add("-A")
        if repo.is_dirty(index=True):
            repo.index.commit(f"[obsidian] {changed} file(s) [skip ci]")
            push_with_rebase(repo)
            log.info("[task-a] pushed %d file(s)", changed)

    return changed


async def flush_vault(pending: dict, repo: Repo, kv, seq: str) -> None:
    if not pending:
        return
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _flush_vault_sync, dict(pending), repo)
    await kv_put(kv, KV_LAST_SEQ, seq)


async def task_a_obsidian_to_vault(kv, shutdown: asyncio.Event) -> None:
    """Stream obsidian _changes → commit to vault repo."""
    loop = asyncio.get_running_loop()
    repo = await loop.run_in_executor(None, ensure_vault_repo, VAULT_DIR)
    log.info("[task-a] vault repo ready")
    await loop.run_in_executor(None, _cleanup_sidecar_dir, repo)
    # Maps doc_id (lowercase CouchDB _id) → (original_path, content|None)
    # Keyed by lowercase id for dedup; original_path preserves phone case.
    pending: dict[str, tuple[str, str | None]] = {}
    last_flush = time.monotonic()
    backoff = 1

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None)) as session:
        while not shutdown.is_set():
            # Re-read from KV on every (re)connection so a manual seq reset takes effect
            # without requiring a pod restart.
            current_seq = await kv_get(kv, KV_LAST_SEQ) or "0"
            try:
                async for seq, row in stream_changes(session, OBSIDIAN_DB, current_seq):
                    if shutdown.is_set():
                        break
                    if seq is None:
                        if pending and time.monotonic() - last_flush >= FLUSH_INTERVAL:
                            await flush_vault(pending, repo, kv, current_seq)
                            pending.clear()
                            last_flush = time.monotonic()
                        continue

                    doc = row.get("doc", {})
                    doc_id: str = doc.get("_id", "")
                    if not doc_id or any(doc_id.startswith(p) for p in OBSIDIAN_SKIP_PREFIXES) or doc_id in OBSIDIAN_SKIP_EXACT:
                        current_seq = str(seq)
                        continue

                    # Use the original-case path from the doc's "path" field;
                    # fall back to doc_id if absent (older docs may lack it).
                    original_path = doc.get("path") or doc_id
                    target = safe_path(VAULT_DIR, original_path)
                    if not target or target.is_dir():
                        current_seq = str(seq)
                        continue

                    if row.get("deleted") or doc.get("_deleted"):
                        pending[doc_id] = (original_path, None)
                    else:
                        content = await assemble_content(session, OBSIDIAN_DB, doc)
                        if isinstance(content, str):
                            pending[doc_id] = (original_path, content)

                    current_seq = str(seq)
                    if len(pending) >= 50:
                        await flush_vault(pending, repo, kv, current_seq)
                        pending.clear()
                        last_flush = time.monotonic()

                if pending:
                    await flush_vault(pending, repo, kv, current_seq)
                    pending.clear()
                backoff = 1

            except Exception as e:
                log.warning("[task-a] stream error: %s(%s) — reconnecting in %ds", type(e).__name__, e, backoff)
                if pending:
                    try:
                        await flush_vault(pending, repo, kv, current_seq)
                        pending.clear()
                    except Exception as fe:
                        log.error("[task-a] pre-reconnect flush failed: %s", fe)
                    last_flush = time.monotonic()
                try:
                    await asyncio.wait_for(shutdown.wait(), timeout=float(backoff))
                    break
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 2, 60)
                if not VAULT_DIR.exists():
                    try:
                        repo = await loop.run_in_executor(None, ensure_vault_repo, VAULT_DIR)
                    except Exception as ce:
                        log.error("[task-a] re-clone failed: %s", ce)

    if pending:
        try:
            await flush_vault(pending, repo, kv, current_seq)
        except Exception as e:
            log.error("[task-a] final flush failed: %s", e)
    log.info("[task-a] stopped")


# ── Task B: .claude repo → claude-config DB ──────────────────────────────────

def _pull_or_clone_claude() -> Repo:
    """Pull .claude repo (master) if present, re-clone only if corrupt. Runs in executor."""
    import shutil
    if CLAUDE_DIR.exists():
        try:
            repo = Repo(CLAUDE_DIR)
            repo.remotes.origin.set_url(_github_url(CLAUDE_REPO))
            repo.git.fetch("origin")
            repo.git.checkout(CLAUDE_BRANCH)
            repo.git.reset("--hard", f"origin/{CLAUDE_BRANCH}")
            return repo
        except InvalidGitRepositoryError:
            shutil.rmtree(CLAUDE_DIR)
        except Exception as e:
            log.warning("[task-b] fetch failed (%s) — using last known state", e)
            try:
                return Repo(CLAUDE_DIR)
            except Exception:
                shutil.rmtree(CLAUDE_DIR)
    return clone_repo(CLAUDE_DIR, CLAUDE_REPO)


def _parse_diff_name_status(diff_output: str) -> list[tuple[str, str]]:
    """Parse `git diff --name-status` into (status, path) pairs.
    Renames (R) are split into a delete of the old path and an add of the new."""
    entries = []
    for line in diff_output.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        status = parts[0].strip()
        if status.startswith("R") and len(parts) == 3:
            entries.append(("D", parts[1].strip()))
            entries.append(("A", parts[2].strip()))
        elif len(parts) == 2:
            entries.append((status[0], parts[1].strip()))
    return entries


async def task_b_claude_to_couchdb(kv, kv_b_seeded, shutdown: asyncio.Event) -> None:
    """Poll .claude repo every CLAUDE_POLL_INTERVAL seconds; sync changes to claude-config DB.

    On first run: seeds all files (full sync).
    On subsequent runs: uses git diff to seed only added/modified files and delete
    CouchDB docs for removed files. seed_file's idempotency check makes re-runs cheap.

    Loop killer: before seeding each file, store its chunk_id in kv_b_seeded so
    Task C can identify Task B's own writes and ignore them.
    """
    loop = asyncio.get_running_loop()

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        while not shutdown.is_set():
            try:
                await couch_ensure_db(session, CLAUDE_CONFIG_DB)
                # Git operations are synchronous — run off the event loop thread
                repo = await loop.run_in_executor(None, _pull_or_clone_claude)
                current_sha = repo.head.commit.hexsha
                last_sha = await kv_get(kv, KV_LAST_CLAUDE_SHA)

                if last_sha == current_sha:
                    log.debug("[task-b] no new .claude commits")
                else:
                    log.info("[task-b] syncing .claude %s → %s",
                             (last_sha or "none")[:8], current_sha[:8])

                    # Determine which files to seed/delete
                    if last_sha:
                        # Incremental: only changed files since last sync
                        diff_out = repo.git.diff("--name-status", last_sha, current_sha)
                        changes = _parse_diff_name_status(diff_out)
                    else:
                        # First run: treat all non-hidden files as added
                        changes = []
                        for abs_path in sorted(CLAUDE_DIR.rglob("*")):
                            if not abs_path.is_file():
                                continue
                            rel = abs_path.relative_to(CLAUDE_DIR)
                            if any(p.startswith(".") for p in rel.parts):
                                continue
                            changes.append(("A", str(rel)))

                    ok = fail = skip = deleted = 0
                    for status, rel_path in changes:
                        # Skip hidden paths that may appear in diffs (.gitignore changes etc.)
                        if any(p.startswith(".") for p in Path(rel_path).parts):
                            continue
                        # Skip LiveSync internal docs (e.g. obsydian_livesync_version)
                        if any(rel_path.startswith(p) for p in CLAUDE_CONFIG_SKIP_PREFIXES) or rel_path in CLAUDE_CONFIG_SKIP_EXACT:
                            continue

                        if status == "D":
                            if await delete_couch_doc(session, CLAUDE_CONFIG_DB, rel_path, caller="task-b"):
                                deleted += 1
                            else:
                                fail += 1
                        else:
                            # Skip files whose last commit was by vault-bridge — they
                            # originated as phone edits and re-seeding would cause conflicts.
                            if last_sha and _is_obsidian_commit(repo, rel_path, last_sha, current_sha):
                                skip += 1
                                log.debug("[task-b] skipping phone-originated file: %s", rel_path)
                                continue

                            abs_path = CLAUDE_DIR / rel_path
                            if not abs_path.is_file():
                                skip += 1
                                continue
                            try:
                                content = abs_path.read_text(encoding="utf-8")
                            except (UnicodeDecodeError, IOError):
                                skip += 1
                                continue

                            # Store chunk_id in KV BEFORE seeding so Task C can
                            # identify this as Task B's own write (loop killer).
                            content_bytes = content.encode("utf-8")
                            chunk_id = f"h:{xxhash.xxh64(content_bytes).hexdigest()}"
                            await kv_put(kv_b_seeded, rel_path.lower(), chunk_id)

                            if await seed_file(session, CLAUDE_CONFIG_DB, rel_path, content):
                                ok += 1
                            else:
                                log.warning("[task-b] failed to seed %s", rel_path)
                                fail += 1

                    log.info("[task-b] seeded %d, deleted %d (%d skipped, %d failed)",
                             ok, deleted, skip, fail)
                    if fail == 0:
                        await kv_put(kv, KV_LAST_CLAUDE_SHA, current_sha)
                        log.info("[task-b] advanced last-claude-sha to %s", current_sha[:8])
                    else:
                        log.warning("[task-b] not advancing SHA due to %d failures", fail)

            except Exception as e:
                log.warning("[task-b] error: %s", e)

            try:
                await asyncio.wait_for(shutdown.wait(), timeout=float(CLAUDE_POLL_INTERVAL))
                break
            except asyncio.TimeoutError:
                pass

    log.info("[task-b] stopped")


# ── Task C: claude-config DB → .claude PR ────────────────────────────────────

async def task_c_couchdb_to_claude_pr(kv, kv_b_seeded, shutdown: asyncio.Event) -> None:
    """Stream claude-config _changes; commit genuine phone edits directly to .claude master.

    Loop killer: when a change arrives, check doc["children"][0] (the chunk_id) against
    the kv_b_seeded bucket. If Task B stored that exact chunk_id for this path before
    seeding, the change is Task B's own write — skip it. Otherwise it's a phone edit.

    Failed commits are re-queued so they are retried on the next debounce cycle rather
    than silently dropped.
    """
    # "now" on first run: don't replay historical changes (e.g. from prior seeding runs).
    # Only watch for changes that happen while this deployment is running.
    current_seq = await kv_get(kv, KV_LAST_CC_SEQ) or "now"
    pr_pending: dict[str, str | None] = {}
    last_activity = 0.0
    backoff = 1

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None)) as session:
        while not shutdown.is_set():
            try:
                async for seq, row in stream_changes(session, CLAUDE_CONFIG_DB, current_seq):
                    if shutdown.is_set():
                        break

                    if seq is None:
                        # Heartbeat — fire commit if debounce window has elapsed
                        if pr_pending and time.monotonic() - last_activity >= PR_DEBOUNCE:
                            items = list(pr_pending.items())
                            pr_pending.clear()
                            await kv_put(kv, KV_LAST_CC_SEQ, current_seq)
                            loop = asyncio.get_running_loop()
                            failed = await loop.run_in_executor(None, commit_to_claude, items)
                            # Re-queue failures so next debounce cycle retries them
                            for path, content in failed:
                                pr_pending[path] = content
                            if failed:
                                last_activity = time.monotonic()
                        continue

                    doc = row.get("doc", {})
                    doc_id: str = doc.get("_id", "")
                    current_seq = str(seq)

                    if not doc_id or any(doc_id.startswith(p) for p in CLAUDE_CONFIG_SKIP_PREFIXES) or doc_id in CLAUDE_CONFIG_SKIP_EXACT:
                        continue

                    if row.get("deleted") or doc.get("_deleted"):
                        local = safe_path(CLAUDE_DIR, doc_id)
                        if local and local.exists():
                            pr_pending[doc_id] = None
                            last_activity = time.monotonic()
                        continue

                    # Loop killer: check whether Task B seeded this exact chunk.
                    # kv_b_seeded stores lower_path → chunk_id with a 5-min TTL.
                    # If the incoming chunk matches what Task B stored, skip it.
                    incoming_chunks = doc.get("children", [])
                    if incoming_chunks:
                        stored_chunk = await kv_get(kv_b_seeded, doc_id.lower())
                        if stored_chunk == incoming_chunks[0]:
                            log.debug("[task-c] skipping Task B write: %s", doc_id)
                            continue

                    content = await assemble_content(session, CLAUDE_CONFIG_DB, doc)
                    pr_pending[doc_id] = content
                    last_activity = time.monotonic()
                    log.info("[task-c] queued phone edit: %s", doc_id)

                # Stream ended cleanly
                if pr_pending:
                    items = list(pr_pending.items())
                    pr_pending.clear()
                    await kv_put(kv, KV_LAST_CC_SEQ, current_seq)
                    loop = asyncio.get_running_loop()
                    failed = await loop.run_in_executor(None, commit_to_claude, items)
                    for path, content in failed:
                        pr_pending[path] = content
                    if failed:
                        last_activity = time.monotonic()
                backoff = 1

            except Exception as e:
                log.warning("[task-c] stream error: %s(%s) — reconnecting in %ds", type(e).__name__, e, backoff)
                # If the DB was deleted/recreated the stored sequence is invalid.
                # Reset to "now" so we don't replay stale history on reconnect.
                if "404" in str(e) or "Not Found" in str(e):
                    log.info("[task-c] DB not found — resetting sequence to 'now'")
                    current_seq = "now"
                    await kv_put(kv, KV_LAST_CC_SEQ, current_seq)
                try:
                    await asyncio.wait_for(shutdown.wait(), timeout=float(backoff))
                    break
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 2, 60)

    log.info("[task-c] stopped")


# ── Task D: vault repo → obsidian DB ─────────────────────────────────────────

def _pull_or_clone_vault_poll() -> Repo:
    """Pull vault repo (main) into VAULT_POLL_DIR if present, re-clone only if corrupt."""
    import shutil
    if VAULT_POLL_DIR.exists():
        try:
            repo = Repo(VAULT_POLL_DIR)
            repo.remotes.origin.set_url(_github_url(VAULT_REPO))
            repo.git.fetch("origin")
            repo.git.checkout("main")
            repo.git.reset("--hard", "origin/main")
            return repo
        except InvalidGitRepositoryError:
            shutil.rmtree(VAULT_POLL_DIR)
        except Exception as e:
            log.warning("[task-d] fetch failed (%s) — using last known state", e)
            try:
                return Repo(VAULT_POLL_DIR)
            except Exception:
                shutil.rmtree(VAULT_POLL_DIR)
    return clone_repo(VAULT_POLL_DIR, VAULT_REPO)


async def task_d_vault_to_couchdb(kv, shutdown: asyncio.Event) -> None:
    """Poll vault repo every VAULT_POLL_INTERVAL seconds; seed changes to obsidian DB.

    Conflict handling: agent writes land in CouchDB normally. If LiveSync's local
    Obsidian storage has a diverging edit, LiveSync surfaces a conflict dialog in
    Obsidian — the user resolves it there. No silent phone-wins.
    Loop killer: seed_file's idempotency check (same content → no write) means
    Task A's own commits produce no new CouchDB events.
    Skips files that would be ignored by Task A (OBSIDIAN_SKIP_PREFIXES).

    On first iteration:
    - Removes lowercase duplicate files left by old vault-bridge versions.
    - Seeds any vault files missing from obsidian DB (completeness check).
    """
    log.info("[task-d] starting")
    loop = asyncio.get_running_loop()
    first_run = True

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        while not shutdown.is_set():
            try:
                repo = await loop.run_in_executor(None, _pull_or_clone_vault_poll)

                if first_run:
                    first_run = False
                    # Seed any files missing from obsidian DB (handles DB wipes)
                    await seed_missing_vault_files(session, VAULT_POLL_DIR)

                current_sha = repo.head.commit.hexsha
                last_sha = await kv_get(kv, KV_LAST_VAULT_SHA)

                if last_sha == current_sha:
                    log.debug("[task-d] no new vault commits")
                else:
                    log.info("[task-d] syncing vault %s → %s",
                             (last_sha or "none")[:8], current_sha[:8])
                    # Run case-duplicate cleanup on every sync so new folders
                    # with case collisions are caught, not just on first startup.
                    removed = await loop.run_in_executor(
                        None, cleanup_case_duplicates_sync, VAULT_POLL_DIR, repo
                    )
                    if removed:
                        # Re-pull so the diff is computed against the post-cleanup HEAD
                        repo = await loop.run_in_executor(None, _pull_or_clone_vault_poll)
                        current_sha = repo.head.commit.hexsha

                    if last_sha:
                        diff_out = repo.git.diff("--name-status", last_sha, current_sha)
                        changes = _parse_diff_name_status(diff_out)
                    else:
                        changes = []
                        for abs_path in sorted(VAULT_POLL_DIR.rglob("*")):
                            if not abs_path.is_file():
                                continue
                            rel = abs_path.relative_to(VAULT_POLL_DIR)
                            if any(p.startswith(".") for p in rel.parts):
                                continue
                            changes.append(("A", str(rel)))

                    ok = skipped = fail = deleted = 0
                    for status, rel_path in changes:
                        # Skip hidden paths and anything Task A would ignore
                        if any(p.startswith(".") for p in Path(rel_path).parts):
                            continue
                        if any(rel_path.startswith(p) for p in OBSIDIAN_SKIP_PREFIXES) or rel_path in OBSIDIAN_SKIP_EXACT:
                            continue

                        if status == "D":
                            if await delete_couch_doc(session, OBSIDIAN_DB, rel_path, caller="task-d"):
                                deleted += 1
                            else:
                                fail += 1
                        else:
                            # Skip files whose last commit was by vault-bridge — they
                            # originated as phone edits and re-seeding would cause conflicts.
                            if last_sha and _is_obsidian_commit(repo, rel_path, last_sha, current_sha):
                                skipped += 1
                                log.debug("[task-d] skipping phone-originated file: %s", rel_path)
                                continue

                            abs_path = VAULT_POLL_DIR / rel_path
                            if not abs_path.is_file():
                                skipped += 1
                                continue
                            try:
                                content = abs_path.read_text(encoding="utf-8")
                            except (UnicodeDecodeError, IOError):
                                skipped += 1
                                continue
                            result = await seed_file(session, OBSIDIAN_DB, rel_path, content)
                            if result is True:
                                ok += 1
                            elif result is None:
                                skipped += 1
                            else:
                                log.warning("[task-d] failed to seed %s", rel_path)
                                fail += 1

                    log.info("[task-d] seeded %d, deleted %d (%d skipped, %d failed)",
                             ok, deleted, skipped, fail)
                    if fail == 0:
                        await kv_put(kv, KV_LAST_VAULT_SHA, current_sha)
                        log.info("[task-d] advanced last-vault-sha to %s", current_sha[:8])
                    else:
                        log.warning("[task-d] not advancing SHA due to %d failures", fail)

            except Exception as e:
                log.warning("[task-d] error: %s", e)

            try:
                await asyncio.wait_for(shutdown.wait(), timeout=float(VAULT_POLL_INTERVAL))
                break
            except asyncio.TimeoutError:
                pass

    log.info("[task-d] stopped")


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    configure_git_credentials()

    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown.set)

    nc = await connect_nats()
    js = nc.jetstream()
    try:
        kv = await js.key_value(KV_BUCKET)
    except Exception:
        kv = await js.create_key_value(nats.js.api.KeyValueConfig(bucket=KV_BUCKET))

    log.info("vault-bridge: starting (flush=%ds, claude_poll=%ds, vault_poll=%ds, pr_debounce=%ds)",
             FLUSH_INTERVAL, CLAUDE_POLL_INTERVAL, VAULT_POLL_INTERVAL, PR_DEBOUNCE)

    # Short-lived KV bucket for loop killer: Task B stores chunk_ids here before
    # seeding so Task C can identify Task B's own writes and not echo them back.
    try:
        kv_b_seeded = await js.key_value(KV_B_SEEDED_BUCKET)
    except Exception:
        kv_b_seeded = await js.create_key_value(
            nats.js.api.KeyValueConfig(bucket=KV_B_SEEDED_BUCKET, ttl=KV_B_SEEDED_TTL_S)
        )

    # Repair obsydian_livesync_version in both databases at startup.
    # This self-heals the "remote database is corrupted" error caused by any agent
    # or tool that writes the doc with wrong metadata.
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as _repair_session:
        await couch_ensure_db(_repair_session, OBSIDIAN_DB)
        await couch_ensure_db(_repair_session, CLAUDE_CONFIG_DB)
        await repair_livesync_version(_repair_session, OBSIDIAN_DB)
        await repair_livesync_version(_repair_session, CLAUDE_CONFIG_DB)

    results = await asyncio.gather(
        task_a_obsidian_to_vault(kv, shutdown),
        task_b_claude_to_couchdb(kv, kv_b_seeded, shutdown),
        task_c_couchdb_to_claude_pr(kv, kv_b_seeded, shutdown),
        task_d_vault_to_couchdb(kv, shutdown),
        return_exceptions=True,
    )
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            log.error("task-%s unhandled exception: %s", ["a", "b", "c", "d"][i], r)

    await nc.drain()
    await nc.close()
    log.info("vault-bridge: done")


if __name__ == "__main__":
    asyncio.run(main())
