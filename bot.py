import asyncio
import fnmatch
import json
import logging
import re
import secrets
import shutil
import sqlite3
import sys
import time
import uuid
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Any

import requests
from telethon import TelegramClient, events
from telethon.errors import RPCError

try:
    from telethon.network.connection.tcpabridged import ConnectionTcpAbridged
    from telethon.network.connection.tcpfull import ConnectionTcpFull
    from telethon.network.connection.tcpintermediate import ConnectionTcpIntermediate
except ImportError:
    ConnectionTcpAbridged = None
    ConnectionTcpFull = None
    ConnectionTcpIntermediate = None

from bale import BaleApi, BaleThrottledEditor, prime_bale_offset, spawn_bale_task
from config import (
    ADMIN_BALE_IDS,
    ADMIN_REST_HOST,
    ADMIN_REST_PORT,
    ADMIN_REST_TOKEN,
    ADMIN_TELEGRAM_IDS,
    API_HASH,
    API_ID,
    AUTO_UID_LENGTH,
    BALE_API_URL,
    BALE_BOT_TOKEN,
    BALE_MAX_UPLOAD_BYTES,
    BALE_ZIP_PART_BYTES,
    BALE_BROADCAST_DELAY_SECONDS,
    BOT_API_URL,
    BOT_SESSION,
    BOT_TOKEN,
    BOT_USERNAME,
    BROADCAST_RETRY_COUNT,
    DB_PATH,
    DOWNLOAD_CLEANUP_INTERVAL_SECONDS,
    DOWNLOAD_DIR,
    DOWNLOAD_DIR_MAX_BYTES,
    MAX_CONCURRENT_DOWNLOADS,
    MAX_KEYS_PER_USER,
    MAX_PENDING_PER_USER,
    MAX_QUEUE_SIZE,
    MAX_UPLOAD_BYTES,
    MODERATION_DB_PATH,
    PROGRESS_INTERVAL_SECONDS,
    REST_MAX_BODY_BYTES,
    SESSION,
    SIZE_LIMIT_EXCEEDED,
    TELEGRAM_BROADCAST_DELAY_SECONDS,
    TELEGRAM_UPLOAD_RETRY_COUNT,
    TELEGRAM_UPLOAD_TIMEOUT_SECONDS,
    TELETHON_CONNECTION,
    USER_PASSWORD,
    USER_PHONE,
)
from download_utils import (
    choose_download_filename,
    compute_sha256,
    create_zip,
    download_with_progress,
    improve_filename_from_file,
    make_hash_caption,
    probe_response_meta,
    shorten_filename,
    should_zip,
    split_file,
)
from models import Job, UserRef
from safety import (
    ascii_filename,
    resolve_safe_download_url,
    sanitize_filename,
    validate_public_http_url,
)
from text_commands import (
    banned_message,
    blocked_message,
    extract_hash_query,
    extract_key,
    extract_key_command,
    extract_url,
    format_key_rows,
    format_log_rows,
    parse_appeal_command,
    parse_auto_command,
)

try:
    from aiohttp import web
except ImportError:  # REST admin API is disabled until aiohttp is installed.
    web = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("linktofile")


def telegram_connection_class() -> object | None:
    mapping = {
        "tcpabridged": ConnectionTcpAbridged,
        "abridged": ConnectionTcpAbridged,
        "tcpintermediate": ConnectionTcpIntermediate,
        "intermediate": ConnectionTcpIntermediate,
        "tcpfull": ConnectionTcpFull,
        "full": ConnectionTcpFull,
    }
    connection = mapping.get(TELETHON_CONNECTION)
    if connection is None and TELETHON_CONNECTION not in {"", "default"}:
        logger.warning(
            "Unknown or unavailable TELETHON_CONNECTION=%s; using Telethon default",
            TELETHON_CONNECTION,
        )
    return connection


def make_telegram_client(session: str) -> TelegramClient:
    connection = telegram_connection_class()
    if connection is None:
        return TelegramClient(session, API_ID, API_HASH)
    return TelegramClient(session, API_ID, API_HASH, connection=connection)


class ThrottledEditor:
    def __init__(self, client: TelegramClient, chat_id: int, message_id: int) -> None:
        self._client = client
        self._chat_id = chat_id
        self._message_id = message_id
        self._last_update = 0.0

    async def update(self, text: str, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._last_update < PROGRESS_INTERVAL_SECONDS:
            return
        try:
            await self._client.edit_message(self._chat_id, self._message_id, text)
        except RPCError:
            return
        self._last_update = now


queue: asyncio.Queue[Job] = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
pending_by_user: dict[int, int] = defaultdict(int)
pending_links_by_user: dict[int, set[str]] = defaultdict(set)
per_user_semaphore: dict[int, asyncio.Semaphore] = defaultdict(
    lambda: asyncio.Semaphore(MAX_PENDING_PER_USER)
)
global_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
bale_pending_by_user: dict[int, int] = defaultdict(int)
bale_pending_links_by_user: dict[int, set[str]] = defaultdict(set)
bale_per_user_semaphore: dict[int, asyncio.Semaphore] = defaultdict(
    lambda: asyncio.Semaphore(MAX_PENDING_PER_USER)
)
bale_global_semaphore = asyncio.Semaphore(4)
bot_upload_target: str | None = None
user_self_id: int | None = None
bot_self_id: int | None = None
bot_api_offset = 0
bot_api_lock = asyncio.Lock()
db_lock = asyncio.Lock()
db_conn: sqlite3.Connection | None = None
mod_db_lock = asyncio.Lock()
mod_db_conn: sqlite3.Connection | None = None
active_job_dirs: set[Path] = set()
bale_api_offset = 0
bale_api: BaleApi | None = None


def bytes_to_mb(value: int) -> float:
    return round(value / (1024 * 1024), 2)


def make_temp_dir(prefix: str) -> Path:
    path = DOWNLOAD_DIR / f"{prefix}-{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=True)
    active_job_dirs.add(path.resolve())
    return path


def cleanup_temp_dir(path: Path) -> None:
    active_job_dirs.discard(path.resolve())
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)


def generate_auto_uid() -> str:
    return uuid.uuid4().hex[:AUTO_UID_LENGTH]


def format_username(username: str | None, fallback_id: int | None) -> str:
    if username:
        return username if username.startswith("@") else f"@{username}"
    return str(fallback_id) if fallback_id is not None else "unknown"


async def format_telegram_user(
    client: TelegramClient, user_id: int | None
) -> str:
    if user_id is None:
        return "unknown"
    try:
        entity = await client.get_entity(user_id)
    except Exception:
        return str(user_id)
    username = getattr(entity, "username", None)
    return format_username(username, user_id)


def format_bale_user(sender: dict | None, fallback_id: int | None) -> str:
    if sender:
        username = sender.get("username") or sender.get("user_name")
        if username:
            return format_username(username, fallback_id)
    return str(fallback_id) if fallback_id is not None else "unknown"


async def get_telegram_entity(
    client: TelegramClient, user_id: int
) -> object | None:
    try:
        return await client.get_input_entity(user_id)
    except Exception as exc:
        logger.warning("Telegram entity lookup failed user_id=%s err=%s", user_id, exc)
        return None


async def upload_with_progress(
    user_client: TelegramClient,
    target: object,
    file_path: Path,
    editor: ThrottledEditor,
    caption: str | None,
) -> object:
    last_update = 0.0

    async def progress_callback(current: int, total: int) -> None:
        nonlocal last_update
        now = time.monotonic()
        if now - last_update < PROGRESS_INTERVAL_SECONDS:
            return
        last_update = now
        await editor.update(
            f"Uploading... {bytes_to_mb(current)} / {bytes_to_mb(total)} MB"
        )

    kwargs: dict[str, object] = {"progress_callback": progress_callback}
    if caption and caption.strip():
        kwargs["caption"] = caption
        kwargs["parse_mode"] = None
    file_size = file_path.stat().st_size
    last_error: Exception | None = None
    for attempt in range(TELEGRAM_UPLOAD_RETRY_COUNT + 1):
        try:
            logger.info(
                "Telegram upload start target=%s file=%s size=%s attempt=%s",
                target,
                file_path.name,
                file_size,
                attempt + 1,
            )
            return await asyncio.wait_for(
                user_client.send_file(target, file_path, **kwargs),
                timeout=TELEGRAM_UPLOAD_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            last_error = exc
            logger.warning(
                "Telegram upload failed attempt=%s/%s file=%s error=%s",
                attempt + 1,
                TELEGRAM_UPLOAD_RETRY_COUNT + 1,
                file_path.name,
                exc,
            )
            if attempt >= TELEGRAM_UPLOAD_RETRY_COUNT:
                break
            await editor.update("Upload connection failed. Retrying...", force=True)
            try:
                await user_client.disconnect()
                await asyncio.sleep(2 * (attempt + 1))
                await user_client.connect()
                if not await user_client.is_user_authorized():
                    raise RuntimeError("Telegram upload account is not authorized")
            except Exception as reconnect_exc:
                logger.warning("Telegram upload reconnect failed: %s", reconnect_exc)
                await asyncio.sleep(2 * (attempt + 1))
    raise RuntimeError(f"Telegram upload failed: {last_error}")


async def relay_via_bot(
    bot_client: TelegramClient,
    job: Job,
    upload_message_id: int,
    caption: str | None,
) -> None:
    if user_self_id is None:
        raise RuntimeError("Bot relay is not configured.")
    for _ in range(10):
        try:
            await copy_message_via_bot_api(
                job.chat_id, user_self_id, upload_message_id, caption
            )
            return
        except Exception:
            await asyncio.sleep(1)
    raise RuntimeError("Bot relay failed to access uploaded media.")


async def copy_message_via_bot_api(
    chat_id: int, from_chat_id: int, message_id: int, caption: str | None
) -> None:
    def _do_request() -> None:
        url = f"{BOT_API_URL.rstrip('/')}/bot{BOT_TOKEN}/copyMessage"
        data = {
            "chat_id": str(chat_id),
            "from_chat_id": str(from_chat_id),
            "message_id": str(message_id),
        }
        if caption is not None:
            data["caption"] = caption
        payload = urllib.parse.urlencode(data).encode()
        req = urllib.request.Request(url, data=payload)
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode()
        data = json.loads(body)
        if not data.get("ok"):
            raise RuntimeError(data.get("description", "copyMessage failed"))

    await asyncio.to_thread(_do_request)


async def get_updates_via_bot_api(timeout_seconds: int = 5) -> list[dict]:
    global bot_api_offset

    def _do_request(offset: int) -> dict:
        url = f"{BOT_API_URL.rstrip('/')}/bot{BOT_TOKEN}/getUpdates"
        payload = urllib.parse.urlencode(
            {"offset": str(offset), "timeout": str(timeout_seconds)}
        ).encode()
        req = urllib.request.Request(url, data=payload)
        with urllib.request.urlopen(req, timeout=timeout_seconds + 10) as resp:
            body = resp.read().decode()
        return json.loads(body)

    async with bot_api_lock:
        data = await asyncio.to_thread(_do_request, bot_api_offset)
        if not data.get("ok"):
            raise RuntimeError(data.get("description", "getUpdates failed"))
        results = data.get("result", [])
        for update in results:
            bot_api_offset = max(bot_api_offset, update.get("update_id", 0) + 1)
        return results


def utc_ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def normalize_url(url: str | None) -> str:
    if not url:
        return ""
    parsed = urllib.parse.urlsplit(url.strip())
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    path = urllib.parse.quote(urllib.parse.unquote(parsed.path), safe="/%:@")
    return urllib.parse.urlunsplit((scheme, netloc, path, parsed.query, ""))


def normalize_url_pattern(pattern: str | None) -> str:
    if not pattern:
        return ""
    parsed = urllib.parse.urlsplit(pattern.strip())
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    path = urllib.parse.quote(urllib.parse.unquote(parsed.path), safe="/%:@*")
    query = urllib.parse.quote(urllib.parse.unquote(parsed.query), safe="=&;%:@/?*")
    return urllib.parse.urlunsplit((scheme, netloc, path, query, ""))


def domain_from_url(url: str | None) -> str:
    if not url:
        return ""
    host = urllib.parse.urlsplit(url.strip()).hostname or ""
    return host.lower().removeprefix("www.")


def normalize_block_value(kind: str, value: str) -> str:
    if kind == "link":
        if "*" in value:
            return normalize_url_pattern(value)
        return normalize_url(value)
    normalized = value.strip().lower()
    if kind == "domain":
        normalized = normalized.removeprefix("www.")
    return normalized


def admin_name(platform: str, user_id: int | None) -> str:
    return f"{platform}:{user_id}" if user_id is not None else "system"


def is_telegram_admin(user_id: int | None) -> bool:
    return user_id is not None and user_id in ADMIN_TELEGRAM_IDS


def is_bale_admin(user_id: int | None) -> bool:
    return user_id is not None and user_id in ADMIN_BALE_IDS


def row_to_dict(cursor: sqlite3.Cursor, row: tuple) -> dict:
    return {description[0]: row[index] for index, description in enumerate(cursor.description)}


def _execute_mod_schema() -> None:
    if mod_db_conn is None:
        return
    mod_db_conn.execute(
        "CREATE TABLE IF NOT EXISTS abuse_log ("
        "link TEXT NOT NULL,"
        "file_hash TEXT NOT NULL,"
        "platform TEXT NOT NULL,"
        "user_id INTEGER NOT NULL,"
        "chat_id INTEGER,"
        "username TEXT,"
        "filename TEXT,"
        "file_size INTEGER,"
        "content_type TEXT,"
        "first_seen_at TEXT NOT NULL,"
        "last_seen_at TEXT NOT NULL,"
        "times_seen INTEGER NOT NULL DEFAULT 1,"
        "status TEXT NOT NULL DEFAULT 'clean',"
        "PRIMARY KEY(link, file_hash, platform, user_id))"
    )
    mod_db_conn.execute("CREATE INDEX IF NOT EXISTS idx_abuse_log_hash ON abuse_log(file_hash)")
    mod_db_conn.execute("CREATE INDEX IF NOT EXISTS idx_abuse_log_link ON abuse_log(link)")
    mod_db_conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_abuse_log_user ON abuse_log(platform, user_id)"
    )
    mod_db_conn.execute(
        "CREATE TABLE IF NOT EXISTS banned_users ("
        "platform TEXT NOT NULL,"
        "user_id INTEGER NOT NULL,"
        "reason TEXT,"
        "created_at TEXT NOT NULL,"
        "created_by_platform TEXT,"
        "created_by_user_id INTEGER,"
        "expires_at TEXT,"
        "appeal_allowed INTEGER NOT NULL DEFAULT 1,"
        "PRIMARY KEY(platform, user_id))"
    )
    mod_db_conn.execute(
        "CREATE TABLE IF NOT EXISTS content_blocklist ("
        "type TEXT NOT NULL,"
        "value TEXT NOT NULL,"
        "reason TEXT,"
        "created_at TEXT NOT NULL,"
        "created_by TEXT,"
        "action TEXT NOT NULL DEFAULT 'quarantine',"
        "PRIMARY KEY(type, value))"
    )
    mod_db_conn.execute(
        "CREATE TABLE IF NOT EXISTS quarantine ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "platform TEXT NOT NULL,"
        "user_id INTEGER NOT NULL,"
        "chat_id INTEGER,"
        "username TEXT,"
        "link TEXT,"
        "file_hash TEXT,"
        "filename TEXT,"
        "file_size INTEGER,"
        "reason TEXT,"
        "status TEXT NOT NULL DEFAULT 'pending',"
        "created_at TEXT NOT NULL,"
        "resolved_at TEXT,"
        "admin_note TEXT)"
    )
    mod_db_conn.execute(
        "CREATE TABLE IF NOT EXISTS registered_users ("
        "platform TEXT NOT NULL,"
        "user_id INTEGER NOT NULL,"
        "chat_id INTEGER,"
        "username TEXT,"
        "first_seen_at TEXT NOT NULL,"
        "last_seen_at TEXT NOT NULL,"
        "auto_enabled INTEGER NOT NULL DEFAULT 0,"
        "PRIMARY KEY(platform, user_id))"
    )
    mod_db_conn.execute(
        "CREATE TABLE IF NOT EXISTS broadcast_jobs ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "target_platform TEXT NOT NULL,"
        "message TEXT NOT NULL,"
        "status TEXT NOT NULL DEFAULT 'pending',"
        "created_at TEXT NOT NULL,"
        "started_at TEXT,"
        "finished_at TEXT,"
        "created_by TEXT)"
    )
    mod_db_conn.execute(
        "CREATE TABLE IF NOT EXISTS broadcast_deliveries ("
        "job_id INTEGER NOT NULL,"
        "platform TEXT NOT NULL,"
        "user_id INTEGER NOT NULL,"
        "chat_id INTEGER,"
        "status TEXT NOT NULL DEFAULT 'pending',"
        "attempts INTEGER NOT NULL DEFAULT 0,"
        "error TEXT,"
        "sent_at TEXT,"
        "PRIMARY KEY(job_id, platform, user_id))"
    )
    mod_db_conn.execute(
        "CREATE TABLE IF NOT EXISTS appeals ("
        "id TEXT PRIMARY KEY,"
        "platform TEXT NOT NULL,"
        "user_id INTEGER NOT NULL,"
        "message TEXT,"
        "status TEXT NOT NULL DEFAULT 'open',"
        "created_at TEXT NOT NULL,"
        "resolved_at TEXT,"
        "admin_note TEXT)"
    )
    mod_db_conn.commit()


def _migrate_key_cache() -> None:
    if db_conn is None:
        return
    cursor = db_conn.execute("PRAGMA table_info(key_cache)")
    columns = {row[1] for row in cursor.fetchall()}
    if "owner_platform" not in columns:
        db_conn.execute("ALTER TABLE key_cache ADD COLUMN owner_platform TEXT")
    if "owner_user_id" not in columns:
        db_conn.execute("ALTER TABLE key_cache ADD COLUMN owner_user_id INTEGER")
    if "created_at" not in columns:
        db_conn.execute("ALTER TABLE key_cache ADD COLUMN created_at TEXT")
    db_conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_key_cache_owner "
        "ON key_cache(owner_platform, owner_user_id)"
    )
    db_conn.commit()


async def mod_register_user(user: UserRef, auto_enabled: bool | None = None) -> None:
    if mod_db_conn is None:
        return
    now = utc_ts()
    async with mod_db_lock:
        mod_db_conn.execute(
            "INSERT INTO registered_users(platform, user_id, chat_id, username, first_seen_at, last_seen_at, auto_enabled) "
            "VALUES(?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(platform, user_id) DO UPDATE SET "
            "chat_id=excluded.chat_id, username=excluded.username, last_seen_at=excluded.last_seen_at, "
            "auto_enabled=CASE WHEN ? IS NULL THEN registered_users.auto_enabled ELSE excluded.auto_enabled END",
            (
                user.platform,
                user.user_id,
                user.chat_id,
                user.username,
                now,
                now,
                int(bool(auto_enabled)) if auto_enabled is not None else 0,
                auto_enabled,
            ),
        )
        mod_db_conn.commit()


async def mod_is_banned(platform: str, user_id: int) -> dict | None:
    if mod_db_conn is None:
        return None
    async with mod_db_lock:
        cursor = mod_db_conn.execute(
            "SELECT platform, user_id, reason, created_at, expires_at, appeal_allowed "
            "FROM banned_users WHERE platform = ? AND user_id = ?",
            (platform, user_id),
        )
        row = cursor.fetchone()
    if not row:
        return None
    return {
        "platform": row[0],
        "user_id": row[1],
        "reason": row[2],
        "created_at": row[3],
        "expires_at": row[4],
        "appeal_allowed": bool(row[5]),
    }


async def mod_ban_user(
    platform: str,
    user_id: int,
    reason: str,
    created_by_platform: str | None = None,
    created_by_user_id: int | None = None,
) -> None:
    if mod_db_conn is None:
        return
    async with mod_db_lock:
        mod_db_conn.execute(
            "INSERT OR REPLACE INTO banned_users(platform, user_id, reason, created_at, created_by_platform, created_by_user_id, appeal_allowed) "
            "VALUES(?, ?, ?, ?, ?, ?, 1)",
            (platform, user_id, reason, utc_ts(), created_by_platform, created_by_user_id),
        )
        mod_db_conn.commit()


async def mod_unban_user(platform: str, user_id: int) -> None:
    if mod_db_conn is None:
        return
    async with mod_db_lock:
        mod_db_conn.execute(
            "DELETE FROM banned_users WHERE platform = ? AND user_id = ?",
            (platform, user_id),
        )
        mod_db_conn.commit()


async def mod_add_block(kind: str, value: str, reason: str, created_by: str, action: str = "quarantine") -> None:
    if mod_db_conn is None:
        return
    normalized = normalize_block_value(kind, value)
    async with mod_db_lock:
        mod_db_conn.execute(
            "INSERT OR REPLACE INTO content_blocklist(type, value, reason, created_at, created_by, action) "
            "VALUES(?, ?, ?, ?, ?, ?)",
            (kind, normalized, reason, utc_ts(), created_by, action),
        )
        mod_db_conn.commit()


async def mod_remove_block(kind: str, value: str) -> None:
    if mod_db_conn is None:
        return
    normalized = normalize_block_value(kind, value)
    async with mod_db_lock:
        mod_db_conn.execute(
            "DELETE FROM content_blocklist WHERE type = ? AND value = ?",
            (kind, normalized),
        )
        mod_db_conn.commit()


async def mod_find_block_for_link(url: str) -> dict | None:
    if mod_db_conn is None:
        return None
    normalized = normalize_url(url)
    domain = domain_from_url(url)
    async with mod_db_lock:
        cursor = mod_db_conn.execute(
            "SELECT type, value, reason, action FROM content_blocklist "
            "WHERE (type = 'link' AND value = ?) OR (type = 'domain' AND value = ?) "
            "LIMIT 1",
            (normalized, domain),
        )
        row = cursor.fetchone()
        if not row:
            cursor = mod_db_conn.execute(
                "SELECT type, value, reason, action FROM content_blocklist "
                "WHERE type = 'link' AND instr(value, '*') > 0"
            )
            rows = cursor.fetchall()
            row = next(
                (
                    candidate
                    for candidate in rows
                    if fnmatch.fnmatchcase(normalized, candidate[1])
                ),
                None,
            )
    return {"type": row[0], "value": row[1], "reason": row[2], "action": row[3]} if row else None


async def mod_find_block_for_hash(file_hash: str) -> dict | None:
    if mod_db_conn is None:
        return None
    async with mod_db_lock:
        cursor = mod_db_conn.execute(
            "SELECT type, value, reason, action FROM content_blocklist "
            "WHERE type = 'hash' AND value = ? LIMIT 1",
            (file_hash.lower(),),
        )
        row = cursor.fetchone()
    return {"type": row[0], "value": row[1], "reason": row[2], "action": row[3]} if row else None


async def mod_list_blocks(kind: str | None = None, limit: int = 50) -> list[dict]:
    if mod_db_conn is None:
        return []
    if kind and kind not in {"hash", "link", "domain"}:
        return []
    async with mod_db_lock:
        if kind:
            cursor = mod_db_conn.execute(
                "SELECT type, value, reason, created_at, created_by, action "
                "FROM content_blocklist WHERE type = ? "
                "ORDER BY created_at DESC LIMIT ?",
                (kind, limit),
            )
        else:
            cursor = mod_db_conn.execute(
                "SELECT type, value, reason, created_at, created_by, action "
                "FROM content_blocklist ORDER BY created_at DESC LIMIT ?",
                (limit,),
            )
        rows = cursor.fetchall()
    return [
        {
            "type": row[0],
            "value": row[1],
            "reason": row[2],
            "created_at": row[3],
            "created_by": row[4],
            "action": row[5],
        }
        for row in rows
    ]


def format_block_rows(rows: list[dict]) -> str:
    if not rows:
        return "No blocklist entries."
    lines = []
    for row in rows:
        reason = row.get("reason") or "-"
        created_at = row.get("created_at") or "-"
        lines.append(f"{row['type']} | {row['value']} | {reason} | {created_at}")
    return "\n".join(lines)


async def mod_log_abuse(
    user: UserRef,
    link: str | None,
    file_hash: str,
    filename: str | None,
    file_size: int | None,
    content_type: str | None,
    status: str,
) -> None:
    if mod_db_conn is None:
        return
    now = utc_ts()
    normalized_link = normalize_url(link) if link else ""
    async with mod_db_lock:
        mod_db_conn.execute(
            "INSERT INTO abuse_log(link, file_hash, platform, user_id, chat_id, username, filename, file_size, content_type, first_seen_at, last_seen_at, times_seen, status) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?) "
            "ON CONFLICT(link, file_hash, platform, user_id) DO UPDATE SET "
            "last_seen_at=excluded.last_seen_at, times_seen=abuse_log.times_seen + 1, "
            "chat_id=excluded.chat_id, username=excluded.username, filename=excluded.filename, "
            "file_size=excluded.file_size, content_type=excluded.content_type, status=excluded.status",
            (
                normalized_link,
                file_hash,
                user.platform,
                user.user_id,
                user.chat_id,
                user.username,
                filename,
                file_size,
                content_type,
                now,
                now,
                status,
            ),
        )
        mod_db_conn.commit()


async def mod_create_quarantine(
    user: UserRef,
    link: str | None,
    file_hash: str | None,
    filename: str | None,
    file_size: int | None,
    reason: str,
) -> int | None:
    if mod_db_conn is None:
        return None
    async with mod_db_lock:
        cursor = mod_db_conn.execute(
            "INSERT INTO quarantine(platform, user_id, chat_id, username, link, file_hash, filename, file_size, reason, status, created_at) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)",
            (
                user.platform,
                user.user_id,
                user.chat_id,
                user.username,
                normalize_url(link) if link else None,
                file_hash,
                filename,
                file_size,
                reason,
                utc_ts(),
            ),
        )
        mod_db_conn.commit()
        return int(cursor.lastrowid)


async def mod_search_logs(kind: str, value: str, limit: int = 20) -> list[dict]:
    if mod_db_conn is None:
        return []
    if kind == "hash":
        where = "file_hash = ?"
        args: tuple[object, ...] = (value,)
    elif kind == "link":
        where = "link = ?"
        args = (normalize_url(value),)
    elif kind == "user":
        platform, raw_user_id = value.split(":", 1)
        where = "platform = ? AND user_id = ?"
        args = (platform, int(raw_user_id))
    else:
        return []
    async with mod_db_lock:
        cursor = mod_db_conn.execute(
            f"SELECT link, file_hash, platform, user_id, username, filename, file_size, times_seen, status, last_seen_at "
            f"FROM abuse_log WHERE {where} ORDER BY last_seen_at DESC LIMIT ?",
            (*args, limit),
        )
        rows = cursor.fetchall()
    return [row_to_dict(cursor, row) for row in rows]


async def mod_list_quarantine(limit: int = 10) -> list[dict]:
    if mod_db_conn is None:
        return []
    async with mod_db_lock:
        cursor = mod_db_conn.execute(
            "SELECT id, platform, user_id, link, file_hash, filename, reason, status, created_at "
            "FROM quarantine ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        rows = cursor.fetchall()
    return [row_to_dict(cursor, row) for row in rows]


async def mod_update_quarantine(item_id: int, status: str, note: str | None = None) -> None:
    if mod_db_conn is None:
        return
    async with mod_db_lock:
        mod_db_conn.execute(
            "UPDATE quarantine SET status = ?, resolved_at = ?, admin_note = ? WHERE id = ?",
            (status, utc_ts(), note, item_id),
        )
        mod_db_conn.commit()


async def mod_create_or_update_appeal(user: UserRef, message: str) -> str:
    if mod_db_conn is None:
        return ""
    appeal_id = f"{user.platform}-{user.user_id}"
    async with mod_db_lock:
        mod_db_conn.execute(
            "INSERT INTO appeals(id, platform, user_id, message, status, created_at) "
            "VALUES(?, ?, ?, ?, 'open', ?) "
            "ON CONFLICT(id) DO UPDATE SET message=excluded.message, status='open', created_at=excluded.created_at, resolved_at=NULL, admin_note=NULL",
            (appeal_id, user.platform, user.user_id, message, utc_ts()),
        )
        mod_db_conn.commit()
    return appeal_id


async def mod_get_appeal(user: UserRef) -> dict | None:
    if mod_db_conn is None:
        return None
    async with mod_db_lock:
        cursor = mod_db_conn.execute(
            "SELECT id, status, message, created_at, resolved_at, admin_note FROM appeals "
            "WHERE platform = ? AND user_id = ?",
            (user.platform, user.user_id),
        )
        row = cursor.fetchone()
    return row_to_dict(cursor, row) if row else None


async def mod_list_appeals(limit: int = 10) -> list[dict]:
    if mod_db_conn is None:
        return []
    async with mod_db_lock:
        cursor = mod_db_conn.execute(
            "SELECT id, platform, user_id, status, message, created_at FROM appeals "
            "ORDER BY created_at DESC LIMIT ?",
            (limit,),
        )
        rows = cursor.fetchall()
    return [row_to_dict(cursor, row) for row in rows]


async def mod_resolve_appeal(appeal_id: str, status: str, note: str | None = None) -> dict | None:
    if mod_db_conn is None:
        return None
    async with mod_db_lock:
        cursor = mod_db_conn.execute(
            "SELECT platform, user_id FROM appeals WHERE id = ?",
            (appeal_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        mod_db_conn.execute(
            "UPDATE appeals SET status = ?, resolved_at = ?, admin_note = ? WHERE id = ?",
            (status, utc_ts(), note, appeal_id),
        )
        mod_db_conn.commit()
    if status == "accepted":
        await mod_unban_user(row[0], row[1])
    return {"platform": row[0], "user_id": row[1]}


async def mod_create_broadcast(target_platform: str, message: str, created_by: str) -> int | None:
    if mod_db_conn is None:
        return None
    async with mod_db_lock:
        cursor = mod_db_conn.execute(
            "INSERT INTO broadcast_jobs(target_platform, message, status, created_at, created_by) "
            "VALUES(?, ?, 'pending', ?, ?)",
            (target_platform, message, utc_ts(), created_by),
        )
        job_id = int(cursor.lastrowid)
        if target_platform == "both":
            platforms = ("telegram", "bale")
        else:
            platforms = (target_platform,)
        users = mod_db_conn.execute(
            "SELECT platform, user_id, chat_id FROM registered_users "
            f"WHERE platform IN ({','.join('?' for _ in platforms)})",
            platforms,
        ).fetchall()
        for platform, user_id, chat_id in users:
            mod_db_conn.execute(
                "INSERT OR IGNORE INTO broadcast_deliveries(job_id, platform, user_id, chat_id) "
                "VALUES(?, ?, ?, ?)",
                (job_id, platform, user_id, chat_id),
            )
        mod_db_conn.commit()
        return job_id


async def mod_next_broadcast_delivery() -> tuple[dict, dict] | None:
    if mod_db_conn is None:
        return None
    async with mod_db_lock:
        cursor = mod_db_conn.execute(
            "SELECT j.id, j.message, d.platform, d.user_id, d.chat_id, d.attempts "
            "FROM broadcast_deliveries d JOIN broadcast_jobs j ON j.id = d.job_id "
            "WHERE d.status = 'pending' AND j.status IN ('pending', 'running') "
            "ORDER BY j.id, d.platform, d.user_id LIMIT 1"
        )
        row = cursor.fetchone()
        if not row:
            return None
        mod_db_conn.execute(
            "UPDATE broadcast_jobs SET status = 'running', started_at = COALESCE(started_at, ?) WHERE id = ?",
            (utc_ts(), row[0]),
        )
        mod_db_conn.execute(
            "UPDATE broadcast_deliveries SET attempts = attempts + 1 WHERE job_id = ? AND platform = ? AND user_id = ?",
            (row[0], row[2], row[3]),
        )
        mod_db_conn.commit()
    job = {"id": row[0], "message": row[1]}
    delivery = {"platform": row[2], "user_id": row[3], "chat_id": row[4], "attempts": row[5] + 1}
    return job, delivery


async def mod_finish_broadcast_delivery(job_id: int, platform: str, user_id: int, ok: bool, error: str | None = None) -> None:
    if mod_db_conn is None:
        return
    async with mod_db_lock:
        attempts = mod_db_conn.execute(
            "SELECT attempts FROM broadcast_deliveries WHERE job_id = ? AND platform = ? AND user_id = ?",
            (job_id, platform, user_id),
        ).fetchone()[0]
        status = "sent" if ok else ("pending" if attempts < BROADCAST_RETRY_COUNT else "failed")
        mod_db_conn.execute(
            "UPDATE broadcast_deliveries SET status = ?, error = ?, sent_at = ? WHERE job_id = ? AND platform = ? AND user_id = ?",
            (status, error, utc_ts() if ok else None, job_id, platform, user_id),
        )
        remaining = mod_db_conn.execute(
            "SELECT COUNT(*) FROM broadcast_deliveries WHERE job_id = ? AND status = 'pending'",
            (job_id,),
        ).fetchone()[0]
        if remaining == 0:
            mod_db_conn.execute(
                "UPDATE broadcast_jobs SET status = 'finished', finished_at = ? WHERE id = ?",
                (utc_ts(), job_id),
            )
        mod_db_conn.commit()


def path_size(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        try:
            return path.stat().st_size
        except OSError:
            return 0
    total = 0
    for item in path.rglob("*"):
        if item.is_file():
            try:
                total += item.stat().st_size
            except OSError:
                continue
    return total


def is_active_path(path: Path) -> bool:
    try:
        resolved = path.resolve()
    except OSError:
        return False
    for active in active_job_dirs:
        if resolved == active or active in resolved.parents:
            return True
    return False


async def enforce_download_dir_limit() -> None:
    if not DOWNLOAD_DIR.exists():
        return
    total = await asyncio.to_thread(path_size, DOWNLOAD_DIR)
    if total <= DOWNLOAD_DIR_MAX_BYTES:
        return
    candidates: list[tuple[float, Path, int]] = []
    for item in DOWNLOAD_DIR.iterdir():
        if is_active_path(item):
            continue
        try:
            stat = item.stat()
        except OSError:
            continue
        candidates.append((stat.st_mtime, item, await asyncio.to_thread(path_size, item)))
    for _, item, size in sorted(candidates, key=lambda value: value[0]):
        if total <= DOWNLOAD_DIR_MAX_BYTES:
            break
        try:
            if item.is_dir():
                shutil.rmtree(item, ignore_errors=True)
            else:
                item.unlink(missing_ok=True)
            total -= size
            logger.info("cleanup removed path=%s bytes=%s", item, size)
        except OSError as exc:
            logger.warning("cleanup failed path=%s err=%s", item, exc)


async def cleanup_loop() -> None:
    while True:
        try:
            await enforce_download_dir_limit()
        except Exception as exc:
            logger.exception("download cleanup failed: %s", exc)
        await asyncio.sleep(DOWNLOAD_CLEANUP_INTERVAL_SECONDS)


async def db_get_message_id(file_hash: str) -> int | None:
    if db_conn is None:
        return None
    async with db_lock:
        cursor = db_conn.execute(
            "SELECT message_id FROM file_cache WHERE hash = ?", (file_hash,)
        )
        row = cursor.fetchone()
        return row[0] if row else None


async def db_set_message_id(file_hash: str, message_id: int) -> None:
    if db_conn is None:
        return
    async with db_lock:
        db_conn.execute(
            "INSERT OR REPLACE INTO file_cache(hash, message_id) VALUES(?, ?)",
            (file_hash, message_id),
        )
        db_conn.commit()


async def db_delete_hash(file_hash: str) -> None:
    if db_conn is None:
        return
    async with db_lock:
        db_conn.execute("DELETE FROM file_cache WHERE hash = ?", (file_hash,))
        db_conn.commit()


async def db_get_bale_file_id(file_hash: str) -> str | None:
    if db_conn is None:
        return None
    async with db_lock:
        cursor = db_conn.execute(
            "SELECT file_id FROM bale_file_cache WHERE hash = ?", (file_hash,)
        )
        row = cursor.fetchone()
        return row[0] if row else None


async def db_set_bale_file_id(file_hash: str, file_id: str) -> None:
    if db_conn is None:
        return
    async with db_lock:
        db_conn.execute(
            "INSERT OR REPLACE INTO bale_file_cache(hash, file_id) VALUES(?, ?)",
            (file_hash, file_id),
        )
        db_conn.commit()


async def db_delete_bale_hash(file_hash: str) -> None:
    if db_conn is None:
        return
    async with db_lock:
        db_conn.execute("DELETE FROM bale_file_cache WHERE hash = ?", (file_hash,))
        db_conn.commit()


async def db_get_key_entry(key: str) -> dict | None:
    if db_conn is None:
        return None
    async with db_lock:
        cursor = db_conn.execute(
            "SELECT key, source, tg_chat_id, tg_message_id, bale_file_id, filename, file_hash, owner_platform, owner_user_id, created_at "
            "FROM key_cache WHERE key = ?",
            (key,),
        )
        row = cursor.fetchone()
    if not row:
        return None
    return {
        "key": row[0],
        "source": row[1],
        "tg_chat_id": row[2],
        "tg_message_id": row[3],
        "bale_file_id": row[4],
        "filename": row[5],
        "file_hash": row[6],
        "owner_platform": row[7],
        "owner_user_id": row[8],
        "created_at": row[9],
    }


async def db_insert_key_entry(
    key: str,
    source: str,
    tg_chat_id: int | None = None,
    tg_message_id: int | None = None,
    bale_file_id: str | None = None,
    filename: str | None = None,
    file_hash: str | None = None,
    owner_platform: str | None = None,
    owner_user_id: int | None = None,
) -> bool:
    if db_conn is None:
        return False
    async with db_lock:
        cursor = db_conn.execute("SELECT 1 FROM key_cache WHERE key = ?", (key,))
        if cursor.fetchone():
            return False
        if owner_platform and owner_user_id is not None:
            cursor = db_conn.execute(
                "SELECT COUNT(*) FROM key_cache WHERE owner_platform = ? AND owner_user_id = ?",
                (owner_platform, owner_user_id),
            )
            if cursor.fetchone()[0] >= MAX_KEYS_PER_USER:
                return False
        db_conn.execute(
            "INSERT INTO key_cache(key, source, tg_chat_id, tg_message_id, bale_file_id, filename, file_hash, owner_platform, owner_user_id, created_at) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                key,
                source,
                tg_chat_id,
                tg_message_id,
                bale_file_id,
                filename,
                file_hash,
                owner_platform,
                owner_user_id,
                utc_ts(),
            ),
        )
        db_conn.commit()
        return True


async def db_update_key_entry(
    key: str,
    tg_chat_id: int | None = None,
    tg_message_id: int | None = None,
    bale_file_id: str | None = None,
    filename: str | None = None,
    file_hash: str | None = None,
) -> None:
    if db_conn is None:
        return
    fields: list[str] = []
    values: list[object] = []
    if tg_chat_id is not None:
        fields.append("tg_chat_id = ?")
        values.append(tg_chat_id)
    if tg_message_id is not None:
        fields.append("tg_message_id = ?")
        values.append(tg_message_id)
    if bale_file_id is not None:
        fields.append("bale_file_id = ?")
        values.append(bale_file_id)
    if filename is not None:
        fields.append("filename = ?")
        values.append(filename)
    if file_hash is not None:
        fields.append("file_hash = ?")
        values.append(file_hash)
    if not fields:
        return
    values.append(key)
    async with db_lock:
        db_conn.execute(
            f"UPDATE key_cache SET {', '.join(fields)} WHERE key = ?",
            tuple(values),
        )
        db_conn.commit()


async def db_get_key_entry_by_hash(file_hash: str) -> dict | None:
    if db_conn is None:
        return None
    async with db_lock:
        cursor = db_conn.execute(
            "SELECT key, source, tg_chat_id, tg_message_id, bale_file_id, filename, file_hash "
            "FROM key_cache WHERE file_hash = ? LIMIT 1",
            (file_hash,),
        )
        row = cursor.fetchone()
    if not row:
        return None
    return {
        "key": row[0],
        "source": row[1],
        "tg_chat_id": row[2],
        "tg_message_id": row[3],
        "bale_file_id": row[4],
        "filename": row[5],
        "file_hash": row[6],
    }


async def db_count_keys_for_owner(platform: str, user_id: int) -> int:
    if db_conn is None:
        return 0
    async with db_lock:
        cursor = db_conn.execute(
            "SELECT COUNT(*) FROM key_cache WHERE owner_platform = ? AND owner_user_id = ?",
            (platform, user_id),
        )
        return int(cursor.fetchone()[0])


async def db_list_keys_for_owner(platform: str, user_id: int) -> list[dict]:
    if db_conn is None:
        return []
    async with db_lock:
        cursor = db_conn.execute(
            "SELECT key, source, filename, file_hash, created_at FROM key_cache "
            "WHERE owner_platform = ? AND owner_user_id = ? ORDER BY created_at DESC, key",
            (platform, user_id),
        )
        rows = cursor.fetchall()
    return [
        {
            "key": row[0],
            "source": row[1],
            "filename": row[2],
            "file_hash": row[3],
            "created_at": row[4],
        }
        for row in rows
    ]


async def db_delete_key_for_owner(key: str, platform: str, user_id: int, admin: bool = False) -> bool:
    if db_conn is None:
        return False
    async with db_lock:
        if admin:
            cursor = db_conn.execute("DELETE FROM key_cache WHERE key = ?", (key,))
        else:
            cursor = db_conn.execute(
                "DELETE FROM key_cache WHERE key = ? AND owner_platform = ? AND owner_user_id = ?",
                (key, platform, user_id),
            )
        db_conn.commit()
        return cursor.rowcount > 0


async def db_get_auto_link_by_tg(tg_user_id: int) -> dict | None:
    if db_conn is None:
        return None
    async with db_lock:
        cursor = db_conn.execute(
            "SELECT uid, tg_user_id, bale_user_id FROM auto_link WHERE tg_user_id = ?",
            (tg_user_id,),
        )
        row = cursor.fetchone()
    if not row:
        return None
    return {"uid": row[0], "tg_user_id": row[1], "bale_user_id": row[2]}


async def db_get_auto_link_by_bale(bale_user_id: int) -> dict | None:
    if db_conn is None:
        return None
    async with db_lock:
        cursor = db_conn.execute(
            "SELECT uid, tg_user_id, bale_user_id FROM auto_link WHERE bale_user_id = ?",
            (bale_user_id,),
        )
        row = cursor.fetchone()
    if not row:
        return None
    return {"uid": row[0], "tg_user_id": row[1], "bale_user_id": row[2]}


async def db_get_auto_link_by_uid(uid: str) -> dict | None:
    if db_conn is None:
        return None
    async with db_lock:
        cursor = db_conn.execute(
            "SELECT uid, tg_user_id, bale_user_id FROM auto_link WHERE uid = ?",
            (uid,),
        )
        row = cursor.fetchone()
    if not row:
        return None
    return {"uid": row[0], "tg_user_id": row[1], "bale_user_id": row[2]}


async def db_set_auto_link(uid: str, tg_user_id: int, bale_user_id: int) -> None:
    if db_conn is None:
        return
    async with db_lock:
        db_conn.execute(
            "INSERT OR REPLACE INTO auto_link(uid, tg_user_id, bale_user_id) VALUES(?, ?, ?)",
            (uid, tg_user_id, bale_user_id),
        )
        db_conn.commit()


async def db_delete_auto_link_by_uid(uid: str) -> None:
    if db_conn is None:
        return
    async with db_lock:
        db_conn.execute("DELETE FROM auto_link WHERE uid = ?", (uid,))
        db_conn.commit()


async def db_delete_auto_link_by_tg(tg_user_id: int) -> None:
    if db_conn is None:
        return
    async with db_lock:
        db_conn.execute("DELETE FROM auto_link WHERE tg_user_id = ?", (tg_user_id,))
        db_conn.commit()


async def db_delete_auto_link_by_bale(bale_user_id: int) -> None:
    if db_conn is None:
        return
    async with db_lock:
        db_conn.execute("DELETE FROM auto_link WHERE bale_user_id = ?", (bale_user_id,))
        db_conn.commit()


async def db_get_auto_pending_by_uid(uid: str) -> dict | None:
    if db_conn is None:
        return None
    async with db_lock:
        cursor = db_conn.execute(
            "SELECT uid, source, tg_user_id, bale_user_id FROM auto_pending WHERE uid = ?",
            (uid,),
        )
        row = cursor.fetchone()
    if not row:
        return None
    return {
        "uid": row[0],
        "source": row[1],
        "tg_user_id": row[2],
        "bale_user_id": row[3],
    }


async def db_get_auto_pending_by_tg(tg_user_id: int) -> dict | None:
    if db_conn is None:
        return None
    async with db_lock:
        cursor = db_conn.execute(
            "SELECT uid, source, tg_user_id, bale_user_id FROM auto_pending WHERE tg_user_id = ?",
            (tg_user_id,),
        )
        row = cursor.fetchone()
    if not row:
        return None
    return {
        "uid": row[0],
        "source": row[1],
        "tg_user_id": row[2],
        "bale_user_id": row[3],
    }


async def db_get_auto_pending_by_bale(bale_user_id: int) -> dict | None:
    if db_conn is None:
        return None
    async with db_lock:
        cursor = db_conn.execute(
            "SELECT uid, source, tg_user_id, bale_user_id FROM auto_pending WHERE bale_user_id = ?",
            (bale_user_id,),
        )
        row = cursor.fetchone()
    if not row:
        return None
    return {
        "uid": row[0],
        "source": row[1],
        "tg_user_id": row[2],
        "bale_user_id": row[3],
    }


async def db_set_auto_pending(
    uid: str,
    source: str,
    tg_user_id: int | None = None,
    bale_user_id: int | None = None,
) -> None:
    if db_conn is None:
        return
    async with db_lock:
        db_conn.execute(
            "INSERT OR REPLACE INTO auto_pending(uid, source, tg_user_id, bale_user_id) VALUES(?, ?, ?, ?)",
            (uid, source, tg_user_id, bale_user_id),
        )
        db_conn.commit()


async def db_delete_auto_pending_by_uid(uid: str) -> None:
    if db_conn is None:
        return
    async with db_lock:
        db_conn.execute("DELETE FROM auto_pending WHERE uid = ?", (uid,))
        db_conn.commit()


async def db_delete_auto_pending_by_tg(tg_user_id: int) -> None:
    if db_conn is None:
        return
    async with db_lock:
        db_conn.execute("DELETE FROM auto_pending WHERE tg_user_id = ?", (tg_user_id,))
        db_conn.commit()


async def db_delete_auto_pending_by_bale(bale_user_id: int) -> None:
    if db_conn is None:
        return
    async with db_lock:
        db_conn.execute(
            "DELETE FROM auto_pending WHERE bale_user_id = ?", (bale_user_id,)
        )
        db_conn.commit()


async def find_bot_message_id(
    filename: str, file_hash: str, file_size: int, timeout_seconds: int = 30
) -> int | None:
    if user_self_id is None:
        return None
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        updates = await get_updates_via_bot_api(timeout_seconds=2)
        for update in updates:
            message = update.get("message") or {}
            chat = message.get("chat") or {}
            if chat.get("id") != user_self_id:
                continue
            document = message.get("document") or {}
            if document.get("file_name") == filename:
                return message.get("message_id")
            if document.get("file_size") == file_size:
                return message.get("message_id")
            photo = message.get("photo") or []
            for size in photo:
                if size.get("file_size") == file_size:
                    return message.get("message_id")
            caption = message.get("caption") or ""
            if file_hash in caption or filename in caption:
                return message.get("message_id")
        await asyncio.sleep(1)
    return None


def extract_bale_file_id(message: dict) -> str | None:
    document = message.get("document") or {}
    if document.get("file_id"):
        return document.get("file_id")
    photo = message.get("photo") or []
    if photo:
        return photo[-1].get("file_id")
    return None


def parse_bale_file_ids(value: str | None) -> list[str]:
    if not value:
        return []
    if value.startswith("["):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return [value]
        return [item for item in parsed if isinstance(item, str)]
    return [value]


def is_telegram_photo(message: events.NewMessage.Event | None) -> bool:
    if not message or not message.message:
        return False
    if message.message.photo:
        return True
    if message.message.document:
        return False
    return False


def is_telegram_document(message: events.NewMessage.Event | None) -> bool:
    if not message or not message.message:
        return False
    return bool(message.message.document)


def is_telegram_uploaded_file(message: events.NewMessage.Event | None) -> bool:
    if not message or not message.message:
        return False
    return bool(message.message.photo or message.message.document)


async def download_telegram_media(
    bot_client: TelegramClient, chat_id: int, message_id: int, dest_dir: Path
) -> Path | None:
    message = await bot_client.get_messages(chat_id, ids=message_id)
    if not message or not message.media:
        return None
    dest_dir.mkdir(parents=True, exist_ok=True)
    downloaded = await bot_client.download_media(message, file=str(dest_dir))
    if not downloaded:
        return None
    return Path(str(downloaded))


async def download_bale_media(
    api: BaleApi, file_id: str, dest_dir: Path
) -> Path | None:
    info = await api.get_file(file_id)
    file_path = info.get("file_path")
    if not file_path:
        return None
    url = api.file_url(file_path)
    dest_dir.mkdir(parents=True, exist_ok=True)
    filename = Path(file_path).name
    target_path = dest_dir / filename

    def _download() -> None:
        with requests.get(url, stream=True, timeout=(10, 120)) as resp:
            resp.raise_for_status()
            with target_path.open("wb") as handle:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    handle.write(chunk)

    await asyncio.to_thread(_download)
    return target_path


async def relay_telegram_cached(
    chat_id: int, from_chat_id: int, message_id: int, caption: str | None
) -> None:
    await copy_message_via_bot_api(chat_id, from_chat_id, message_id, caption)


async def upload_path_to_telegram(
    bot_client: TelegramClient,
    user_client: TelegramClient,
    chat_id: int,
    file_path: Path,
    caption: str | None,
) -> None:
    if user_self_id is None or not bot_upload_target:
        raise RuntimeError("Bot relay is not configured.")
    file_hash = compute_sha256(file_path)
    cached_message_id = await db_get_message_id(file_hash)
    if caption is None:
        caption = make_hash_caption(file_path.name, file_hash)
    if cached_message_id is not None:
        await relay_telegram_cached(chat_id, user_self_id, cached_message_id, caption)
        return
    upload_message = await user_client.send_file(
        bot_upload_target,
        file_path,
        caption=caption or None,
        parse_mode=None,
    )
    upload_message_id = (
        upload_message[0].id if isinstance(upload_message, list) else upload_message.id
    )
    file_size = file_path.stat().st_size
    bot_message_id = await find_bot_message_id(file_path.name, file_hash, file_size)
    if bot_message_id is None:
        raise RuntimeError("Bot could not see uploaded media.")
    await db_set_message_id(file_hash, bot_message_id)
    await relay_telegram_cached(chat_id, user_self_id, bot_message_id, caption)


async def upload_path_to_bale(
    api: BaleApi,
    chat_id: int,
    file_path: Path,
    original_hash: str,
) -> list[str]:
    upload_items: list[tuple[Path, str]] = []
    if should_zip(file_path):
        zip_path = file_path.with_suffix(".zip")
        await asyncio.to_thread(create_zip, file_path, zip_path)
        zip_size = zip_path.stat().st_size
        if zip_size > BALE_ZIP_PART_BYTES:
            parts = await asyncio.to_thread(split_file, zip_path, BALE_ZIP_PART_BYTES)
            upload_items.extend(
                (part, f"{zip_path.name} (part {idx}/{len(parts)})")
                for idx, part in enumerate(parts, start=1)
            )
        else:
            upload_items.append((zip_path, zip_path.name))
    else:
        upload_items.append((file_path, file_path.name))

    file_ids: list[str] = []
    for item_path, display_name in upload_items:
        item_hash = await asyncio.to_thread(compute_sha256, item_path)
        caption = f"Uploaded: {display_name}\nHash: {original_hash}"
        cached_file_id = await db_get_bale_file_id(item_hash)
        if cached_file_id:
            await api.send_document(chat_id, caption, file_id=cached_file_id)
            file_ids.append(cached_file_id)
            continue
        upload_filename = item_path.name
        message = None
        for attempt in range(3):
            try:
                message = await api.send_document(
                    chat_id,
                    caption,
                    file_path=item_path,
                    filename=upload_filename,
                )
                break
            except RuntimeError as exc:
                error_text = str(exc).lower()
                retryable = any(
                    token in error_text
                    for token in (
                        "failed to upload file bytes",
                        "504",
                        "timed out",
                        "timeout",
                        "connection aborted",
                        "request failed",
                    )
                )
                if not retryable:
                    raise
                if attempt == 2:
                    raise
                upload_filename = uuid.uuid4().hex
                await asyncio.sleep(2 * (attempt + 1))
        file_id = extract_bale_file_id(message or {})
        if file_id:
            await db_set_bale_file_id(item_hash, file_id)
            file_ids.append(file_id)
    return file_ids


async def process_job(
    bot_client: TelegramClient, user_client: TelegramClient, job: Job
) -> None:
    editor = ThrottledEditor(bot_client, job.chat_id, job.status_message_id)
    job_dir = DOWNLOAD_DIR / uuid.uuid4().hex
    job_dir.mkdir(parents=True, exist_ok=True)
    active_job_dirs.add(job_dir.resolve())
    output_path = job_dir / "download.tmp"
    user = UserRef("telegram", job.user_id, job.chat_id, job.username)
    await editor.update("Starting download...", force=True)
    try:
        try:
            safe_url = await resolve_safe_download_url(job.url)
        except ValueError as exc:
            logger.warning("Rejected unsafe Telegram URL from user=%s: %s", job.user_id, exc)
            await editor.update("This URL is not allowed.", force=True)
            return
        async with global_semaphore:
            async with per_user_semaphore[job.user_id]:
                link_block = await mod_find_block_for_link(safe_url)
                if link_block:
                    await mod_create_quarantine(
                        user,
                        safe_url,
                        None,
                        None,
                        None,
                        link_block.get("reason") or "blocked link",
                    )
                    await editor.update(blocked_message(), force=True)
                    return
                if not bot_upload_target:
                    logger.error("Bot relay is not configured.")
                    await editor.update("Download failed.", force=True)
                    return
                header_name, content_type, content_length = await probe_response_meta(
                    safe_url
                )
                if content_length is not None and content_length > MAX_UPLOAD_BYTES:
                    await editor.update(
                        "I cannot upload files bigger than 2GB :(",
                        force=True,
                    )
                    return
                rc = await download_with_progress(safe_url, output_path, editor)
                if rc == SIZE_LIMIT_EXCEEDED:
                    await editor.update(
                        "I cannot upload files bigger than 2GB :(",
                        force=True,
                    )
                    return
                if rc != 0:
                    await editor.update("Download failed.", force=True)
                    return
                target_name = choose_download_filename(
                    safe_url, header_name, content_type,
                    original_url=job.url,
                )
                target_name = improve_filename_from_file(target_name, output_path)
                target_name = shorten_filename(target_name)
                target_path = job_dir / target_name
                if target_path.exists():
                    stem = target_path.stem or "download"
                    suffix = target_path.suffix
                    target_path = job_dir / f"{stem}-{uuid.uuid4().hex}{suffix}"
                output_path.rename(target_path)
                try:
                    file_hash = compute_sha256(target_path)
                except Exception as exc:
                    logger.exception("Hashing failed: %s", exc)
                    await editor.update("Download failed.", force=True)
                    return
                file_size = target_path.stat().st_size
                hash_block = await mod_find_block_for_hash(file_hash)
                status = "quarantined" if hash_block else "clean"
                await mod_log_abuse(
                    user,
                    safe_url,
                    file_hash,
                    target_path.name,
                    file_size,
                    content_type,
                    status,
                )
                if hash_block:
                    await mod_create_quarantine(
                        user,
                        safe_url,
                        file_hash,
                        target_path.name,
                        file_size,
                        hash_block.get("reason") or "blocked hash",
                    )
                    await editor.update(blocked_message(), force=True)
                    return
                cached_message_id = await db_get_message_id(file_hash)
                upload_caption = make_hash_caption(target_path.name, file_hash)
                if cached_message_id is not None:
                    try:
                        await relay_via_bot(
                            bot_client, job, cached_message_id, upload_caption
                        )
                        await editor.update("Done <3", force=True)
                        return
                    except Exception as exc:
                        logger.exception("Cached relay failed: %s", exc)
                        await db_delete_hash(file_hash)
                await editor.update("Uploading...", force=True)
                upload_message = await upload_with_progress(
                    user_client, bot_upload_target, target_path, editor, upload_caption
                )
                upload_message_id = (
                    upload_message[0].id
                    if isinstance(upload_message, list)
                    else upload_message.id
                )
                bot_message_id = await find_bot_message_id(
                    target_path.name, file_hash, file_size
                )
                if bot_message_id is None:
                    logger.error(
                        "Bot could not see uploaded media filename=%s",
                        target_path.name,
                    )
                    await editor.update("Download failed.", force=True)
                    return
                await db_set_message_id(file_hash, bot_message_id)
                await relay_via_bot(bot_client, job, bot_message_id, upload_caption)
                await editor.update("Done <3", force=True)
    except Exception as exc:
        logger.exception("Job failed: %s", exc)
        await editor.update("Download failed.", force=True)
    finally:
        cleanup_temp_dir(job_dir)
        pending_by_user[job.user_id] = max(0, pending_by_user[job.user_id] - 1)
        pending_links_by_user[job.user_id].discard(job.url)


async def worker(bot_client: TelegramClient, user_client: TelegramClient) -> None:
    while True:
        job = await queue.get()
        try:
            await process_job(bot_client, user_client, job)
        finally:
            queue.task_done()


async def process_bale_link(
    api: BaleApi,
    chat_id: int,
    user_id: int,
    url: str,
    status_message_id: int,
    username: str | None = None,
) -> None:
    editor = BaleThrottledEditor(api, chat_id, status_message_id)
    job_dir = DOWNLOAD_DIR / uuid.uuid4().hex
    job_dir.mkdir(parents=True, exist_ok=True)
    active_job_dirs.add(job_dir.resolve())
    output_path = job_dir / "download.tmp"
    user = UserRef("bale", user_id, chat_id, username)
    await editor.update("Starting download...", force=True)
    try:
        try:
            safe_url = await resolve_safe_download_url(url)
        except ValueError as exc:
            logger.warning("Rejected unsafe Bale URL from user=%s: %s", user_id, exc)
            await editor.update("This URL is not allowed.", force=True)
            return
        async with bale_global_semaphore:
            async with bale_per_user_semaphore[user_id]:
                link_block = await mod_find_block_for_link(safe_url)
                if link_block:
                    await mod_create_quarantine(
                        user,
                        safe_url,
                        None,
                        None,
                        None,
                        link_block.get("reason") or "blocked link",
                    )
                    await editor.update(blocked_message(), force=True)
                    return
                header_name, content_type, content_length = await probe_response_meta(
                    safe_url
                )
                if content_length is not None and content_length == 0:
                    await editor.update("Download failed.", force=True)
                    return
                rc = await download_with_progress(safe_url, output_path, editor)
                if rc != 0:
                    await editor.update("Download failed.", force=True)
                    return
                target_name = choose_download_filename(
                    safe_url, header_name, content_type,
                    original_url=url,
                )
                target_name = improve_filename_from_file(target_name, output_path)
                target_name = shorten_filename(target_name)
                target_path = job_dir / target_name
                if target_path.exists():
                    stem = target_path.stem or "download"
                    suffix = target_path.suffix
                    target_path = job_dir / f"{stem}-{uuid.uuid4().hex}{suffix}"
                output_path.rename(target_path)
                if target_path.stat().st_size == 0:
                    await editor.update("Download failed.", force=True)
                    return
                original_hash = await asyncio.to_thread(compute_sha256, target_path)
                file_size = target_path.stat().st_size
                hash_block = await mod_find_block_for_hash(original_hash)
                status = "quarantined" if hash_block else "clean"
                await mod_log_abuse(
                    user,
                    safe_url,
                    original_hash,
                    target_path.name,
                    file_size,
                    content_type,
                    status,
                )
                if hash_block:
                    await mod_create_quarantine(
                        user,
                        safe_url,
                        original_hash,
                        target_path.name,
                        file_size,
                        hash_block.get("reason") or "blocked hash",
                    )
                    await editor.update(blocked_message(), force=True)
                    return
                await editor.update("Uploading...", force=True)
                await upload_path_to_bale(api, chat_id, target_path, original_hash)
                await editor.update("Done <3", force=True)
    except Exception as exc:
        logger.exception("Bale link job failed: %s", exc)
        await editor.update("Download failed.", force=True)
    finally:
        cleanup_temp_dir(job_dir)
        bale_pending_by_user[user_id] = max(0, bale_pending_by_user[user_id] - 1)
        bale_pending_links_by_user[user_id].discard(url)


async def handle_telegram_key_store(
    event: events.NewMessage.Event, key: str
) -> None:
    if event.sender_id is None:
        await event.reply("Could not resolve your user id.")
        return
    if await db_count_keys_for_owner("telegram", event.sender_id) >= MAX_KEYS_PER_USER:
        await event.reply(f"You already have the maximum {MAX_KEYS_PER_USER} keys.")
        return
    message = event.message
    filename = None
    if message and message.file:
        filename = message.file.name
    success = await db_insert_key_entry(
        key,
        "telegram",
        tg_chat_id=event.chat_id,
        tg_message_id=message.id if message else None,
        filename=filename,
        owner_platform="telegram",
        owner_user_id=event.sender_id,
    )
    if not success:
        await event.reply("Key already exists.")
        return
    await event.reply("Key saved.")


async def handle_telegram_key_reply_store(
    event: events.NewMessage.Event, key: str
) -> None:
    if event.sender_id is None:
        await event.reply("Could not resolve your user id.")
        return
    if await db_count_keys_for_owner("telegram", event.sender_id) >= MAX_KEYS_PER_USER:
        await event.reply(f"You already have the maximum {MAX_KEYS_PER_USER} keys.")
        return
    reply = await event.get_reply_message()
    if not reply or not reply.media:
        await event.reply("Reply to a file to save a key.")
        return
    filename = reply.file.name if reply.file else None
    success = await db_insert_key_entry(
        key,
        "telegram",
        tg_chat_id=reply.chat_id,
        tg_message_id=reply.id,
        filename=filename,
        owner_platform="telegram",
        owner_user_id=event.sender_id,
    )
    if not success:
        await event.reply("Key already exists.")
        return
    await event.reply("Key saved.")


async def handle_telegram_key_request(
    bot_client: TelegramClient,
    user_client: TelegramClient,
    event: events.NewMessage.Event,
    key: str,
) -> None:
    status = await event.reply("Fetching file for this key...")
    entry = await db_get_key_entry(key)
    if not entry:
        await status.edit("Key not found.")
        return
    if entry.get("source") == "telegram":
        tg_chat_id = entry.get("tg_chat_id")
        tg_message_id = entry.get("tg_message_id")
        if tg_chat_id is None or tg_message_id is None:
            await status.edit("Key is missing Telegram metadata.")
            return
        file_hash = entry.get("file_hash")
        caption = (
            make_hash_caption(entry.get("filename"), file_hash)
            if file_hash
            else None
        )
        await relay_telegram_cached(event.chat_id, tg_chat_id, tg_message_id, caption)
        await status.edit("Done <3")
        return
    if bale_api is None:
        await status.edit("Bale bot is not configured.")
        return
    bale_ids = parse_bale_file_ids(entry.get("bale_file_id"))
    if not bale_ids:
        await status.edit("Key is missing Bale metadata.")
        return
    temp_dir = make_temp_dir("bale-to-telegram")
    try:
        file_path = await download_bale_media(bale_api, bale_ids[0], temp_dir)
        if not file_path:
            await status.edit("Download failed.")
            return
        file_hash = compute_sha256(file_path)
        await db_update_key_entry(key, file_hash=file_hash)
        await upload_path_to_telegram(bot_client, user_client, event.chat_id, file_path, None)
        await status.edit("Done <3")
    finally:
        cleanup_temp_dir(temp_dir)


async def handle_telegram_hash_request(
    bot_client: TelegramClient,
    user_client: TelegramClient,
    event: events.NewMessage.Event,
    file_hash: str,
) -> None:
    status = await event.reply("Fetching file for this hash...")
    if await mod_find_block_for_hash(file_hash):
        await status.edit(blocked_message())
        return
    cached_message_id = await db_get_message_id(file_hash)
    if cached_message_id is not None and user_self_id is not None:
        await relay_telegram_cached(
            event.chat_id,
            user_self_id,
            cached_message_id,
            make_hash_caption(None, file_hash),
        )
        await status.edit("Done.")
        return
    if bale_api is not None:
        bale_file_id = await db_get_bale_file_id(file_hash)
        if bale_file_id:
            temp_dir = make_temp_dir("bale-to-telegram-hash")
            try:
                file_path = await download_bale_media(bale_api, bale_file_id, temp_dir)
                if not file_path:
                    await status.edit("Download failed.")
                    return
                await upload_path_to_telegram(
                    bot_client, user_client, event.chat_id, file_path, None
                )
                await status.edit("Done.")
                return
            finally:
                cleanup_temp_dir(temp_dir)
    entry = await db_get_key_entry_by_hash(file_hash)
    if entry:
        tg_chat_id = entry.get("tg_chat_id")
        tg_message_id = entry.get("tg_message_id")
        if tg_chat_id and tg_message_id and user_self_id is not None:
            await relay_telegram_cached(
                event.chat_id,
                tg_chat_id,
                tg_message_id,
                make_hash_caption(None, file_hash),
            )
            await status.edit("Done.")
            return
        bale_ids = parse_bale_file_ids(entry.get("bale_file_id"))
        if bale_api is not None and bale_ids:
            temp_dir = make_temp_dir("bale-to-telegram-hash")
            try:
                file_path = await download_bale_media(bale_api, bale_ids[0], temp_dir)
                if not file_path:
                    await status.edit("Download failed.")
                    return
                await upload_path_to_telegram(
                    bot_client, user_client, event.chat_id, file_path, None
                )
                await status.edit("Done.")
                return
            finally:
                cleanup_temp_dir(temp_dir)
    await status.edit("Hash not found.")


async def handle_bale_key_store(
    api: BaleApi, chat_id: int, message: dict, user_id: int
) -> None:
    caption = (message.get("caption") or "").strip()
    key = extract_key(caption)
    if not key:
        return
    if await db_count_keys_for_owner("bale", user_id) >= MAX_KEYS_PER_USER:
        await api.send_message(chat_id, f"You already have the maximum {MAX_KEYS_PER_USER} keys.")
        return
    file_id = extract_bale_file_id(message)
    if not file_id:
        await api.send_message(chat_id, "Key requires a file.")
        return
    filename = None
    document = message.get("document") or {}
    if document:
        filename = document.get("file_name")
    success = await db_insert_key_entry(
        key,
        "bale",
        bale_file_id=file_id,
        filename=filename,
        owner_platform="bale",
        owner_user_id=user_id,
    )
    if not success:
        await api.send_message(chat_id, "Key already exists.")
        return
    await api.send_message(chat_id, "Key saved.")


async def handle_bale_key_reply_store(
    api: BaleApi, chat_id: int, message: dict, key: str
) -> None:
    sender = message.get("from") or {}
    user_id = sender.get("id", chat_id)
    if await db_count_keys_for_owner("bale", user_id) >= MAX_KEYS_PER_USER:
        await api.send_message(chat_id, f"You already have the maximum {MAX_KEYS_PER_USER} keys.")
        return
    reply = message.get("reply_to_message") or {}
    file_id = extract_bale_file_id(reply)
    if not file_id:
        await api.send_message(chat_id, "Reply to a file to save a key.")
        return
    filename = None
    document = reply.get("document") or {}
    if document:
        filename = document.get("file_name")
    success = await db_insert_key_entry(
        key,
        "bale",
        bale_file_id=file_id,
        filename=filename,
        owner_platform="bale",
        owner_user_id=user_id,
    )
    if not success:
        await api.send_message(chat_id, "Key already exists.")
        return
    await api.send_message(chat_id, "Key saved.")


async def handle_bale_key_request(
    api: BaleApi,
    bot_client: TelegramClient,
    chat_id: int,
    key: str,
) -> None:
    status = await api.send_message(chat_id, "Fetching file for this key...")
    entry = await db_get_key_entry(key)
    if not entry:
        await api.edit_message_text(chat_id, status.get("message_id"), "Key not found.")
        return
    cached_bale_ids = parse_bale_file_ids(entry.get("bale_file_id"))
    if cached_bale_ids:
        try:
            for file_id in cached_bale_ids:
                await api.send_document(chat_id, None, file_id=file_id)
            await api.edit_message_text(chat_id, status.get("message_id"), "Done <3")
            return
        except Exception as exc:
            logger.exception("Bale cached send failed: %s", exc)
            await db_update_key_entry(key, bale_file_id="")
    if entry.get("source") == "bale":
        bale_ids = parse_bale_file_ids(entry.get("bale_file_id"))
        if not bale_ids:
            await api.edit_message_text(
                chat_id, status.get("message_id"), "Key is missing Bale metadata."
            )
            return
        try:
            for file_id in bale_ids:
                await api.send_document(chat_id, None, file_id=file_id)
            await api.edit_message_text(chat_id, status.get("message_id"), "Done <3")
        except Exception as exc:
            logger.exception("Bale send failed: %s", exc)
            await api.edit_message_text(
                chat_id, status.get("message_id"), "Upload failed."
            )
        return
    tg_chat_id = entry.get("tg_chat_id")
    tg_message_id = entry.get("tg_message_id")
    if tg_chat_id is None or tg_message_id is None:
        await api.edit_message_text(
            chat_id, status.get("message_id"), "Key is missing Telegram metadata."
        )
        return
    temp_dir = make_temp_dir("telegram-to-bale")
    try:
        file_path = await download_telegram_media(
            bot_client, tg_chat_id, tg_message_id, temp_dir
        )
        if not file_path:
            await api.edit_message_text(
                chat_id, status.get("message_id"), "Download failed."
            )
            return
        original_hash = await asyncio.to_thread(compute_sha256, file_path)
        file_ids = await upload_path_to_bale(api, chat_id, file_path, original_hash)
        if file_ids:
            bale_value = json.dumps(file_ids) if len(file_ids) > 1 else file_ids[0]
            await db_update_key_entry(
                key, bale_file_id=bale_value, file_hash=original_hash
            )
        await api.edit_message_text(chat_id, status.get("message_id"), "Done <3")
    finally:
        cleanup_temp_dir(temp_dir)


async def handle_bale_hash_request(
    api: BaleApi,
    bot_client: TelegramClient,
    chat_id: int,
    file_hash: str,
) -> None:
    status = await api.send_message(chat_id, "Fetching file for this hash...")
    if await mod_find_block_for_hash(file_hash):
        await api.edit_message_text(chat_id, status.get("message_id"), blocked_message())
        return
    bale_file_id = await db_get_bale_file_id(file_hash)
    if bale_file_id:
        try:
            await api.send_document(chat_id, None, file_id=bale_file_id)
            await api.edit_message_text(chat_id, status.get("message_id"), "Done.")
            return
        except Exception as exc:
            logger.exception("Bale cached hash send failed: %s", exc)
            await db_delete_bale_hash(file_hash)
    cached_message_id = await db_get_message_id(file_hash)
    if cached_message_id is not None:
        temp_dir = make_temp_dir("telegram-to-bale-hash")
        try:
            file_path = await download_telegram_media(
                bot_client, user_self_id, cached_message_id, temp_dir
            )
            if not file_path:
                await api.edit_message_text(
                    chat_id, status.get("message_id"), "Download failed."
                )
                return
            file_ids = await upload_path_to_bale(api, chat_id, file_path, file_hash)
            if file_ids:
                await db_set_bale_file_id(file_hash, file_ids[0])
            await api.edit_message_text(chat_id, status.get("message_id"), "Done.")
            return
        finally:
            cleanup_temp_dir(temp_dir)
    entry = await db_get_key_entry_by_hash(file_hash)
    if entry:
        bale_ids = parse_bale_file_ids(entry.get("bale_file_id"))
        if bale_ids:
            try:
                for file_id in bale_ids:
                    await api.send_document(chat_id, None, file_id=file_id)
                await api.edit_message_text(
                    chat_id, status.get("message_id"), "Done."
                )
                return
            except Exception as exc:
                logger.exception("Bale hash send failed: %s", exc)
        tg_chat_id = entry.get("tg_chat_id")
        tg_message_id = entry.get("tg_message_id")
        if tg_chat_id and tg_message_id:
            temp_dir = make_temp_dir("telegram-to-bale-hash")
            try:
                file_path = await download_telegram_media(
                    bot_client, tg_chat_id, tg_message_id, temp_dir
                )
                if not file_path:
                    await api.edit_message_text(
                        chat_id, status.get("message_id"), "Download failed."
                    )
                    return
                file_ids = await upload_path_to_bale(api, chat_id, file_path, file_hash)
                if file_ids:
                    await db_set_bale_file_id(file_hash, file_ids[0])
                await api.edit_message_text(
                    chat_id, status.get("message_id"), "Done."
                )
                return
            finally:
                cleanup_temp_dir(temp_dir)
    await api.edit_message_text(chat_id, status.get("message_id"), "Hash not found.")


async def poll_bale_updates(
    api: BaleApi, bot_client: TelegramClient, user_client: TelegramClient
) -> None:
    global bale_api_offset
    while True:
        try:
            updates = await api.get_updates(bale_api_offset, timeout_seconds=5)
        except Exception as exc:
            logger.exception("Bale getUpdates failed: %s", exc)
            await asyncio.sleep(2)
            continue
        for update in updates:
            bale_api_offset = max(
                bale_api_offset, update.get("update_id", 0) + 1
            )
            try:
                message = update.get("message") or {}
                chat = message.get("chat") or {}
                chat_id = chat.get("id")
                if chat_id is None:
                    continue
                sender = message.get("from") or {}
                if sender.get("is_bot"):
                    continue
                sender_id = sender.get("id", chat_id)
                username = sender.get("username") or sender.get("user_name")
                text = (message.get("text") or "").strip()
                caption = (message.get("caption") or "").strip()
                bale_user = UserRef("bale", sender_id, chat_id, username)
                await mod_register_user(bale_user)
                if text.lower().startswith("/admin"):
                    if not is_bale_admin(sender_id):
                        await api.send_message(chat_id, "Admin access denied.")
                        continue
                    try:
                        await api.send_message(
                            chat_id,
                            await handle_admin_command(text, "bale", sender_id),
                        )
                    except Exception as exc:
                        logger.exception("Bale admin command failed: %s", exc)
                        await api.send_message(chat_id, f"Admin command failed: {exc}")
                    continue
                appeal_cmd, appeal_arg = parse_appeal_command(text)
                if appeal_cmd:
                    if appeal_cmd == "status":
                        appeal = await mod_get_appeal(bale_user)
                        await api.send_message(
                            chat_id,
                            json.dumps(appeal, ensure_ascii=False, indent=2)
                            if appeal
                            else "No appeal found.",
                        )
                        continue
                    if appeal_cmd == "create" and appeal_arg:
                        appeal_id = await mod_create_or_update_appeal(
                            bale_user, appeal_arg
                        )
                        await api.send_message(chat_id, f"Appeal submitted: {appeal_id}")
                        continue
                    await api.send_message(chat_id, "Usage: /appeal <message> | /appeal status")
                    continue
                ban = await mod_is_banned("bale", sender_id)
                if ban:
                    await api.send_message(chat_id, banned_message(ban))
                    continue
                lowered = text.lower()
                if lowered == "/keys":
                    await api.send_message(
                        chat_id,
                        await handle_user_keys_command("bale", sender_id, "list", None),
                    )
                    continue
                if lowered.startswith("/keydel "):
                    await api.send_message(
                        chat_id,
                        await handle_user_keys_command(
                            "bale", sender_id, "delete", text.split(maxsplit=1)[1]
                        ),
                    )
                    continue
                if lowered.startswith("/keyinfo "):
                    await api.send_message(
                        chat_id,
                        await handle_user_keys_command(
                            "bale", sender_id, "info", text.split(maxsplit=1)[1]
                        ),
                    )
                    continue
                auto_cmd, auto_arg = parse_auto_command(text)
                if auto_cmd:
                    if auto_cmd == "enable":
                        await handle_auto_enable_bale(api, chat_id, sender_id)
                        continue
                    if auto_cmd == "disable":
                        await handle_auto_disable_bale(api, chat_id, sender_id)
                        continue
                    if auto_cmd == "set":
                        await handle_auto_set_bale(
                            api, bot_client, chat_id, sender_id, auto_arg, sender
                        )
                        continue
                    await api.send_message(
                        chat_id, "Usage: /auto enable | /auto disable | /auto set <UID>"
                    )
                    continue
                message_has_file = extract_bale_file_id(message) is not None
                handled_key = False
                if caption and extract_key(caption):
                    await handle_bale_key_store(api, chat_id, message, sender_id)
                    handled_key = True
                if message_has_file:
                    link = await db_get_auto_link_by_bale(sender_id)
                    if link:
                        spawn_bale_task(
                            forward_bale_file_to_telegram(
                                api, bot_client, user_client, sender_id, message
                            ),
                            "auto-bale-to-telegram",
                        )
                        continue
                    if handled_key:
                        continue
                if text:
                    key_command = extract_key_command(text)
                    if key_command:
                        await handle_bale_key_reply_store(
                            api, chat_id, message, key_command
                        )
                        continue
                    hash_query = extract_hash_query(text)
                    if hash_query:
                        spawn_bale_task(
                            handle_bale_hash_request(
                                api, bot_client, chat_id, hash_query
                            ),
                            "bale-hash-request",
                        )
                        continue
                    key = extract_key(text)
                    if key:
                        spawn_bale_task(
                            handle_bale_key_request(api, bot_client, chat_id, key),
                            "bale-key-request",
                        )
                        continue
                    url = extract_url(text)
                    if url:
                        try:
                            await asyncio.to_thread(validate_public_http_url, url)
                        except ValueError:
                            await api.send_message(chat_id, "This URL is not allowed.")
                            continue
                        link_block = await mod_find_block_for_link(url)
                        if link_block:
                            await mod_create_quarantine(
                                bale_user,
                                url,
                                None,
                                None,
                                None,
                                link_block.get("reason") or "blocked link",
                            )
                            await api.send_message(chat_id, blocked_message())
                            continue
                        if url in bale_pending_links_by_user[sender_id]:
                            await api.send_message(
                                chat_id, "I'm still trying to upload this one :("
                            )
                            continue
                        if bale_pending_by_user[sender_id] >= MAX_PENDING_PER_USER:
                            await api.send_message(
                                chat_id, "You already have 3 pending downloads."
                            )
                            continue
                        bale_pending_by_user[sender_id] += 1
                        bale_pending_links_by_user[sender_id].add(url)
                        status_message = await api.send_message(
                            chat_id, "Queued."
                        )
                        spawn_bale_task(
                            process_bale_link(
                                api,
                                chat_id,
                                sender_id,
                                url,
                                status_message.get("message_id"),
                                username,
                            ),
                            "bale-link",
                        )
                        continue
                    if text.lower() in {"ping", "/ping"}:
                        await api.send_message(chat_id, "Pong!")
                        continue
                    await api.send_message(
                        chat_id,
                        "send me a link, a (key:keystring) message, or a file with a caption like (key:yourkey) or forward any file and reply to it with /key <your key> or a file hash (hash: hashfrombefore) :3",
                    )
                    continue
            except Exception as exc:
                logger.exception("Bale update handling failed: %s", exc)
                continue


async def handle_user_keys_command(
    platform: str, user_id: int, action: str, arg: str | None
) -> str:
    if action == "list":
        rows = await db_list_keys_for_owner(platform, user_id)
        return format_key_rows(rows)
    if action == "delete" and arg:
        deleted = await db_delete_key_for_owner(arg, platform, user_id)
        return "Key deleted." if deleted else "Key not found."
    if action == "info" and arg:
        entry = await db_get_key_entry(arg)
        if not entry or entry.get("owner_platform") != platform or entry.get("owner_user_id") != user_id:
            return "Key not found."
        return (
            f"Key: {entry['key']}\n"
            f"Source: {entry['source']}\n"
            f"Filename: {entry.get('filename') or '-'}\n"
            f"Hash: {entry.get('file_hash') or '-'}"
        )
    return "Usage: /keys | /keydel <key> | /keyinfo <key>"


async def handle_admin_command(
    text: str,
    admin_platform: str,
    admin_user_id: int,
) -> str:
    parts = text.split()
    if len(parts) < 2 or parts[0].lower() != "/admin":
        return ""
    cmd = parts[1].lower()
    if cmd == "ban" and len(parts) >= 4:
        platform = parts[2].lower()
        user_id = int(parts[3])
        reason = " ".join(parts[4:]) or "admin ban"
        await mod_ban_user(platform, user_id, reason, admin_platform, admin_user_id)
        return f"Banned {platform}:{user_id}."
    if cmd == "unban" and len(parts) >= 4:
        platform = parts[2].lower()
        user_id = int(parts[3])
        await mod_unban_user(platform, user_id)
        return f"Unbanned {platform}:{user_id}."
    if cmd == "baninfo" and len(parts) >= 4:
        ban = await mod_is_banned(parts[2].lower(), int(parts[3]))
        return json.dumps(ban, ensure_ascii=False, indent=2) if ban else "Not banned."
    if cmd == "search" and len(parts) >= 4:
        kind = parts[2].lower()
        if kind == "user" and len(parts) >= 5:
            rows = await mod_search_logs("user", f"{parts[3].lower()}:{parts[4]}")
        else:
            rows = await mod_search_logs(kind, " ".join(parts[3:]))
        return format_log_rows(rows)
    if cmd == "block" and len(parts) >= 4:
        kind = parts[2].lower()
        value = parts[3]
        reason = " ".join(parts[4:]) or "admin block"
        if kind == "hash":
            kind = "hash"
        if kind not in {"hash", "link", "domain"}:
            return "Usage: /admin block <hash|link|domain> <value> [reason]"
        await mod_add_block(kind, value, reason, admin_name(admin_platform, admin_user_id))
        normalized = normalize_block_value(kind, value)
        return f"Blocked {kind}: {normalized}"
    if cmd == "unblock" and len(parts) >= 4:
        kind = parts[2].lower()
        if kind not in {"hash", "link", "domain"}:
            return "Usage: /admin unblock <hash|link|domain> <value>"
        normalized = normalize_block_value(kind, parts[3])
        await mod_remove_block(kind, parts[3])
        return f"Unblocked {kind}: {normalized}"
    if cmd in {"blocks", "blocklist"}:
        kind = parts[2].lower() if len(parts) >= 3 else None
        if kind is not None and kind not in {"hash", "link", "domain"}:
            return "Usage: /admin blocks [hash|link|domain]"
        rows = await mod_list_blocks(kind, limit=50)
        return format_block_rows(rows)
    if cmd == "quarantine":
        if len(parts) >= 4 and parts[2].lower() in {"approve", "reject"}:
            status = "approved" if parts[2].lower() == "approve" else "rejected"
            await mod_update_quarantine(int(parts[3]), status, " ".join(parts[4:]) or None)
            return f"Quarantine item {parts[3]} {status}."
        rows = await mod_list_quarantine()
        if not rows:
            return "No quarantine items."
        return "\n".join(
            f"#{row['id']} {row['platform']}:{row['user_id']} {row['status']} {row.get('file_hash') or row.get('link') or '-'} {row.get('reason') or ''}"
            for row in rows
        )
    if cmd == "broadcast" and len(parts) >= 4:
        target = parts[2].lower()
        if target not in {"telegram", "bale", "both"}:
            return "Usage: /admin broadcast <telegram|bale|both> <message>"
        job_id = await mod_create_broadcast(target, " ".join(parts[3:]), admin_name(admin_platform, admin_user_id))
        return f"Broadcast queued: {job_id}"
    if cmd == "appeals":
        if len(parts) >= 4 and parts[2].lower() in {"accept", "reject"}:
            status = "accepted" if parts[2].lower() == "accept" else "rejected"
            resolved = await mod_resolve_appeal(parts[3], status, " ".join(parts[4:]) or None)
            return f"Appeal {parts[3]} {status}." if resolved else "Appeal not found."
        rows = await mod_list_appeals()
        return "\n".join(
            f"{row['id']} {row['status']} {row['platform']}:{row['user_id']} {row.get('message') or ''}"
            for row in rows
        ) or "No appeals."
    if cmd == "keys" and len(parts) >= 4:
        rows = await db_list_keys_for_owner(parts[2].lower(), int(parts[3]))
        return format_key_rows(rows)
    if cmd == "keydel" and len(parts) >= 3:
        deleted = await db_delete_key_for_owner(parts[2], "telegram", admin_user_id, admin=True)
        return "Key deleted." if deleted else "Key not found."
    if cmd == "banhash" and len(parts) >= 3:
        rows = await mod_search_logs("hash", parts[2], limit=200)
        reason = " ".join(parts[3:]) or f"abusive hash {parts[2]}"
        seen: set[tuple[str, int]] = set()
        for row in rows:
            seen.add((row["platform"], int(row["user_id"])))
        for platform, user_id in seen:
            await mod_ban_user(platform, user_id, reason, admin_platform, admin_user_id)
        return f"Banned {len(seen)} users for hash {parts[2]}."
    return (
        "Admin commands: ban, unban, baninfo, search, block, unblock, blocks, "
        "quarantine, broadcast, appeals, keys, keydel, banhash."
    )


async def handle_auto_enable_telegram(event: events.NewMessage.Event) -> None:
    if event.sender_id is None:
        await event.reply("Could not resolve your user id.")
        return
    existing = await db_get_auto_link_by_tg(event.sender_id)
    if existing:
        await event.reply(
            f"Auto forwarding is already enabled. UID: {existing['uid']}"
        )
        return
    pending = await db_get_auto_pending_by_tg(event.sender_id)
    if pending:
        await event.reply(
            f"Auto link is pending. UID: {pending['uid']}\n"
            "Use /auto set <UID> on the other side."
        )
        return
    uid = generate_auto_uid()
    await db_set_auto_pending(uid, "telegram", tg_user_id=event.sender_id)
    await mod_register_user(UserRef("telegram", event.sender_id, event.chat_id), auto_enabled=True)
    await event.reply(
        f"Auto link UID: {uid}\nUse /auto set {uid} on the other side."
    )


async def handle_auto_enable_bale(
    api: BaleApi, chat_id: int, bale_user_id: int
) -> None:
    existing = await db_get_auto_link_by_bale(bale_user_id)
    if existing:
        await api.send_message(
            chat_id, f"Auto forwarding is already enabled. UID: {existing['uid']}"
        )
        return
    pending = await db_get_auto_pending_by_bale(bale_user_id)
    if pending:
        await api.send_message(
            chat_id,
            f"Auto link is pending. UID: {pending['uid']}\n"
            "Use /auto set <UID> on the other side.",
        )
        return
    uid = generate_auto_uid()
    await db_set_auto_pending(uid, "bale", bale_user_id=bale_user_id)
    await mod_register_user(UserRef("bale", bale_user_id, chat_id), auto_enabled=True)
    await api.send_message(
        chat_id, f"Auto link UID: {uid}\nUse /auto set {uid} on the other side."
    )


async def handle_auto_set_telegram(
    bot_client: TelegramClient,
    event: events.NewMessage.Event,
    uid: str | None,
) -> None:
    if not uid:
        await event.reply("Usage: /auto set <UID>")
        return
    if event.sender_id is None:
        await event.reply("Could not resolve your user id.")
        return
    existing = await db_get_auto_link_by_tg(event.sender_id)
    if existing:
        await event.reply(
            f"Auto forwarding is already enabled. UID: {existing['uid']}"
        )
        return
    linked_uid = await db_get_auto_link_by_uid(uid)
    if linked_uid:
        await event.reply("This UID is already linked.")
        return
    pending = await db_get_auto_pending_by_uid(uid)
    if not pending:
        await event.reply("UID not found or expired.")
        return
    if pending.get("tg_user_id"):
        await event.reply("This UID must be set from the other side.")
        return
    bale_user_id = pending.get("bale_user_id")
    if bale_user_id is None:
        await event.reply("UID not found or incomplete.")
        return
    existing_bale = await db_get_auto_link_by_bale(bale_user_id)
    if existing_bale:
        await event.reply("This Bale user already has an active auto link.")
        return
    await db_set_auto_link(uid, event.sender_id, bale_user_id)
    await db_delete_auto_pending_by_uid(uid)
    await mod_register_user(UserRef("telegram", event.sender_id, event.chat_id), auto_enabled=True)
    tg_name = await format_telegram_user(bot_client, event.sender_id)
    bale_name = format_bale_user(None, bale_user_id)
    await event.reply(
        f"Auto forwarding enabled.\nTelegram user: {tg_name}\nBale user: {bale_name}"
    )


async def handle_auto_set_bale(
    api: BaleApi,
    bot_client: TelegramClient,
    chat_id: int,
    bale_user_id: int,
    uid: str | None,
    sender: dict | None,
) -> None:
    if not uid:
        await api.send_message(chat_id, "Usage: /auto set <UID>")
        return
    existing = await db_get_auto_link_by_bale(bale_user_id)
    if existing:
        await api.send_message(
            chat_id, f"Auto forwarding is already enabled. UID: {existing['uid']}"
        )
        return
    linked_uid = await db_get_auto_link_by_uid(uid)
    if linked_uid:
        await api.send_message(chat_id, "This UID is already linked.")
        return
    pending = await db_get_auto_pending_by_uid(uid)
    if not pending:
        await api.send_message(chat_id, "UID not found or expired.")
        return
    if pending.get("bale_user_id"):
        await api.send_message(chat_id, "This UID must be set from the other side.")
        return
    tg_user_id = pending.get("tg_user_id")
    if tg_user_id is None:
        await api.send_message(chat_id, "UID not found or incomplete.")
        return
    existing_tg = await db_get_auto_link_by_tg(tg_user_id)
    if existing_tg:
        await api.send_message(chat_id, "This Telegram user already has an active auto link.")
        return
    await db_set_auto_link(uid, tg_user_id, bale_user_id)
    await db_delete_auto_pending_by_uid(uid)
    await mod_register_user(UserRef("bale", bale_user_id, chat_id), auto_enabled=True)
    tg_name = await format_telegram_user(bot_client, tg_user_id)
    bale_name = format_bale_user(sender, bale_user_id)
    await api.send_message(
        chat_id,
        f"Auto forwarding enabled.\nTelegram user: {tg_name}\nBale user: {bale_name}",
    )


async def handle_auto_disable_telegram(event: events.NewMessage.Event) -> None:
    if event.sender_id is None:
        await event.reply("Could not resolve your user id.")
        return
    await db_delete_auto_link_by_tg(event.sender_id)
    await db_delete_auto_pending_by_tg(event.sender_id)
    await mod_register_user(UserRef("telegram", event.sender_id, event.chat_id), auto_enabled=False)
    await event.reply("Auto forwarding disabled.")


async def handle_auto_disable_bale(
    api: BaleApi, chat_id: int, bale_user_id: int
) -> None:
    await db_delete_auto_link_by_bale(bale_user_id)
    await db_delete_auto_pending_by_bale(bale_user_id)
    await mod_register_user(UserRef("bale", bale_user_id, chat_id), auto_enabled=False)
    await api.send_message(chat_id, "Auto forwarding disabled.")


async def forward_telegram_file_to_bale(
    bot_client: TelegramClient,
    user_client: TelegramClient,
    api: BaleApi,
    event: events.NewMessage.Event,
    bale_user_id: int,
) -> None:
    if not event.message:
        return
    sender_id = event.sender_id
    if sender_id is not None:
        tg_name = await format_telegram_user(bot_client, sender_id)
        await event.reply("Forwarding your file to Bale...")
        await api.send_message(
            bale_user_id, f"Incoming file from Telegram user {tg_name}."
        )
    temp_dir = make_temp_dir("telegram-auto")
    try:
        file_path = await download_telegram_media(
            bot_client, event.chat_id, event.message.id, temp_dir
        )
        if not file_path:
            return
        if sender_id is not None:
            file_hash = await asyncio.to_thread(compute_sha256, file_path)
            file_size = file_path.stat().st_size
            filename = event.message.file.name if event.message and event.message.file else file_path.name
            user = UserRef("telegram", sender_id, event.chat_id, None)
            hash_block = await mod_find_block_for_hash(file_hash)
            await mod_log_abuse(
                user,
                None,
                file_hash,
                filename,
                file_size,
                None,
                "quarantined" if hash_block else "clean",
            )
            if hash_block:
                await mod_create_quarantine(
                    user,
                    None,
                    file_hash,
                    filename,
                    file_size,
                    hash_block.get("reason") or "blocked hash",
                )
                await event.reply(blocked_message())
                return
        if is_telegram_photo(event):
            await api.send_photo(bale_user_id, None, file_path=file_path)
            return
        filename = None
        if event.message and event.message.file:
            filename = event.message.file.name
        await api.send_document(
            bale_user_id, None, file_path=file_path, filename=filename
        )
    finally:
        cleanup_temp_dir(temp_dir)


async def forward_bale_file_to_telegram(
    api: BaleApi,
    bot_client: TelegramClient,
    user_client: TelegramClient,
    bale_user_id: int,
    message: dict,
) -> None:
    file_id = extract_bale_file_id(message)
    if not file_id:
        return
    link = await db_get_auto_link_by_bale(bale_user_id)
    if not link:
        return
    bale_name = format_bale_user(message.get("from") or {}, bale_user_id)
    await api.send_message(bale_user_id, "Forwarding your file to Telegram...")
    tg_entity = await get_telegram_entity(bot_client, link["tg_user_id"])
    if tg_entity is None:
        return
    try:
        await bot_client.send_message(
            tg_entity, f"Incoming file from Bale user {bale_name}."
        )
    except Exception as exc:
        logger.warning("Auto forward notify failed: %s", exc)
    temp_dir = make_temp_dir("bale-auto")
    try:
        file_path = await download_bale_media(api, file_id, temp_dir)
        if not file_path:
            return
        user = UserRef("bale", bale_user_id, bale_user_id, None)
        file_hash = await asyncio.to_thread(compute_sha256, file_path)
        file_size = file_path.stat().st_size
        document = message.get("document") or {}
        filename = document.get("file_name") if document else file_path.name
        hash_block = await mod_find_block_for_hash(file_hash)
        await mod_log_abuse(
            user,
            None,
            file_hash,
            filename,
            file_size,
            None,
            "quarantined" if hash_block else "clean",
        )
        if hash_block:
            await mod_create_quarantine(
                user,
                None,
                file_hash,
                filename,
                file_size,
                hash_block.get("reason") or "blocked hash",
            )
            await api.send_message(bale_user_id, blocked_message())
            return
        document = message.get("document") or {}
        if document:
            original_name = document.get("file_name") or ""
            safe_name = ascii_filename(original_name)
            if safe_name and safe_name != file_path.name:
                desired_path = file_path.with_name(safe_name)
                try:
                    file_path.rename(desired_path)
                    file_path = desired_path
                except OSError:
                    pass
            await bot_client.send_file(tg_entity, file_path, force_document=True)
            return
        await bot_client.send_file(tg_entity, file_path)
    finally:
        cleanup_temp_dir(temp_dir)


async def broadcast_loop(bot_client: TelegramClient, api: BaleApi | None) -> None:
    while True:
        try:
            item = await mod_next_broadcast_delivery()
            if item is None:
                await asyncio.sleep(2)
                continue
            job, delivery = item
            ok = False
            error = None
            try:
                platform = delivery["platform"]
                chat_id = delivery["chat_id"] or delivery["user_id"]
                if platform == "telegram":
                    await bot_client.send_message(chat_id, job["message"])
                    await asyncio.sleep(TELEGRAM_BROADCAST_DELAY_SECONDS)
                elif platform == "bale":
                    if api is None:
                        raise RuntimeError("Bale integration is disabled")
                    await api.send_message(chat_id, job["message"])
                    await asyncio.sleep(BALE_BROADCAST_DELAY_SECONDS)
                ok = True
            except Exception as exc:
                error = str(exc)
                logger.warning("broadcast delivery failed: %s", exc)
            await mod_finish_broadcast_delivery(
                job["id"], delivery["platform"], delivery["user_id"], ok, error
            )
        except Exception as exc:
            logger.exception("broadcast loop failed: %s", exc)
            await asyncio.sleep(5)


def require_rest_auth(request: Any) -> None:
    if not ADMIN_REST_TOKEN:
        raise web.HTTPForbidden(text="ADMIN_REST_TOKEN is not configured")
    header = request.headers.get("Authorization", "")
    expected = f"Bearer {ADMIN_REST_TOKEN}"
    if not secrets.compare_digest(header, expected):
        raise web.HTTPUnauthorized(text="invalid bearer token")


def require_platform(value: str) -> str:
    if value not in {"telegram", "bale"}:
        raise web.HTTPBadRequest(text="platform must be telegram or bale")
    return value


def require_block_type(value: str) -> str:
    if value not in {"hash", "link", "domain"}:
        raise web.HTTPBadRequest(text="type must be hash, link, or domain")
    return value


def require_broadcast_target(value: str) -> str:
    if value not in {"telegram", "bale", "both"}:
        raise web.HTTPBadRequest(text="target must be telegram, bale, or both")
    return value


async def read_rest_json(request: Any) -> dict[str, Any]:
    try:
        data = await request.json()
    except Exception as exc:
        raise web.HTTPBadRequest(text="request body must be valid JSON") from exc
    if not isinstance(data, dict):
        raise web.HTTPBadRequest(text="request JSON must be an object")
    return data


async def start_rest_admin_server() -> None:
    if web is None:
        logger.warning("aiohttp is not installed; REST admin API disabled")
        return
    if not ADMIN_REST_TOKEN:
        logger.info("ADMIN_REST_TOKEN is not set; REST admin API disabled")
        return

    app = web.Application(client_max_size=REST_MAX_BODY_BYTES)

    async def health(_: Any) -> Any:
        return web.json_response({"ok": True})

    async def logs(request: Any) -> Any:
        require_rest_auth(request)
        query = request.query
        if "hash" in query:
            rows = await mod_search_logs("hash", query["hash"], limit=100)
        elif "link" in query:
            rows = await mod_search_logs("link", query["link"], limit=100)
        elif "platform" in query and "user_id" in query:
            platform = require_platform(query["platform"])
            rows = await mod_search_logs("user", f"{platform}:{query['user_id']}", limit=100)
        else:
            rows = []
        return web.json_response(rows)

    async def create_ban(request: Any) -> Any:
        require_rest_auth(request)
        data = await read_rest_json(request)
        await mod_ban_user(
            require_platform(data["platform"]),
            int(data["user_id"]),
            data.get("reason") or "REST admin ban",
            "rest",
            None,
        )
        return web.json_response({"ok": True})

    async def delete_ban(request: Any) -> Any:
        require_rest_auth(request)
        await mod_unban_user(
            require_platform(request.match_info["platform"]),
            int(request.match_info["user_id"]),
        )
        return web.json_response({"ok": True})

    async def create_block(request: Any) -> Any:
        require_rest_auth(request)
        data = await read_rest_json(request)
        await mod_add_block(
            require_block_type(data["type"]),
            data["value"],
            data.get("reason") or "REST admin block",
            "rest",
            data.get("action") or "quarantine",
        )
        return web.json_response({"ok": True})

    async def delete_block(request: Any) -> Any:
        require_rest_auth(request)
        data = await read_rest_json(request)
        await mod_remove_block(require_block_type(data["type"]), data["value"])
        return web.json_response({"ok": True})

    async def quarantine(request: Any) -> Any:
        require_rest_auth(request)
        return web.json_response(await mod_list_quarantine(limit=100))

    async def quarantine_action(request: Any) -> Any:
        require_rest_auth(request)
        action = request.match_info["action"]
        status = "approved" if action == "approve" else "rejected"
        await mod_update_quarantine(int(request.match_info["id"]), status)
        return web.json_response({"ok": True})

    async def create_broadcast(request: Any) -> Any:
        require_rest_auth(request)
        data = await read_rest_json(request)
        job_id = await mod_create_broadcast(
            require_broadcast_target(data["target"]),
            data["message"],
            "rest",
        )
        return web.json_response({"ok": True, "job_id": job_id})

    async def appeals(request: Any) -> Any:
        require_rest_auth(request)
        return web.json_response(await mod_list_appeals(limit=100))

    async def appeal_action(request: Any) -> Any:
        require_rest_auth(request)
        data = await read_rest_json(request) if request.can_read_body else {}
        action = request.match_info["action"]
        status = "accepted" if action == "accept" else "rejected"
        resolved = await mod_resolve_appeal(request.match_info["id"], status, data.get("note"))
        return web.json_response({"ok": bool(resolved)})

    async def keys(request: Any) -> Any:
        require_rest_auth(request)
        rows = await db_list_keys_for_owner(
            require_platform(request.query["platform"]),
            int(request.query["user_id"]),
        )
        return web.json_response(rows)

    async def delete_key(request: Any) -> Any:
        require_rest_auth(request)
        deleted = await db_delete_key_for_owner(request.match_info["key"], "telegram", 0, admin=True)
        return web.json_response({"ok": deleted})

    app.router.add_get("/health", health)
    app.router.add_get("/logs", logs)
    app.router.add_post("/bans", create_ban)
    app.router.add_delete("/bans/{platform:telegram|bale}/{user_id}", delete_ban)
    app.router.add_post("/blocklist", create_block)
    app.router.add_delete("/blocklist", delete_block)
    app.router.add_get("/quarantine", quarantine)
    app.router.add_post("/quarantine/{id}/{action:approve|reject}", quarantine_action)
    app.router.add_post("/broadcasts", create_broadcast)
    app.router.add_get("/appeals", appeals)
    app.router.add_post("/appeals/{id}/{action:accept|reject}", appeal_action)
    app.router.add_get("/keys", keys)
    app.router.add_delete("/keys/{key}", delete_key)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, ADMIN_REST_HOST, ADMIN_REST_PORT)
    await site.start()
    logger.info("REST admin API listening on %s:%s", ADMIN_REST_HOST, ADMIN_REST_PORT)


async def main() -> None:
    if shutil.which("wget") is None:
        raise RuntimeError("wget not found in PATH.")

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    bot_client = make_telegram_client(BOT_SESSION)
    await bot_client.start(bot_token=BOT_TOKEN)
    user_client = make_telegram_client(SESSION)
    await user_client.start(phone=USER_PHONE, password=USER_PASSWORD)
    bot_me = await bot_client.get_me()
    user_me = await user_client.get_me()
    if not getattr(bot_me, "bot", False):
        raise RuntimeError(
            f"{BOT_SESSION}.session is not logged in as a bot. "
            "Set TELETHON_BOT_SESSION to a fresh dev session name or delete the stale session file."
        )
    if getattr(user_me, "bot", False):
        raise RuntimeError(
            f"{SESSION}.session is logged in as a bot, but uploads require a user account session."
        )
    if BOT_SESSION == SESSION:
        raise RuntimeError("TELETHON_BOT_SESSION and TELETHON_SESSION must be different.")
    bot_username = BOT_USERNAME or bot_me.username
    if not bot_username:
        raise RuntimeError("Bot username is required for relay.")
    global bot_upload_target, user_self_id, bot_self_id
    bot_upload_target = bot_username
    user_self_id = user_me.id
    bot_self_id = bot_me.id
    logger.info(
        "Telegram bot client ready id=%s username=%s session=%s",
        bot_self_id,
        bot_me.username,
        BOT_SESSION,
    )
    logger.info(
        "Telegram user client ready id=%s username=%s session=%s",
        user_self_id,
        getattr(user_me, "username", None),
        SESSION,
    )
    logger.info("Telethon connection mode=%s", TELETHON_CONNECTION or "default")
    global db_conn
    db_conn = sqlite3.connect(DB_PATH)
    db_conn.execute(
        "CREATE TABLE IF NOT EXISTS file_cache ("
        "hash TEXT PRIMARY KEY,"
        "message_id INTEGER NOT NULL)"
    )
    db_conn.execute("CREATE INDEX IF NOT EXISTS idx_file_cache_hash ON file_cache(hash)")
    db_conn.execute(
        "CREATE TABLE IF NOT EXISTS bale_file_cache ("
        "hash TEXT PRIMARY KEY,"
        "file_id TEXT NOT NULL)"
    )
    db_conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_bale_file_cache_hash "
        "ON bale_file_cache(hash)"
    )
    db_conn.execute(
        "CREATE TABLE IF NOT EXISTS key_cache ("
        "key TEXT PRIMARY KEY,"
        "source TEXT NOT NULL,"
        "tg_chat_id INTEGER,"
        "tg_message_id INTEGER,"
        "bale_file_id TEXT,"
        "filename TEXT,"
        "file_hash TEXT,"
        "owner_platform TEXT,"
        "owner_user_id INTEGER,"
        "created_at TEXT)"
    )
    db_conn.execute(
        "CREATE TABLE IF NOT EXISTS auto_link ("
        "uid TEXT PRIMARY KEY,"
        "tg_user_id INTEGER UNIQUE,"
        "bale_user_id INTEGER UNIQUE)"
    )
    db_conn.execute(
        "CREATE TABLE IF NOT EXISTS auto_pending ("
        "uid TEXT PRIMARY KEY,"
        "source TEXT NOT NULL,"
        "tg_user_id INTEGER,"
        "bale_user_id INTEGER)"
    )
    db_conn.commit()
    _migrate_key_cache()

    global mod_db_conn
    mod_db_conn = sqlite3.connect(MODERATION_DB_PATH)
    _execute_mod_schema()

    for _ in range(MAX_CONCURRENT_DOWNLOADS):
        asyncio.create_task(worker(bot_client, user_client))
    asyncio.create_task(cleanup_loop())

    global bale_api
    if BALE_BOT_TOKEN:
        bale_api = BaleApi(BALE_BOT_TOKEN, BALE_API_URL)
        global bale_api_offset
        bale_api_offset = await prime_bale_offset(bale_api)
        asyncio.create_task(poll_bale_updates(bale_api, bot_client, user_client))
    else:
        bale_api = None
        logger.info("BALE_BOT_TOKEN is not set; Bale integration disabled")
    asyncio.create_task(broadcast_loop(bot_client, bale_api))
    asyncio.create_task(start_rest_admin_server())

    @bot_client.on(events.NewMessage(incoming=True))
    async def handler(event: events.NewMessage.Event) -> None:
        if bot_self_id is not None and event.sender_id == bot_self_id:
            return
        if (
            user_self_id is not None
            and event.sender_id == user_self_id
            and event.message
            and event.message.media
        ):
            logger.info(
                "Ignoring relay upload from owner user session message_id=%s",
                event.message.id,
            )
            return
        text = (event.raw_text or "").strip()
        logger.info(
            "Telegram bot received message sender_id=%s chat_id=%s text=%r",
            event.sender_id,
            event.chat_id,
            text[:120],
        )
        if event.sender_id is None:
            await event.reply("Could not resolve your user id.")
            return
        tg_user = UserRef("telegram", event.sender_id, event.chat_id, None)
        await mod_register_user(tg_user)
        if text.lower().startswith("/admin"):
            if not is_telegram_admin(event.sender_id):
                await event.reply("Admin access denied.")
                return
            try:
                await event.reply(await handle_admin_command(text, "telegram", event.sender_id))
            except Exception as exc:
                logger.exception("Telegram admin command failed: %s", exc)
                await event.reply(f"Admin command failed: {exc}")
            return
        appeal_cmd, appeal_arg = parse_appeal_command(text)
        if appeal_cmd:
            if appeal_cmd == "status":
                appeal = await mod_get_appeal(tg_user)
                await event.reply(json.dumps(appeal, ensure_ascii=False, indent=2) if appeal else "No appeal found.")
                return
            if appeal_cmd == "create" and appeal_arg:
                appeal_id = await mod_create_or_update_appeal(tg_user, appeal_arg)
                await event.reply(f"Appeal submitted: {appeal_id}")
                return
            await event.reply("Usage: /appeal <message> | /appeal status")
            return
        ban = await mod_is_banned("telegram", event.sender_id)
        if ban:
            await event.reply(banned_message(ban))
            return
        lowered = text.lower()
        if lowered == "/start" or lowered.startswith("/start@"):
            await event.reply("send me a link, a (key:keystring) message, or a file with a caption like (key:yourkey) or forward any file and reply to it with /key <your key> or a file hash (hash: hashfrombefore) :3")
            return
        if lowered == "/keys":
            await event.reply(await handle_user_keys_command("telegram", event.sender_id, "list", None))
            return
        if lowered.startswith("/keydel "):
            await event.reply(await handle_user_keys_command("telegram", event.sender_id, "delete", text.split(maxsplit=1)[1]))
            return
        if lowered.startswith("/keyinfo "):
            await event.reply(await handle_user_keys_command("telegram", event.sender_id, "info", text.split(maxsplit=1)[1]))
            return
        auto_cmd, auto_arg = parse_auto_command(text)
        if auto_cmd:
            if auto_cmd == "enable":
                await handle_auto_enable_telegram(event)
                return
            if auto_cmd == "disable":
                await handle_auto_disable_telegram(event)
                return
            if auto_cmd == "set":
                await handle_auto_set_telegram(bot_client, event, auto_arg)
                return
            await event.reply("Usage: /auto enable | /auto disable | /auto set <UID>")
            return
        key_command = extract_key_command(text)
        if key_command:
            await handle_telegram_key_reply_store(event, key_command)
            return
        hash_query = extract_hash_query(text)
        if hash_query and not (event.message and event.message.media):
            await handle_telegram_hash_request(
                bot_client, user_client, event, hash_query
            )
            return
        key = extract_key(text)
        if is_telegram_uploaded_file(event) and key:
            await handle_telegram_key_store(event, key)
        if is_telegram_uploaded_file(event):
            if bale_api is not None and event.sender_id is not None:
                link = await db_get_auto_link_by_tg(event.sender_id)
                if link:
                    spawn_bale_task(
                        forward_telegram_file_to_bale(
                            bot_client,
                            user_client,
                            bale_api,
                            event,
                            link["bale_user_id"],
                        ),
                        "auto-telegram-to-bale",
                    )
                    return
        if key and not (event.message and event.message.media):
            await handle_telegram_key_request(bot_client, user_client, event, key)
            return
        if lowered in {"ping", "/ping"}:
            await event.reply("Pong!")
            return
        url = extract_url(text)
        if not url:
            await event.reply("send me a link, a (key:keystring) message, or a file with a caption like (key:yourkey) or forward any file and reply to it with /key <your key> or a file hash (hash: hashfrombefore) :3")
            return
        try:
            await asyncio.to_thread(validate_public_http_url, url)
        except ValueError:
            await event.reply("This URL is not allowed.")
            return
        link_block = await mod_find_block_for_link(url)
        if link_block:
            await mod_create_quarantine(
                tg_user,
                url,
                None,
                None,
                None,
                link_block.get("reason") or "blocked link",
            )
            await event.reply(blocked_message())
            return
        user_id = event.sender_id
        if url in pending_links_by_user[user_id]:
            await event.reply("I'm still trying to upload this one :( please wait.")
            return
        if pending_by_user[user_id] >= MAX_PENDING_PER_USER:
            await event.reply("You already have 3 pending downloads. Please wait.")
            return
        if queue.full():
            await event.reply(f"Queue is full ({MAX_QUEUE_SIZE}). Please try later.")
            return
        pending_by_user[user_id] += 1
        pending_links_by_user[user_id].add(url)
        position = queue.qsize() + 1
        status_message = await event.reply(f"Queued (position {position}).")
        job = Job(
            user_id=user_id,
            chat_id=event.chat_id,
            url=url,
            status_message_id=status_message.id,
            username=None,
        )
        await queue.put(job)

    print("Bot is running. User client ready for uploads.")
    await bot_client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
