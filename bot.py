import asyncio
import json
import logging
import os
import re
import shutil
import sqlite3
import sys
import time
import uuid
import urllib.parse
import urllib.request
import hashlib
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse

from telethon import TelegramClient, events
from telethon.errors import RPCError


API_ID = int(os.environ["TELETHON_API_ID"])
API_HASH = os.environ["TELETHON_API_HASH"]
SESSION = os.environ.get("TELETHON_SESSION", "userbot")
BOT_TOKEN = os.environ["TELETHON_BOT_TOKEN"]
BOT_USERNAME = os.environ.get("TELETHON_BOT_USERNAME")
BOT_API_URL = os.environ.get("BOT_API_URL", "https://api.telegram.org")
USER_PHONE = os.environ["TELETHON_PHONE"]
USER_PASSWORD = os.environ.get("TELETHON_PASSWORD")
DOWNLOAD_DIR = Path(os.environ.get("DOWNLOAD_DIR", "downloads"))
DB_PATH = Path(os.environ.get("HASH_DB_PATH", "hash_cache.db"))

MAX_CONCURRENT_DOWNLOADS = 10
MAX_PENDING_PER_USER = 3
MAX_QUEUE_SIZE = 100
PROGRESS_INTERVAL_SECONDS = 20
MAX_UPLOAD_BYTES = 2 * 1024 * 1024 * 1024
SIZE_LIMIT_EXCEEDED = 3

URL_RE = re.compile(r"(https?://\S+)")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("linktofile")


@dataclass
class Job:
    user_id: int
    chat_id: int
    url: str
    status_message_id: int


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
per_user_semaphore: dict[int, asyncio.Semaphore] = defaultdict(
    lambda: asyncio.Semaphore(MAX_PENDING_PER_USER)
)
global_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
bot_upload_target: str | None = None
user_self_id: int | None = None
bot_api_offset = 0
bot_api_lock = asyncio.Lock()
db_lock = asyncio.Lock()
db_conn: sqlite3.Connection | None = None


def bytes_to_mb(value: int) -> float:
    return round(value / (1024 * 1024), 2)


def filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    basename = Path(unquote(parsed.path)).name
    basename = re.sub(r'[<>:"/\\\\|?*]', "_", basename)
    basename = basename.strip(" .")
    return basename


def filename_from_header(value: str) -> str | None:
    match = re.search(r"filename\*=([^']*)''([^;]+)", value, flags=re.IGNORECASE)
    if match:
        return unquote(match.group(2))
    match = re.search(r'filename="([^"]+)"', value, flags=re.IGNORECASE)
    if match:
        return match.group(1)
    match = re.search(r"filename=([^;]+)", value, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip().strip('"')
    return None


async def probe_response_meta(url: str) -> tuple[str | None, str | None, int | None]:
    process = await asyncio.create_subprocess_exec(
        "wget",
        "--server-response",
        "--spider",
        "--max-redirect=20",
        "--connect-timeout=30",
        "--read-timeout=30",
        url,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await process.communicate()
    if process.returncode != 0 or not stderr:
        return None, None, None
    headers = stderr.decode(errors="ignore")
    header_lines = [
        line for line in headers.splitlines() if "content-disposition" in line.lower()
    ]
    for line in reversed(header_lines):
        value = line.split(":", 1)[-1].strip()
        name = filename_from_header(value)
        if name:
            return name, None, None
    content_type = None
    content_length = None
    type_lines = [
        line for line in headers.splitlines() if "content-type" in line.lower()
    ]
    for line in reversed(type_lines):
        content_type = line.split(":", 1)[-1].strip().lower()
        break
    length_lines = [
        line for line in headers.splitlines() if "content-length" in line.lower()
    ]
    for line in reversed(length_lines):
        value = line.split(":", 1)[-1].strip()
        try:
            content_length = int(value)
        except ValueError:
            content_length = None
        break
    return None, content_type, content_length


def apply_html_extension(name: str, content_type: str | None) -> str:
    if not name:
        return name
    if Path(name).suffix:
        return name
    if not content_type:
        return name
    if content_type.startswith("text/html") or content_type.startswith(
        "application/xhtml+xml"
    ):
        return f"{name}.html"
    return name


async def download_with_progress(
    url: str, output_path: Path, editor: ThrottledEditor
) -> int:
    process = await asyncio.create_subprocess_exec(
        "wget",
        "--progress=dot:mega",
        "--connect-timeout=30",
        "--read-timeout=30",
        "-O",
        str(output_path),
        url,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    last_update = 0.0
    percent_re = re.compile(r"(\d+)%")
    stderr_tail: list[str] = []
    while True:
        line = await process.stderr.readline()
        if not line:
            break
        decoded = line.decode(errors="ignore")
        stderr_tail.append(decoded.strip())
        if len(stderr_tail) > 5:
            stderr_tail.pop(0)
        if output_path.exists() and output_path.stat().st_size > MAX_UPLOAD_BYTES:
            logger.error("wget exceeded size cap url=%s", url)
            process.terminate()
            await process.wait()
            return SIZE_LIMIT_EXCEEDED
        match = percent_re.search(decoded)
        now = time.monotonic()
        if match and now - last_update >= PROGRESS_INTERVAL_SECONDS:
            last_update = now
            await editor.update(f"Downloading... {match.group(1)}%")
    rc = await process.wait()
    if rc != 0:
        tail = " | ".join([line for line in stderr_tail if line])
        logger.error("wget failed rc=%s url=%s tail=%s", rc, url, tail)
    return rc


def compute_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


async def upload_with_progress(
    user_client: TelegramClient,
    target: object,
    file_path: Path,
    editor: ThrottledEditor,
    caption: str,
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

    return await user_client.send_file(
        target,
        file_path,
        caption=caption,
        progress_callback=progress_callback,
    )


async def relay_via_bot(
    bot_client: TelegramClient,
    job: Job,
    upload_message_id: int,
) -> None:
    if user_self_id is None:
        raise RuntimeError("Bot relay is not configured.")
    for _ in range(10):
        try:
            await copy_message_via_bot_api(job.chat_id, user_self_id, upload_message_id)
            return
        except Exception:
            await asyncio.sleep(1)
    raise RuntimeError("Bot relay failed to access uploaded media.")


async def copy_message_via_bot_api(
    chat_id: int, from_chat_id: int, message_id: int
) -> None:
    def _do_request() -> None:
        url = f"{BOT_API_URL.rstrip('/')}/bot{BOT_TOKEN}/copyMessage"
        payload = urllib.parse.urlencode(
            {
                "chat_id": str(chat_id),
                "from_chat_id": str(from_chat_id),
                "message_id": str(message_id),
            }
        ).encode()
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


async def find_bot_message_id(
    filename: str, file_hash: str, timeout_seconds: int = 30
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
            caption = message.get("caption") or ""
            if file_hash in caption or filename in caption:
                return message.get("message_id")
        await asyncio.sleep(1)
    return None


async def process_job(
    bot_client: TelegramClient, user_client: TelegramClient, job: Job
) -> None:
    editor = ThrottledEditor(bot_client, job.chat_id, job.status_message_id)
    job_dir = DOWNLOAD_DIR / uuid.uuid4().hex
    job_dir.mkdir(parents=True, exist_ok=True)
    output_path = job_dir / "download.tmp"
    await editor.update("Starting download...", force=True)
    try:
        async with global_semaphore:
            async with per_user_semaphore[job.user_id]:
                if not bot_upload_target:
                    logger.error("Bot relay is not configured.")
                    await editor.update("Download failed.", force=True)
                    return
                header_name, content_type, content_length = await probe_response_meta(
                    job.url
                )
                if content_length is not None and content_length > MAX_UPLOAD_BYTES:
                    await editor.update(
                        "I cannot upload files bigger than 2GB :(",
                        force=True,
                    )
                    return
                rc = await download_with_progress(job.url, output_path, editor)
                if rc == SIZE_LIMIT_EXCEEDED:
                    await editor.update(
                        "I cannot upload files bigger than 2GB :(",
                        force=True,
                    )
                    return
                if rc != 0:
                    await editor.update("Download failed.", force=True)
                    return
                url_name = filename_from_url(job.url)
                header_name = (
                    re.sub(r'[<>:"/\\\\|?*]', "_", header_name).strip(" .")
                    if header_name
                    else ""
                )
                url_name = apply_html_extension(url_name, content_type)
                header_name = apply_html_extension(header_name, content_type)
                target_name = url_name or header_name or "untitled"
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
                cached_message_id = await db_get_message_id(file_hash)
                if cached_message_id is not None:
                    try:
                        await relay_via_bot(bot_client, job, cached_message_id)
                        await editor.update("Done.", force=True)
                        return
                    except Exception as exc:
                        logger.exception("Cached relay failed: %s", exc)
                        await db_delete_hash(file_hash)
                await editor.update("Uploading...", force=True)
                #caption = f"Uploaded: {target_path.name}\nHash: {file_hash}"
                caption = f"File: `{target_path.name}`"
                upload_message = await upload_with_progress(
                    user_client, bot_upload_target, target_path, editor, caption
                )
                upload_message_id = (
                    upload_message[0].id
                    if isinstance(upload_message, list)
                    else upload_message.id
                )
                bot_message_id = await find_bot_message_id(
                    target_path.name, file_hash
                )
                if bot_message_id is None:
                    logger.error(
                        "Bot could not see uploaded media filename=%s",
                        target_path.name,
                    )
                    await editor.update("Download failed.", force=True)
                    return
                await db_set_message_id(file_hash, bot_message_id)
                await relay_via_bot(bot_client, job, bot_message_id)
                await editor.update("Done.", force=True)
    except Exception as exc:
        logger.exception("Job failed: %s", exc)
        await editor.update("Download failed.", force=True)
    finally:
        if job_dir.exists():
            try:
                shutil.rmtree(job_dir)
            except OSError:
                pass
        pending_by_user[job.user_id] = max(0, pending_by_user[job.user_id] - 1)


async def worker(bot_client: TelegramClient, user_client: TelegramClient) -> None:
    while True:
        job = await queue.get()
        try:
            await process_job(bot_client, user_client, job)
        finally:
            queue.task_done()


def extract_url(text: str) -> str | None:
    match = URL_RE.search(text)
    if not match:
        return None
    return match.group(1)


async def main() -> None:
    if shutil.which("wget") is None:
        raise RuntimeError("wget not found in PATH.")

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    bot_client = TelegramClient("bot", API_ID, API_HASH)
    await bot_client.start(bot_token=BOT_TOKEN)
    user_client = TelegramClient(SESSION, API_ID, API_HASH)
    await user_client.start(phone=USER_PHONE, password=USER_PASSWORD)
    bot_me = await bot_client.get_me()
    user_me = await user_client.get_me()
    bot_username = BOT_USERNAME or bot_me.username
    if not bot_username:
        raise RuntimeError("Bot username is required for relay.")
    global bot_upload_target, user_self_id
    bot_upload_target = bot_username
    user_self_id = user_me.id
    global db_conn
    db_conn = sqlite3.connect(DB_PATH)
    db_conn.execute(
        "CREATE TABLE IF NOT EXISTS file_cache ("
        "hash TEXT PRIMARY KEY,"
        "message_id INTEGER NOT NULL)"
    )
    db_conn.execute("CREATE INDEX IF NOT EXISTS idx_file_cache_hash ON file_cache(hash)")
    db_conn.commit()

    for _ in range(MAX_CONCURRENT_DOWNLOADS):
        asyncio.create_task(worker(bot_client, user_client))

    @bot_client.on(events.NewMessage(incoming=True))
    async def handler(event: events.NewMessage.Event) -> None:
        if user_self_id is not None and event.sender_id == user_self_id:
            return
        text = (event.raw_text or "").strip()
        lowered = text.lower()
        if lowered in {"ping", "/ping"}:
            await event.reply("Pong!")
            return
        url = extract_url(text)
        if not url:
            await event.reply("Send me a link :3")
            return
        user_id = event.sender_id
        if pending_by_user[user_id] >= MAX_PENDING_PER_USER:
            await event.reply("You already have 3 pending downloads. Please wait.")
            return
        if queue.full():
            await event.reply(f"Queue is full ({MAX_QUEUE_SIZE}). Please try later.")
            return
        pending_by_user[user_id] += 1
        position = queue.qsize() + 1
        status_message = await event.reply(f"Queued (position {position}).")
        job = Job(
            user_id=user_id,
            chat_id=event.chat_id,
            url=url,
            status_message_id=status_message.id,
        )
        await queue.put(job)

    print("Bot is running. User client ready for uploads.")
    await bot_client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
