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

import requests
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
BALE_BOT_TOKEN = os.environ.get("BALE_BOT_TOKEN")
BALE_API_URL = os.environ.get("BALE_API_URL", "https://tapi.bale.ai")
BALE_MAX_UPLOAD_BYTES = 50 * 1024 * 1024
BALE_ZIP_PART_BYTES = 40 * 1024 * 1024

MAX_CONCURRENT_DOWNLOADS = 10
MAX_PENDING_PER_USER = 3
MAX_QUEUE_SIZE = 100
PROGRESS_INTERVAL_SECONDS = 20
MAX_UPLOAD_BYTES = 2 * 1024 * 1024 * 1024
SIZE_LIMIT_EXCEEDED = 3

URL_RE = re.compile(r"(https?://\S+)")
KEY_RE = re.compile(r"(?i)\bkey:(.+)")

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


class BaleThrottledEditor:
    def __init__(self, api: "BaleApi", chat_id: int, message_id: int) -> None:
        self._api = api
        self._chat_id = chat_id
        self._message_id = message_id
        self._last_update = 0.0

    async def update(self, text: str, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._last_update < PROGRESS_INTERVAL_SECONDS:
            return
        await self._api.edit_message_text(self._chat_id, self._message_id, text)
        self._last_update = now


class BaleApi:
    def __init__(self, token: str, base_url: str) -> None:
        self._token = token
        self._base_url = base_url.rstrip("/")

    def _request_sync(
        self,
        method: str,
        data: dict[str, str] | None = None,
        files: dict | None = None,
        timeout: int | tuple[int, int] = 30,
    ) -> dict:
        url = f"{self._base_url}/bot{self._token}/{method}"
        resp = requests.post(url, data=data, files=files, timeout=timeout)
        if not resp.ok:
            body = resp.text.strip()
            raise RuntimeError(
                f"{method} failed ({resp.status_code}): {body or 'no response body'}"
            )
        payload = resp.json()
        if not payload.get("ok"):
            raise RuntimeError(payload.get("description", f"{method} failed"))
        return payload["result"]

    async def request(
        self,
        method: str,
        data: dict[str, str] | None = None,
        files: dict | None = None,
        timeout: int | tuple[int, int] = 30,
    ) -> dict:
        return await asyncio.to_thread(
            self._request_sync, method, data, files, timeout
        )

    async def get_updates(self, offset: int, timeout_seconds: int = 5) -> list[dict]:
        result = await self.request(
            "getUpdates",
            data={"offset": str(offset), "timeout": str(timeout_seconds)},
            timeout=timeout_seconds + 15,
        )
        return result or []

    async def send_message(self, chat_id: int, text: str) -> dict:
        return await self.request(
            "sendMessage", data={"chat_id": str(chat_id), "text": text}
        )

    async def edit_message_text(
        self, chat_id: int, message_id: int, text: str
    ) -> dict | None:
        try:
            return await self.request(
                "editMessageText",
                data={
                    "chat_id": str(chat_id),
                    "message_id": str(message_id),
                    "text": text,
                },
            )
        except Exception:
            return None

    async def send_document(
        self,
        chat_id: int,
        caption: str | None,
        file_id: str | None = None,
        file_path: Path | None = None,
        filename: str | None = None,
    ) -> dict:
        if file_id:
            data = {"chat_id": str(chat_id), "document": file_id}
            if caption:
                data["caption"] = caption
            return await self.request("sendDocument", data=data, timeout=60)
        if not file_path:
            raise ValueError("file_path is required when no file_id is provided")

        def _upload() -> dict:
            data = {"chat_id": str(chat_id)}
            if caption:
                data["caption"] = caption
            safe_name = ascii_filename(filename or file_path.name)
            with file_path.open("rb") as handle:
                files = {
                    "document": (safe_name, handle, "application/octet-stream")
                }
                return self._request_sync(
                    "sendDocument", data=data, files=files, timeout=(10, 300)
                )

        return await asyncio.to_thread(_upload)

    async def get_file(self, file_id: str) -> dict:
        return await self.request("getFile", data={"file_id": file_id})

    def file_url(self, file_path: str) -> str:
        return f"{self._base_url}/file/bot{self._token}/{file_path}"


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
bot_api_offset = 0
bot_api_lock = asyncio.Lock()
db_lock = asyncio.Lock()
db_conn: sqlite3.Connection | None = None
bale_api_offset = 0
bale_api: BaleApi | None = None


def bytes_to_mb(value: int) -> float:
    return round(value / (1024 * 1024), 2)


def filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    basename = Path(unquote(parsed.path)).name
    basename = re.sub(r'[<>:"/\\\\|?*]', "_", basename)
    basename = basename.strip(" .")
    return basename


def ascii_filename(name: str) -> str:
    if not name:
        return "file"
    sanitized = "".join(ch if ord(ch) < 128 else "_" for ch in name)
    sanitized = re.sub(r'[<>:"/\\\\|?*]', "_", sanitized).strip(" .")
    return sanitized or "file"


def should_zip(path: Path) -> bool:
    suffix = path.suffix.lower()
    return suffix not in {".png", ".jpg", ".jpeg"}


def create_zip(source_path: Path, zip_path: Path) -> None:
    import zipfile

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(source_path, arcname=source_path.name)


def split_file(path: Path, part_size: int) -> list[Path]:
    parts: list[Path] = []
    index = 1
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(part_size)
            if not chunk:
                break
            part_path = path.with_name(f"{path.stem}.part{index:02d}.zip")
            with part_path.open("wb") as part_handle:
                part_handle.write(chunk)
            parts.append(part_path)
            index += 1
    return parts


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
        "--timeout=10",
        "--tries=2",
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
        "--timeout=10",
        "--tries=2",
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


def make_temp_dir(prefix: str) -> Path:
    path = DOWNLOAD_DIR / f"{prefix}-{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=True)
    return path


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
    return await user_client.send_file(target, file_path, **kwargs)


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
            "SELECT key, source, tg_chat_id, tg_message_id, bale_file_id, filename, file_hash "
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
    }


async def db_insert_key_entry(
    key: str,
    source: str,
    tg_chat_id: int | None = None,
    tg_message_id: int | None = None,
    bale_file_id: str | None = None,
    filename: str | None = None,
    file_hash: str | None = None,
) -> bool:
    if db_conn is None:
        return False
    async with db_lock:
        cursor = db_conn.execute("SELECT 1 FROM key_cache WHERE key = ?", (key,))
        if cursor.fetchone():
            return False
        db_conn.execute(
            "INSERT INTO key_cache(key, source, tg_chat_id, tg_message_id, bale_file_id, filename, file_hash) "
            "VALUES(?, ?, ?, ?, ?, ?, ?)",
            (
                key,
                source,
                tg_chat_id,
                tg_message_id,
                bale_file_id,
                filename,
                file_hash,
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
    with requests.get(url, stream=True, timeout=(10, 120)) as resp:
        resp.raise_for_status()
        with target_path.open("wb") as handle:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                handle.write(chunk)
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
        create_zip(file_path, zip_path)
        zip_size = zip_path.stat().st_size
        if zip_size > BALE_ZIP_PART_BYTES:
            parts = split_file(zip_path, BALE_ZIP_PART_BYTES)
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
        item_hash = compute_sha256(item_path)
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
                if (
                    "failed to upload file bytes" not in error_text
                    and "504" not in error_text
                ):
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
                upload_caption = f"Uploaded: {target_path.name}\nHash: {file_hash}"
                if cached_message_id is not None:
                    try:
                        await relay_via_bot(bot_client, job, cached_message_id, "")
                        await editor.update("Done.", force=True)
                        return
                    except Exception as exc:
                        logger.exception("Cached relay failed: %s", exc)
                        await db_delete_hash(file_hash)
                await editor.update("Uploading...", force=True)
                file_size = target_path.stat().st_size
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
                await relay_via_bot(bot_client, job, bot_message_id, "")
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
) -> None:
    editor = BaleThrottledEditor(api, chat_id, status_message_id)
    job_dir = DOWNLOAD_DIR / uuid.uuid4().hex
    job_dir.mkdir(parents=True, exist_ok=True)
    output_path = job_dir / "download.tmp"
    await editor.update("Starting download...", force=True)
    try:
        async with bale_global_semaphore:
            async with bale_per_user_semaphore[user_id]:
                header_name, content_type, content_length = await probe_response_meta(
                    url
                )
                if content_length is not None and content_length == 0:
                    await editor.update("Download failed.", force=True)
                    return
                rc = await download_with_progress(url, output_path, editor)
                if rc != 0:
                    await editor.update("Download failed.", force=True)
                    return
                url_name = filename_from_url(url)
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
                if target_path.stat().st_size == 0:
                    await editor.update("Download failed.", force=True)
                    return
                original_hash = compute_sha256(target_path)
                await editor.update("Uploading...", force=True)
                await upload_path_to_bale(api, chat_id, target_path, original_hash)
                await editor.update("Done.", force=True)
    except Exception as exc:
        logger.exception("Bale link job failed: %s", exc)
        await editor.update("Download failed.", force=True)
    finally:
        if job_dir.exists():
            shutil.rmtree(job_dir, ignore_errors=True)
        bale_pending_by_user[user_id] = max(0, bale_pending_by_user[user_id] - 1)
        bale_pending_links_by_user[user_id].discard(url)


async def handle_telegram_key_store(
    event: events.NewMessage.Event, key: str
) -> None:
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
        await relay_telegram_cached(event.chat_id, tg_chat_id, tg_message_id, None)
        await status.edit("Done.")
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
        await status.edit("Done.")
    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)


async def handle_bale_key_store(api: BaleApi, chat_id: int, message: dict) -> None:
    caption = (message.get("caption") or "").strip()
    key = extract_key(caption)
    if not key:
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
            await api.edit_message_text(chat_id, status.get("message_id"), "Done.")
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
            await api.edit_message_text(chat_id, status.get("message_id"), "Done.")
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
        original_hash = compute_sha256(file_path)
        file_ids = await upload_path_to_bale(api, chat_id, file_path, original_hash)
        if file_ids:
            bale_value = json.dumps(file_ids) if len(file_ids) > 1 else file_ids[0]
            await db_update_key_entry(
                key, bale_file_id=bale_value, file_hash=original_hash
            )
        await api.edit_message_text(chat_id, status.get("message_id"), "Done.")
    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)


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
                text = (message.get("text") or "").strip()
                caption = (message.get("caption") or "").strip()
                if caption and extract_key(caption):
                    await handle_bale_key_store(api, chat_id, message)
                    continue
                if text:
                    key = extract_key(text)
                    if key:
                        await handle_bale_key_request(api, bot_client, chat_id, key)
                        continue
                    url = extract_url(text)
                    if url:
                        if url in bale_pending_links_by_user[chat_id]:
                            await api.send_message(
                                chat_id, "I'm still trying to upload this one :("
                            )
                            continue
                        if bale_pending_by_user[chat_id] >= MAX_PENDING_PER_USER:
                            await api.send_message(
                                chat_id, "You already have 3 pending downloads."
                            )
                            continue
                        bale_pending_by_user[chat_id] += 1
                        bale_pending_links_by_user[chat_id].add(url)
                        status_message = await api.send_message(
                            chat_id, "Queued."
                        )
                        asyncio.create_task(
                            process_bale_link(
                                api,
                                chat_id,
                                chat_id,
                                url,
                                status_message.get("message_id"),
                            )
                        )
                        continue
                    if text.lower() in {"ping", "/ping"}:
                        await api.send_message(chat_id, "Pong!")
                        continue
                    await api.send_message(
                        chat_id,
                        "send me a link, a (key:keystring) message, or a file with a caption like (key:yourkey) :3",
                    )
                    continue
            except Exception as exc:
                logger.exception("Bale update handling failed: %s", exc)
                continue


def extract_url(text: str) -> str | None:
    match = URL_RE.search(text)
    if not match:
        return None
    return match.group(1)


def extract_key(text: str) -> str | None:
    match = KEY_RE.search(text)
    if not match:
        return None
    key = match.group(1).strip()
    return key or None


async def main() -> None:
    if shutil.which("wget") is None:
        raise RuntimeError("wget not found in PATH.")
    if BALE_BOT_TOKEN is None:
        raise RuntimeError("BALE_BOT_TOKEN is required for Bale integration.")

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
        "file_hash TEXT)"
    )
    db_conn.commit()

    for _ in range(MAX_CONCURRENT_DOWNLOADS):
        asyncio.create_task(worker(bot_client, user_client))

    global bale_api
    bale_api = BaleApi(BALE_BOT_TOKEN, BALE_API_URL)
    asyncio.create_task(poll_bale_updates(bale_api, bot_client, user_client))

    @bot_client.on(events.NewMessage(incoming=True))
    async def handler(event: events.NewMessage.Event) -> None:
        if user_self_id is not None and event.sender_id == user_self_id:
            return
        text = (event.raw_text or "").strip()
        key = extract_key(text)
        if event.message and event.message.media and key:
            await handle_telegram_key_store(event, key)
            return
        if key and not (event.message and event.message.media):
            await handle_telegram_key_request(bot_client, user_client, event, key)
            return
        lowered = text.lower()
        if lowered in {"ping", "/ping"}:
            await event.reply("Pong!")
            return
        url = extract_url(text)
        if not url:
            await event.reply("send me a link, a (key:keystring) message, or a file with a caption like (key:yourkey) :3")
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
        )
        await queue.put(job)

    print("Bot is running. User client ready for uploads.")
    await bot_client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
