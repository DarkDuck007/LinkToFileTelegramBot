import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import sqlite3
import sys
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests


BOT_TOKEN = os.environ["BALE_BOT_TOKEN"]
BOT_API_URL = os.environ.get("BALE_API_URL", "https://tapi.bale.ai")
DOWNLOAD_DIR = Path(os.environ.get("BALE_DOWNLOAD_DIR", "bale_downloads"))
DB_PATH = Path(os.environ.get("BALE_HASH_DB_PATH", "bale_hash_cache.db"))

MAX_CONCURRENT_DOWNLOADS = 6
MAX_PENDING_PER_USER = 3
MAX_QUEUE_SIZE = 100
PROGRESS_INTERVAL_SECONDS = 20
MAX_UPLOAD_BYTES = 50 * 1024 * 1024
SIZE_LIMIT_EXCEEDED = 3

URL_RE = re.compile(r"(https?://\S+)")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("linktofile-bale")


@dataclass
class Job:
    user_id: int
    chat_id: int
    url: str
    status_message_id: int


class BaleApi:
    def __init__(self, token: str, base_url: str) -> None:
        self._token = token
        self._base_url = base_url.rstrip("/")

    def _request_sync(
        self,
        method: str,
        data: dict[str, str] | None = None,
        files: dict | None = None,
        timeout: int = 30,
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
        timeout: int = 30,
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
                    "sendDocument", data=data, files=files, timeout=120
                )

        return await asyncio.to_thread(_upload)


class ThrottledEditor:
    def __init__(self, api: BaleApi, chat_id: int, message_id: int) -> None:
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


queue: asyncio.Queue[Job] = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
pending_by_user: dict[int, int] = defaultdict(int)
pending_links_by_user: dict[int, set[str]] = defaultdict(set)
per_user_semaphore: dict[int, asyncio.Semaphore] = defaultdict(
    lambda: asyncio.Semaphore(MAX_PENDING_PER_USER)
)
global_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
bot_api_offset = 0
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


def ascii_filename(name: str) -> str:
    if not name:
        return "file"
    sanitized = "".join(ch if ord(ch) < 128 else "_" for ch in name)
    sanitized = re.sub(r'[<>:"/\\\\|?*]', "_", sanitized).strip(" .")
    return sanitized or "file"


def filename_from_header(value: str) -> str | None:
    match = re.search(r"filename\\*=([^']*)''([^;]+)", value, flags=re.IGNORECASE)
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


async def db_get_file_id(file_hash: str) -> str | None:
    if db_conn is None:
        return None
    async with db_lock:
        cursor = db_conn.execute(
            "SELECT file_id FROM bale_file_cache WHERE hash = ?", (file_hash,)
        )
        row = cursor.fetchone()
        return row[0] if row else None


async def db_set_file_id(file_hash: str, file_id: str) -> None:
    if db_conn is None:
        return
    async with db_lock:
        db_conn.execute(
            "INSERT OR REPLACE INTO bale_file_cache(hash, file_id) VALUES(?, ?)",
            (file_hash, file_id),
        )
        db_conn.commit()


async def db_delete_hash(file_hash: str) -> None:
    if db_conn is None:
        return
    async with db_lock:
        db_conn.execute("DELETE FROM bale_file_cache WHERE hash = ?", (file_hash,))
        db_conn.commit()


def extract_file_id(message: dict) -> str | None:
    document = message.get("document") or {}
    return document.get("file_id")


async def process_job(api: BaleApi, job: Job) -> None:
    editor = ThrottledEditor(api, job.chat_id, job.status_message_id)
    job_dir = DOWNLOAD_DIR / uuid.uuid4().hex
    job_dir.mkdir(parents=True, exist_ok=True)
    output_path = job_dir / "download.tmp"
    await editor.update("Starting download...", force=True)
    try:
        async with global_semaphore:
            async with per_user_semaphore[job.user_id]:
                header_name, content_type, content_length = await probe_response_meta(
                    job.url
                )
                if content_length is not None and content_length > MAX_UPLOAD_BYTES:
                    await editor.update(
                        "I cannot upload files bigger than 50MB :(",
                        force=True,
                    )
                    return
                rc = await download_with_progress(job.url, output_path, editor)
                if rc == SIZE_LIMIT_EXCEEDED:
                    await editor.update(
                        "I cannot upload files bigger than 50MB :(",
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
                cached_file_id = await db_get_file_id(file_hash)
                file_size = target_path.stat().st_size
                if file_size > MAX_UPLOAD_BYTES:
                    await editor.update(
                        "I cannot upload files bigger than 50MB :(",
                        force=True,
                    )
                    return
                if file_size == 0:
                    await editor.update("Download failed.", force=True)
                    return
                upload_caption = f"Uploaded: {target_path.name}\nHash: {file_hash}"
                if cached_file_id:
                    try:
                        await api.send_document(
                            job.chat_id, upload_caption, file_id=cached_file_id
                        )
                        await editor.update("Done.", force=True)
                        return
                    except Exception as exc:
                        logger.exception("Cached send failed: %s", exc)
                        await db_delete_hash(file_hash)
                await editor.update("Uploading...", force=True)
                message = None
                for attempt in range(3):
                    try:
                        message = await api.send_document(
                            job.chat_id,
                            upload_caption,
                            file_path=target_path,
                            filename=target_path.name,
                        )
                        break
                    except RuntimeError as exc:
                        if "failed to upload file bytes" not in str(exc).lower():
                            raise
                        if attempt == 2:
                            raise
                        await asyncio.sleep(2 * (attempt + 1))
                file_id = extract_file_id(message)
                if file_id:
                    await db_set_file_id(file_hash, file_id)
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


async def worker(api: BaleApi) -> None:
    while True:
        job = await queue.get()
        try:
            await process_job(api, job)
        finally:
            queue.task_done()


def extract_url(text: str) -> str | None:
    match = URL_RE.search(text)
    if not match:
        return None
    return match.group(1)


async def handle_update(api: BaleApi, update: dict) -> None:
    message = update.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    if chat_id is None:
        return
    sender = message.get("from") or {}
    user_id = sender.get("id", chat_id)
    text = (message.get("text") or "").strip()
    if not text:
        return
    lowered = text.lower()
    if lowered in {"ping", "/ping"}:
        await api.send_message(chat_id, "Pong!")
        return
    url = extract_url(text)
    if not url:
        await api.send_message(chat_id, "Send me a link :3")
        return
    if url in pending_links_by_user[user_id]:
        await api.send_message(chat_id, "I'm still trying to upload this one :(")
        return
    if pending_by_user[user_id] >= MAX_PENDING_PER_USER:
        await api.send_message(chat_id, "You already have 3 pending downloads.")
        return
    if queue.full():
        await api.send_message(chat_id, f"Queue is full ({MAX_QUEUE_SIZE}).")
        return
    pending_by_user[user_id] += 1
    pending_links_by_user[user_id].add(url)
    position = queue.qsize() + 1
    status_message = await api.send_message(chat_id, f"Queued (position {position}).")
    job = Job(
        user_id=user_id,
        chat_id=chat_id,
        url=url,
        status_message_id=status_message.get("message_id"),
    )
    await queue.put(job)


async def poll_updates(api: BaleApi) -> None:
    global bot_api_offset
    while True:
        try:
            updates = await api.get_updates(bot_api_offset, timeout_seconds=5)
        except Exception as exc:
            logger.exception("getUpdates failed: %s", exc)
            await asyncio.sleep(2)
            continue
        for update in updates:
            bot_api_offset = max(bot_api_offset, update.get("update_id", 0) + 1)
            await handle_update(api, update)


async def main() -> None:
    if shutil.which("wget") is None:
        raise RuntimeError("wget not found in PATH.")

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    api = BaleApi(BOT_TOKEN, BOT_API_URL)
    global db_conn
    db_conn = sqlite3.connect(DB_PATH)
    db_conn.execute(
        "CREATE TABLE IF NOT EXISTS bale_file_cache ("
        "hash TEXT PRIMARY KEY,"
        "file_id TEXT NOT NULL)"
    )
    db_conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_bale_file_cache_hash "
        "ON bale_file_cache(hash)"
    )
    db_conn.commit()

    for _ in range(MAX_CONCURRENT_DOWNLOADS):
        asyncio.create_task(worker(api))

    print("Bale bot is running.")
    await poll_updates(api)


if __name__ == "__main__":
    asyncio.run(main())
