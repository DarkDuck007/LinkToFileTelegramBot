import asyncio
import io
import json
import os
import re
import shutil
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse

import aiohttp


BOT_TOKEN = os.environ["TELETHON_BOT_TOKEN"]
BOT_API_URL = os.environ["BOT_API_URL"]
DOWNLOAD_DIR = Path(os.environ.get("DOWNLOAD_DIR", "downloads"))

MAX_CONCURRENT_DOWNLOADS = 10
MAX_PENDING_PER_USER = 3
MAX_QUEUE_SIZE = 100
PROGRESS_INTERVAL_SECONDS = 20

URL_RE = re.compile(r"(https?://\S+)")


@dataclass
class Job:
    user_id: int
    chat_id: int
    url: str
    status_message_id: int


class BotAPI:
    def __init__(self, base_url: str, token: str, session: aiohttp.ClientSession) -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._session = session

    def _url(self, method: str) -> str:
        return f"{self._base_url}/bot{self._token}/{method}"

    async def request(self, method: str, **kwargs: object) -> dict:
        async with self._session.post(self._url(method), **kwargs) as response:
            response.raise_for_status()
            payload = await response.json()
            if not payload.get("ok"):
                raise RuntimeError(payload.get("description", "Bot API error"))
            return payload

    async def get_updates(self, offset: int, timeout: int) -> list[dict]:
        payload = await self.request(
            "getUpdates", json={"offset": offset, "timeout": timeout}
        )
        return payload.get("result", [])

    async def send_message(
        self, chat_id: int, text: str, reply_to_message_id: int | None = None
    ) -> dict:
        data: dict[str, object] = {"chat_id": chat_id, "text": text}
        if reply_to_message_id is not None:
            data["reply_to_message_id"] = reply_to_message_id
        payload = await self.request("sendMessage", json=data)
        return payload["result"]

    async def edit_message_text(self, chat_id: int, message_id: int, text: str) -> None:
        await self.request(
            "editMessageText",
            json={"chat_id": chat_id, "message_id": message_id, "text": text},
        )

    async def send_document(self, data: aiohttp.FormData) -> dict:
        payload = await self.request("sendDocument", data=data)
        return payload["result"]


class ThrottledEditor:
    def __init__(self, api: BotAPI, chat_id: int, message_id: int) -> None:
        self._api = api
        self._chat_id = chat_id
        self._message_id = message_id
        self._last_update = 0.0

    async def update(self, text: str, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._last_update < PROGRESS_INTERVAL_SECONDS:
            return
        try:
            await self._api.edit_message_text(self._chat_id, self._message_id, text)
        except Exception:
            return
        self._last_update = now


class UploadState:
    def __init__(self, total: int) -> None:
        self.total = total
        self.sent = 0
        self.done = asyncio.Event()


class ProgressFile(io.BufferedReader):
    def __init__(self, raw: io.BufferedReader, state: UploadState) -> None:
        super().__init__(raw)
        self._state = state

    def read(self, size: int = -1) -> bytes:
        chunk = super().read(size)
        self._state.sent += len(chunk)
        return chunk


queue: asyncio.Queue[Job] = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
pending_by_user: dict[int, int] = defaultdict(int)
per_user_semaphore: dict[int, asyncio.Semaphore] = defaultdict(
    lambda: asyncio.Semaphore(MAX_PENDING_PER_USER)
)
global_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)


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


async def probe_response_meta(url: str) -> tuple[str | None, str | None]:
    process = await asyncio.create_subprocess_exec(
        "wget",
        "--server-response",
        "--spider",
        "--max-redirect=20",
        url,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await process.communicate()
    if process.returncode != 0 or not stderr:
        return None, None
    headers = stderr.decode(errors="ignore")
    header_lines = [
        line for line in headers.splitlines() if "content-disposition" in line.lower()
    ]
    for line in reversed(header_lines):
        value = line.split(":", 1)[-1].strip()
        name = filename_from_header(value)
        if name:
            return name, None
    content_type = None
    type_lines = [
        line for line in headers.splitlines() if "content-type" in line.lower()
    ]
    for line in reversed(type_lines):
        content_type = line.split(":", 1)[-1].strip().lower()
        break
    return None, content_type


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
        "-O",
        str(output_path),
        url,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    last_update = 0.0
    percent_re = re.compile(r"(\d+)%")
    while True:
        line = await process.stderr.readline()
        if not line:
            break
        decoded = line.decode(errors="ignore")
        match = percent_re.search(decoded)
        now = time.monotonic()
        if match and now - last_update >= PROGRESS_INTERVAL_SECONDS:
            last_update = now
            await editor.update(f"Downloading... {match.group(1)}%")
    return await process.wait()


async def upload_with_progress(
    api: BotAPI, job: Job, file_path: Path, editor: ThrottledEditor
) -> None:
    total = file_path.stat().st_size
    state = UploadState(total=total)

    async def reporter() -> None:
        while not state.done.is_set():
            await asyncio.sleep(PROGRESS_INTERVAL_SECONDS)
            await editor.update(
                f"Uploading... {bytes_to_mb(state.sent)} / {bytes_to_mb(state.total)} MB"
            )

    report_task = asyncio.create_task(reporter())
    try:
        with file_path.open("rb") as raw:
            wrapped = ProgressFile(raw, state)
            data = aiohttp.FormData()
            data.add_field("chat_id", str(job.chat_id))
            data.add_field("caption", f"Uploaded: {file_path.name}")
            data.add_field(
                "document",
                wrapped,
                filename=file_path.name,
                content_type="application/octet-stream",
            )
            await api.send_document(data)
    finally:
        state.done.set()
        await asyncio.sleep(0)
        report_task.cancel()


async def process_job(api: BotAPI, job: Job) -> None:
    editor = ThrottledEditor(api, job.chat_id, job.status_message_id)
    job_dir = DOWNLOAD_DIR / uuid.uuid4().hex
    job_dir.mkdir(parents=True, exist_ok=True)
    output_path = job_dir / "download.tmp"
    await editor.update("Starting download...", force=True)
    try:
        async with global_semaphore:
            async with per_user_semaphore[job.user_id]:
                header_name, content_type = await probe_response_meta(job.url)
                rc = await download_with_progress(job.url, output_path, editor)
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
                await editor.update("Uploading...", force=True)
                await upload_with_progress(api, job, target_path, editor)
                await editor.update("Done.", force=True)
    except Exception as exc:
        await editor.update(f"Error: {exc}", force=True)
    finally:
        if job_dir.exists():
            try:
                shutil.rmtree(job_dir)
            except OSError:
                pass
        pending_by_user[job.user_id] = max(0, pending_by_user[job.user_id] - 1)


async def worker(api: BotAPI) -> None:
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


async def handle_message(api: BotAPI, message: dict) -> None:
    if "text" not in message:
        return
    text = (message.get("text") or "").strip()
    lowered = text.lower()
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    sender = message.get("from") or {}
    user_id = sender.get("id")
    if chat_id is None or user_id is None:
        return
    if lowered in {"ping", "/ping"}:
        await api.send_message(chat_id, "Pong!", reply_to_message_id=message.get("message_id"))
        return
    url = extract_url(text)
    if not url:
        return
    if pending_by_user[user_id] >= MAX_PENDING_PER_USER:
        await api.send_message(
            chat_id,
            "You already have 3 pending downloads. Please wait.",
            reply_to_message_id=message.get("message_id"),
        )
        return
    if queue.full():
        await api.send_message(
            chat_id,
            f"Queue is full ({MAX_QUEUE_SIZE}). Please try later.",
            reply_to_message_id=message.get("message_id"),
        )
        return
    pending_by_user[user_id] += 1
    position = queue.qsize() + 1
    status_message = await api.send_message(
        chat_id,
        f"Queued (position {position}).",
        reply_to_message_id=message.get("message_id"),
    )
    job = Job(
        user_id=user_id,
        chat_id=chat_id,
        url=url,
        status_message_id=status_message["message_id"],
    )
    await queue.put(job)


async def poll_updates(api: BotAPI) -> None:
    offset = 0
    while True:
        updates = await api.get_updates(offset=offset, timeout=60)
        for update in updates:
            offset = max(offset, update.get("update_id", 0) + 1)
            message = update.get("message")
            if message:
                await handle_message(api, message)


async def main() -> None:
    if shutil.which("wget") is None:
        raise RuntimeError("wget not found in PATH.")

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=300)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        api = BotAPI(BOT_API_URL, BOT_TOKEN, session)
        for _ in range(MAX_CONCURRENT_DOWNLOADS):
            asyncio.create_task(worker(api))
        print("Bot is running (local Bot API).")
        await poll_updates(api)


if __name__ == "__main__":
    asyncio.run(main())
