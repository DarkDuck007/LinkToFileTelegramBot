import asyncio
import os
import re
import shutil
import time
import uuid
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
USER_PHONE = os.environ["TELETHON_PHONE"]
USER_PASSWORD = os.environ.get("TELETHON_PASSWORD")
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
    user_client: TelegramClient,
    chat_id: int,
    file_path: Path,
    editor: ThrottledEditor,
) -> None:
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

    await user_client.send_file(
        chat_id,
        file_path,
        caption=f"Uploaded: {file_path.name}",
        progress_callback=progress_callback,
    )


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
                await upload_with_progress(user_client, job.chat_id, target_path, editor)
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

    for _ in range(MAX_CONCURRENT_DOWNLOADS):
        asyncio.create_task(worker(bot_client, user_client))

    @bot_client.on(events.NewMessage(incoming=True))
    async def handler(event: events.NewMessage.Event) -> None:
        text = (event.raw_text or "").strip()
        lowered = text.lower()
        if lowered in {"ping", "/ping"}:
            await event.reply("Pong!")
            return
        url = extract_url(text)
        if not url:
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
