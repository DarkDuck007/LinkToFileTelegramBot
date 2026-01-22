import asyncio
import os
import re
import shutil
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from telethon import TelegramClient, events
from telethon.errors import RPCError


API_ID = int(os.environ["TELETHON_API_ID"])
API_HASH = os.environ["TELETHON_API_HASH"]
SESSION = os.environ.get("TELETHON_SESSION", "userbot")
BOT_TOKEN = os.environ["TELETHON_BOT_TOKEN"]
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


async def run_wget(url: str, output_path: Path) -> int:
    process = await asyncio.create_subprocess_exec(
        "wget",
        "-O",
        str(output_path),
        url,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    return await process.wait()


def bytes_to_mb(value: int) -> float:
    return round(value / (1024 * 1024), 2)


async def download_with_progress(
    url: str, output_path: Path, editor: ThrottledEditor
) -> int:
    process = await asyncio.create_subprocess_exec(
        "wget",
        "-O",
        str(output_path),
        url,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    wait_task = asyncio.create_task(process.wait())
    while not wait_task.done():
        await asyncio.sleep(PROGRESS_INTERVAL_SECONDS)
        size = output_path.stat().st_size if output_path.exists() else 0
        await editor.update(f"Downloading... {bytes_to_mb(size)} MB")
    return await wait_task


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
    output_path = DOWNLOAD_DIR / f"{uuid.uuid4().hex}"
    await editor.update("Starting download...", force=True)
    try:
        async with global_semaphore:
            async with per_user_semaphore[job.user_id]:
                rc = await download_with_progress(job.url, output_path, editor)
                if rc != 0:
                    await editor.update("Download failed.", force=True)
                    return
                await editor.update("Uploading...", force=True)
                await upload_with_progress(user_client, job.chat_id, output_path, editor)
                await editor.update("Done.", force=True)
    except Exception as exc:
        await editor.update(f"Error: {exc}", force=True)
    finally:
        if output_path.exists():
            try:
                output_path.unlink()
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
    await user_client.start()

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
