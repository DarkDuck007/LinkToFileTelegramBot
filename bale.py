import asyncio
import logging
import time
from pathlib import Path
from typing import Any, Coroutine

import requests

from config import BALE_UPLOAD_TIMEOUT_SECONDS, PROGRESS_INTERVAL_SECONDS
from safety import ascii_filename


logger = logging.getLogger("linktofile")


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
        try:
            resp = requests.post(url, data=data, files=files, timeout=timeout)
        except requests.RequestException as exc:
            raise RuntimeError(f"{method} request failed: {exc}") from exc
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
                files = {"document": (safe_name, handle, "application/octet-stream")}
                return self._request_sync(
                    "sendDocument",
                    data=data,
                    files=files,
                    timeout=(10, BALE_UPLOAD_TIMEOUT_SECONDS),
                )

        return await asyncio.to_thread(_upload)

    async def send_photo(
        self,
        chat_id: int,
        caption: str | None,
        file_id: str | None = None,
        file_path: Path | None = None,
        filename: str | None = None,
    ) -> dict:
        if file_id:
            data = {"chat_id": str(chat_id), "photo": file_id}
            if caption:
                data["caption"] = caption
            return await self.request("sendPhoto", data=data, timeout=60)
        if not file_path:
            raise ValueError("file_path is required when no file_id is provided")

        def _upload() -> dict:
            data = {"chat_id": str(chat_id)}
            if caption:
                data["caption"] = caption
            safe_name = ascii_filename(filename or file_path.name)
            with file_path.open("rb") as handle:
                files = {"photo": (safe_name, handle, "application/octet-stream")}
                return self._request_sync(
                    "sendPhoto",
                    data=data,
                    files=files,
                    timeout=(10, BALE_UPLOAD_TIMEOUT_SECONDS),
                )

        return await asyncio.to_thread(_upload)

    async def get_file(self, file_id: str) -> dict:
        return await self.request("getFile", data={"file_id": file_id})

    def file_url(self, file_path: str) -> str:
        return f"{self._base_url}/file/bot{self._token}/{file_path}"


def log_bale_task_result(task: asyncio.Task) -> None:
    try:
        task.result()
    except asyncio.CancelledError:
        return
    except Exception as exc:
        logger.exception("Bale background task failed: %s", exc)


def spawn_bale_task(coro: Coroutine[Any, Any, Any], label: str) -> None:
    task = asyncio.create_task(coro, name=label)
    task.add_done_callback(log_bale_task_result)


async def prime_bale_offset(api: BaleApi) -> int:
    try:
        updates = await api.get_updates(0, timeout_seconds=1)
    except Exception as exc:
        logger.warning("Could not prime Bale offset: %s", exc)
        return 0
    if not updates:
        return 0
    return max(update.get("update_id", 0) for update in updates) + 1
