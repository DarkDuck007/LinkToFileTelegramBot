import asyncio
import hashlib
import logging
import re
import time
import zipfile
from pathlib import Path
from typing import Any

from config import MAX_UPLOAD_BYTES, PROGRESS_INTERVAL_SECONDS, SIZE_LIMIT_EXCEEDED
from safety import filename_from_header


logger = logging.getLogger("linktofile")


def should_zip(path: Path) -> bool:
    suffix = path.suffix.lower()
    return suffix in {".apk", ".exe", ".apks"}


def create_zip(source_path: Path, zip_path: Path) -> None:
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


async def probe_response_meta(url: str) -> tuple[str | None, str | None, int | None]:
    process = await asyncio.create_subprocess_exec(
        "wget",
        "--server-response",
        "--spider",
        "--max-redirect=0",
        "--timeout=10",
        "--tries=2",
        "--",
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
    url: str, output_path: Path, editor: Any
) -> int:
    process = await asyncio.create_subprocess_exec(
        "wget",
        "--progress=dot:mega",
        "--timeout=10",
        "--tries=2",
        "-O",
        str(output_path),
        "--max-redirect=0",
        "--",
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


def make_hash_caption(filename: str | None, file_hash: str) -> str:
    if filename:
        return f"Uploaded: {filename}\nHash: {file_hash}"
    return f"Hash: {file_hash}"
