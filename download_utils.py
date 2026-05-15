import asyncio
import hashlib
import logging
import mimetypes
import re
import time
import urllib.parse
import uuid
import zipfile
from pathlib import Path
from typing import Any

from config import MAX_UPLOAD_BYTES, PROGRESS_INTERVAL_SECONDS, SIZE_LIMIT_EXCEEDED
from safety import filename_from_header, filename_from_url, sanitize_filename


logger = logging.getLogger("linktofile")


CONTENT_TYPE_EXTENSIONS = {
    "application/vnd.android.package-archive": ".apk",
    "application/x-msdownload": ".exe",
    "application/x-msdos-program": ".exe",
    "application/vnd.microsoft.portable-executable": ".exe",
    "application/pdf": ".pdf",
    "application/zip": ".zip",
    "application/x-zip-compressed": ".zip",
    "application/vnd.android.apks": ".apks",
    "application/json": ".json",
    "text/plain": ".txt",
}

QUERY_FILENAME_KEYS = (
    "filename",
    "file_name",
    "file",
    "name",
    "download",
    "response-content-disposition",
)


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
        "--max-redirect=20",
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
    header_name = None
    header_lines = [
        line for line in headers.splitlines() if "content-disposition" in line.lower()
    ]
    for line in reversed(header_lines):
        value = line.split(":", 1)[-1].strip()
        name = filename_from_header(value)
        if name:
            header_name = name
            break
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
    return header_name, content_type, content_length


def extension_from_content_type(content_type: str | None) -> str:
    if not content_type:
        return ""
    media_type = content_type.split(";", 1)[0].strip().lower()
    if not media_type:
        return ""
    if media_type in CONTENT_TYPE_EXTENSIONS:
        return CONTENT_TYPE_EXTENSIONS[media_type]
    extension = mimetypes.guess_extension(media_type) or ""
    if extension == ".jpe":
        return ".jpg"
    return extension


def apply_content_extension(name: str, content_type: str | None) -> str:
    if not name:
        return name
    if Path(name).suffix:
        return name
    extension = extension_from_content_type(content_type)
    if not extension:
        return name
    return f"{name}{extension}"


def apply_html_extension(name: str, content_type: str | None) -> str:
    return apply_content_extension(name, content_type)


def filename_from_query(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    query_items = urllib.parse.parse_qsl(parsed.query, keep_blank_values=False)
    fallback_name = ""
    for key, value in query_items:
        lowered_key = key.lower()
        if lowered_key not in QUERY_FILENAME_KEYS:
            continue
        if lowered_key == "response-content-disposition":
            name = filename_from_header(value)
        else:
            name = sanitize_filename(Path(urllib.parse.unquote(value)).name)
        if name and Path(name).suffix:
            return name
        if (
            name
            and not fallback_name
            and not (lowered_key == "download" and name.isdigit())
        ):
            fallback_name = name
    return fallback_name


def choose_download_filename(
    url: str, header_name: str | None, content_type: str | None,
    original_url: str | None = None,
) -> str:
    names = [
        sanitize_filename(header_name) if header_name else "",
        filename_from_query(url),
        filename_from_url(url),
    ]
    if original_url and original_url != url:
        names.insert(2, filename_from_query(original_url))
        names.insert(3, filename_from_url(original_url))
    for name in names:
        if name and Path(name).suffix:
            return name
    for name in names:
        if name:
            return apply_content_extension(name, content_type)
    return apply_content_extension("untitled", content_type)


def shorten_filename(name: str, max_length: int = 64) -> str:
    if len(name) <= max_length:
        return name
    path = Path(name)
    suffix = path.suffix
    if len(suffix) >= max_length:
        return f"{uuid.uuid4().hex[:max_length]}"
    return f"{uuid.uuid4().hex}{suffix}"


def extension_from_file(path: Path) -> str:
    try:
        with path.open("rb") as handle:
            magic = handle.read(8)
    except OSError:
        return ""
    if magic.startswith(b"%PDF-"):
        return ".pdf"
    if magic.startswith(b"MZ"):
        return ".exe"
    if magic.startswith(b"PK\x03\x04") or magic.startswith(b"PK\x05\x06"):
        try:
            with zipfile.ZipFile(path) as archive:
                names = set(archive.namelist())
        except zipfile.BadZipFile:
            return ".zip"
        if "AndroidManifest.xml" in names:
            return ".apk"
        if any(name.endswith(".apk") for name in names):
            return ".apks"
        return ".zip"
    if magic.startswith(b"\x1f\x8b"):
        return ".gz"
    if magic.startswith(b"Rar!\x1a\x07"):
        return ".rar"
    if magic.startswith(b"7z\xbc\xaf\x27\x1c"):
        return ".7z"
    return ""


def improve_filename_from_file(name: str, path: Path) -> str:
    suffix = Path(name).suffix.lower()
    if suffix and suffix not in {".bin", ".tmp", ".download"}:
        return name
    detected_extension = extension_from_file(path)
    if not detected_extension:
        return name
    if suffix:
        return f"{Path(name).stem}{detected_extension}"
    return f"{name}{detected_extension}"


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
        "--max-redirect=20",
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
