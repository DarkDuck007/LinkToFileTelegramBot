import asyncio
import ipaddress
import re
import socket
import urllib.parse
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests

from config import URL_REDIRECT_LIMIT


def has_control_chars(value: str) -> bool:
    return any(ord(ch) < 32 or ord(ch) == 127 for ch in value)


def sanitize_filename(name: str | None) -> str:
    if not name:
        return "file"
    bidi_controls = {
        "\u202a",
        "\u202b",
        "\u202c",
        "\u202d",
        "\u202e",
        "\u2066",
        "\u2067",
        "\u2068",
        "\u2069",
    }
    cleaned = "".join(
        "_" if ord(ch) < 32 or ord(ch) == 127 or ch in bidi_controls else ch
        for ch in name
    )
    cleaned = cleaned.replace("/", "_").replace("\\", "_")
    cleaned = re.sub(r'[<>:"|?*]', "_", cleaned).strip(" .")
    return cleaned or "file"


def filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    basename = Path(unquote(parsed.path)).name
    if not basename:
        return ""
    return sanitize_filename(basename)


def ascii_filename(name: str) -> str:
    if not name:
        return "file"
    sanitized = sanitize_filename(name)
    sanitized = "".join(ch if ord(ch) < 128 else "_" for ch in sanitized)
    sanitized = sanitize_filename(sanitized)
    return sanitized or "file"


def filename_from_header(value: str) -> str | None:
    match = re.search(r"filename\*=([^']*)''([^;]+)", value, flags=re.IGNORECASE)
    if match:
        return sanitize_filename(unquote(match.group(2)))
    match = re.search(r'filename="([^"]+)"', value, flags=re.IGNORECASE)
    if match:
        return sanitize_filename(match.group(1))
    match = re.search(r"filename=([^;]+)", value, flags=re.IGNORECASE)
    if match:
        return sanitize_filename(match.group(1).strip().strip('"'))
    return None


def _is_public_ip(address: str) -> bool:
    try:
        ip = ipaddress.ip_address(address)
    except ValueError:
        return False
    return not (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    )


def validate_public_http_url(url: str) -> str:
    if not url or has_control_chars(url):
        raise ValueError("URL is empty or contains control characters.")
    parsed = urllib.parse.urlsplit(url.strip())
    if parsed.scheme.lower() not in {"http", "https"}:
        raise ValueError("Only http and https URLs are allowed.")
    if not parsed.hostname:
        raise ValueError("URL is missing a hostname.")
    decoded_path_query = urllib.parse.unquote(parsed.path) + urllib.parse.unquote(
        parsed.query
    )
    if has_control_chars(decoded_path_query):
        raise ValueError("URL path or query contains control characters.")
    if parsed.username or parsed.password:
        raise ValueError("URLs with embedded credentials are not allowed.")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("URL contains an invalid port.") from exc
    if port is not None and not 1 <= port <= 65535:
        raise ValueError("URL contains an invalid port.")
    host = parsed.hostname.rstrip(".")
    lookup_port = port or (443 if parsed.scheme == "https" else 80)
    try:
        addresses = socket.getaddrinfo(host, lookup_port)
    except socket.gaierror as exc:
        raise ValueError("URL hostname could not be resolved.") from exc
    resolved_ips = {item[4][0] for item in addresses}
    if not resolved_ips:
        raise ValueError("URL hostname did not resolve to an address.")
    if not all(_is_public_ip(address) for address in resolved_ips):
        raise ValueError("URL resolves to a non-public address.")
    return urllib.parse.urlunsplit(
        (
            parsed.scheme.lower(),
            parsed.netloc,
            parsed.path or "/",
            parsed.query,
            "",
        )
    )


def _resolve_safe_download_url_sync(url: str) -> str:
    current = validate_public_http_url(url)
    session = requests.Session()
    headers = {"User-Agent": "LinkToFileBot/1.0", "Range": "bytes=0-0"}
    for _ in range(URL_REDIRECT_LIMIT + 1):
        with session.get(
            current,
            headers=headers,
            allow_redirects=False,
            stream=True,
            timeout=(5, 10),
        ) as response:
            if response.is_redirect or response.is_permanent_redirect:
                location = response.headers.get("Location")
                if not location:
                    raise ValueError("Redirect response is missing Location header.")
                current = validate_public_http_url(
                    urllib.parse.urljoin(current, location)
                )
                continue
            return current
    raise ValueError("URL has too many redirects.")


async def resolve_safe_download_url(url: str) -> str:
    return await asyncio.to_thread(_resolve_safe_download_url_sync, url)
