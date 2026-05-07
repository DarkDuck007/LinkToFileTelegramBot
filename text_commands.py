from config import HASH_RE, KEY_RE, URL_RE


def blocked_message() -> str:
    return "This request is blocked by the bot's safety policy."


def banned_message(ban: dict | None = None) -> str:
    reason = ban.get("reason") if ban else None
    suffix = f"\nReason: {reason}" if reason else ""
    return f"You are banned from using this bot.{suffix}\nUse /appeal <message> to request review."


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


def extract_key_command(text: str) -> str | None:
    stripped = text.strip()
    if not stripped.lower().startswith("/key"):
        return None
    remainder = stripped[4:]
    if remainder.startswith("@"):
        parts = remainder.split(None, 1)
        remainder = parts[1] if len(parts) > 1 else ""
    key = remainder.lstrip()
    if not key:
        return None
    return key


def extract_hash_query(text: str) -> str | None:
    match = HASH_RE.search(text)
    if not match:
        return None
    value = match.group(1).strip()
    if not value:
        return None
    return value


def parse_auto_command(text: str) -> tuple[str | None, str | None]:
    if not text:
        return None, None
    parts = text.strip().split()
    if not parts:
        return None, None
    if not parts[0].lower().startswith("/auto"):
        return None, None
    if len(parts) == 1:
        return "help", None
    cmd = parts[1].lower()
    arg = parts[2] if len(parts) > 2 else None
    return cmd, arg


def parse_appeal_command(text: str) -> tuple[str | None, str | None]:
    if not text.lower().startswith("/appeal"):
        return None, None
    parts = text.split(maxsplit=1)
    if len(parts) == 1:
        return "help", None
    if parts[1].strip().lower() == "status":
        return "status", None
    return "create", parts[1].strip()


def format_log_rows(rows: list[dict]) -> str:
    if not rows:
        return "No matching logs."
    lines = []
    for row in rows[:20]:
        lines.append(
            f"{row['platform']}:{row['user_id']} hash={row['file_hash']} "
            f"seen={row['times_seen']} status={row['status']} file={row.get('filename') or '-'}"
        )
    return "\n".join(lines)


def format_key_rows(rows: list[dict]) -> str:
    if not rows:
        return "No keys found."
    lines = []
    for row in rows[:50]:
        file_hash = row.get("file_hash") or "-"
        filename = row.get("filename") or "-"
        lines.append(f"{row['key']} | {filename} | {file_hash}")
    return "\n".join(lines)
