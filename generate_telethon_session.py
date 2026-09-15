"""Generate a Telethon session file on the host machine.

Example:

    TELETHON_API_ID=123 TELETHON_API_HASH=abc TELETHON_PHONE=+15551234567 \
      python generate_telethon_session.py --session /path/to/data/userbot
      
"""

from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path


def env_value(name: str) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return None
    value = value.strip()
    return value or None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Log in to Telegram and create a Telethon session file on disk."
    )
    parser.add_argument(
        "--session",
        default=env_value("TELETHON_SESSION") or "userbot",
        help=(
            "Session name or file path. Use an absolute path if you want the session "
            "written into a specific host directory."
        ),
    )
    parser.add_argument(
        "--api-id",
        type=int,
        default=int(env_value("TELETHON_API_ID") or "0"),
        help="Telegram API ID (or set TELETHON_API_ID).",
    )
    parser.add_argument(
        "--api-hash",
        default=env_value("TELETHON_API_HASH"),
        help="Telegram API hash (or set TELETHON_API_HASH).",
    )
    parser.add_argument(
        "--phone",
        default=env_value("TELETHON_PHONE"),
        help="Phone number with country code (or set TELETHON_PHONE).",
    )
    parser.add_argument(
        "--password",
        default=env_value("TELETHON_PASSWORD"),
        help="2FA password if the account has one (or set TELETHON_PASSWORD).",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()

    try:
        from telethon import TelegramClient
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "Telethon is not installed. Run `pip install -r requirements.txt` first."
        ) from exc

    if args.api_id <= 0:
        raise SystemExit("TELETHON_API_ID is required.")
    if not args.api_hash:
        raise SystemExit("TELETHON_API_HASH is required.")
    if not args.phone:
        raise SystemExit("TELETHON_PHONE is required.")

    session_name = args.session
    session_path = Path(session_name)
    if session_path.suffix == ".session":
        session_path = session_path.with_suffix("")
        session_name = str(session_path)
    if session_path.parent != Path("."):
        session_path.parent.mkdir(parents=True, exist_ok=True)

    client = TelegramClient(session_name, args.api_id, args.api_hash)
    await client.start(phone=args.phone, password=args.password)
    me = await client.get_me()
    username = f"@{me.username}" if getattr(me, "username", None) else str(me.id)
    print(f"Session saved to {session_name}.session")
    print(f"Logged in as {username}")
    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())