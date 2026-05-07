#!/usr/bin/env python3
import argparse
import sqlite3
import time
from pathlib import Path


USER_ID_COLUMNS = {
    "user_id",
    "tg_user_id",
    "telegram_user_id",
    "sender_id",
}


def utc_ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def ensure_registered_users_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS registered_users ("
        "platform TEXT NOT NULL,"
        "user_id INTEGER NOT NULL,"
        "chat_id INTEGER,"
        "username TEXT,"
        "first_seen_at TEXT NOT NULL,"
        "last_seen_at TEXT NOT NULL,"
        "auto_enabled INTEGER NOT NULL DEFAULT 0,"
        "PRIMARY KEY(platform, user_id))"
    )
    conn.commit()


def parse_id_list(raw: str | None) -> set[int]:
    ids: set[int] = set()
    if not raw:
        return ids
    for item in raw.replace("\n", ",").split(","):
        item = item.strip()
        if not item:
            continue
        try:
            value = int(item)
        except ValueError:
            print(f"Skipping non-integer list item: {item}")
            continue
        if value > 0:
            ids.add(value)
    return ids


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    cursor = conn.execute(f"PRAGMA table_info({quote_ident(table)})")
    return [str(row[1]) for row in cursor.fetchall()]


def collect_ids_from_db(path: Path) -> set[int]:
    ids: set[int] = set()
    if not path.exists():
        print(f"Skipping missing DB: {path}")
        return ids
    try:
        conn = sqlite3.connect(path)
    except sqlite3.Error as exc:
        print(f"Skipping unreadable DB {path}: {exc}")
        return ids
    try:
        tables = [
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        ]
        for table in tables:
            columns = table_columns(conn, table)
            user_columns = [column for column in columns if column in USER_ID_COLUMNS]
            for column in user_columns:
                try:
                    cursor = conn.execute(
                        f"SELECT DISTINCT {quote_ident(column)} FROM {quote_ident(table)} "
                        f"WHERE {quote_ident(column)} IS NOT NULL"
                    )
                except sqlite3.Error as exc:
                    print(f"Skipping {path}:{table}.{column}: {exc}")
                    continue
                for (value,) in cursor.fetchall():
                    try:
                        user_id = int(value)
                    except (TypeError, ValueError):
                        continue
                    if user_id > 0:
                        ids.add(user_id)
                print(f"Scanned {path}:{table}.{column}")
    finally:
        conn.close()
    return ids


def insert_registered_users(path: Path, ids: set[int]) -> int:
    conn = sqlite3.connect(path)
    try:
        ensure_registered_users_schema(conn)
        now = utc_ts()
        inserted = 0
        for user_id in sorted(ids):
            cursor = conn.execute(
                "INSERT INTO registered_users("
                "platform, user_id, chat_id, username, first_seen_at, last_seen_at, auto_enabled"
                ") VALUES('telegram', ?, ?, NULL, ?, ?, 0) "
                "ON CONFLICT(platform, user_id) DO UPDATE SET "
                "chat_id=COALESCE(registered_users.chat_id, excluded.chat_id), "
                "last_seen_at=registered_users.last_seen_at",
                (user_id, user_id, now, now),
            )
            if cursor.rowcount:
                inserted += 1
        conn.commit()
        return inserted
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Seed moderation registered_users with Telegram user IDs."
    )
    parser.add_argument(
        "--moderation-db",
        default="moderation.db",
        help="Path to moderation DB containing registered_users.",
    )
    parser.add_argument(
        "--source-db",
        action="append",
        default=[],
        help="Source SQLite DB to scan. Can be passed multiple times.",
    )
    parser.add_argument(
        "--ids",
        default="",
        help="Comma-separated Telegram user IDs to add.",
    )
    args = parser.parse_args()

    ids = parse_id_list(args.ids)
    for source in args.source_db:
        ids.update(collect_ids_from_db(Path(source)))

    if not ids:
        print("No Telegram user IDs found.")
        return

    inserted = insert_registered_users(Path(args.moderation_db), ids)
    print(
        f"Registered {len(ids)} unique Telegram user IDs in {args.moderation_db} "
        f"({inserted} rows inserted/updated)."
    )


if __name__ == "__main__":
    main()
