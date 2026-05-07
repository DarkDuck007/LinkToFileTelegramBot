import os
import re
from pathlib import Path


API_ID = int(os.environ["TELETHON_API_ID"])
API_HASH = os.environ["TELETHON_API_HASH"]
SESSION = os.environ.get("TELETHON_SESSION", "userbot")
BOT_SESSION = os.environ.get("TELETHON_BOT_SESSION", "bot")
TELETHON_CONNECTION = os.environ.get("TELETHON_CONNECTION", "tcpabridged").strip().lower()
TELEGRAM_UPLOAD_RETRY_COUNT = int(os.environ.get("TELEGRAM_UPLOAD_RETRY_COUNT", "2"))
TELEGRAM_UPLOAD_TIMEOUT_SECONDS = int(
    os.environ.get("TELEGRAM_UPLOAD_TIMEOUT_SECONDS", "1800")
)
BOT_TOKEN = os.environ["TELETHON_BOT_TOKEN"]
BOT_USERNAME = os.environ.get("TELETHON_BOT_USERNAME")
BOT_API_URL = os.environ.get("BOT_API_URL", "https://api.telegram.org")
USER_PHONE = os.environ["TELETHON_PHONE"]
USER_PASSWORD = os.environ.get("TELETHON_PASSWORD")
DOWNLOAD_DIR = Path(os.environ.get("DOWNLOAD_DIR", "downloads"))
DB_PATH = Path(os.environ.get("HASH_DB_PATH", "hash_cache.db"))
MODERATION_DB_PATH = Path(os.environ.get("MODERATION_DB_PATH", "moderation.db"))
BALE_BOT_TOKEN = os.environ.get("BALE_BOT_TOKEN", "").strip() or None
BALE_API_URL = os.environ.get("BALE_API_URL", "https://tapi.bale.ai")
BALE_MAX_UPLOAD_BYTES = 50 * 1024 * 1024
BALE_ZIP_PART_BYTES = 40 * 1024 * 1024
BALE_UPLOAD_TIMEOUT_SECONDS = int(os.environ.get("BALE_UPLOAD_TIMEOUT_SECONDS", "1800"))
ADMIN_TELEGRAM_IDS = {
    int(value)
    for value in os.environ.get("ADMIN_TELEGRAM_IDS", "").replace(";", ",").split(",")
    if value.strip().isdigit()
}
ADMIN_BALE_IDS = {
    int(value)
    for value in os.environ.get("ADMIN_BALE_IDS", "").replace(";", ",").split(",")
    if value.strip().isdigit()
}
ADMIN_REST_TOKEN = os.environ.get("ADMIN_REST_TOKEN", "").strip() or None
ADMIN_REST_HOST = os.environ.get("ADMIN_REST_HOST", "127.0.0.1")
ADMIN_REST_PORT = int(os.environ.get("ADMIN_REST_PORT", "8080"))
TELEGRAM_BROADCAST_DELAY_SECONDS = float(
    os.environ.get("TELEGRAM_BROADCAST_DELAY_SECONDS", "1.0")
)
BALE_BROADCAST_DELAY_SECONDS = float(
    os.environ.get("BALE_BROADCAST_DELAY_SECONDS", "1.0")
)
BROADCAST_RETRY_COUNT = int(os.environ.get("BROADCAST_RETRY_COUNT", "3"))
DOWNLOAD_DIR_MAX_BYTES = int(os.environ.get("DOWNLOAD_DIR_MAX_BYTES", str(4 * 1024**3)))
DOWNLOAD_CLEANUP_INTERVAL_SECONDS = int(
    os.environ.get("DOWNLOAD_CLEANUP_INTERVAL_SECONDS", "900")
)
MAX_KEYS_PER_USER = int(os.environ.get("MAX_KEYS_PER_USER", "10"))

MAX_CONCURRENT_DOWNLOADS = 10
MAX_PENDING_PER_USER = 3
MAX_QUEUE_SIZE = 100
PROGRESS_INTERVAL_SECONDS = 20
MAX_UPLOAD_BYTES = 2 * 1024 * 1024 * 1024
SIZE_LIMIT_EXCEEDED = 3
URL_REDIRECT_LIMIT = 8
REST_MAX_BODY_BYTES = 256 * 1024
AUTO_UID_LENGTH = 8

URL_RE = re.compile(r"(https?://\S+)")
KEY_RE = re.compile(r"(?i)\bkey:(.+)")
HASH_RE = re.compile(r"(?i)\bhash:\s*(.+)")
