# Safer Abuse-Control Plan

## Summary

Build a shared moderation and administration layer used by both Telegram and Bale, while keeping platform-specific code limited to message parsing and send/upload operations. V1 prioritizes reliable abuse logging, manual/admin controls, known-content blocking by hash/link/domain, quarantining suspicious content, global announcements, download-folder cleanup, key limits, and ban appeals.

No existing Telegram/Bale user-facing download behavior should change for non-banned users unless content is blocked or quarantined.

## Key Changes

### Shared core structure

Refactor `bot.py` into a small multi-module project:

- `main.py`: startup, client wiring, task startup.
- `platforms/telegram_adapter.py`: Telegram event parsing, replies, uploads, admin command binding.
- `platforms/bale_adapter.py`: Bale polling, replies, uploads, admin command binding.
- `core/pipeline.py`: shared request pipeline for links, keys, hashes, auto-forwarded files.
- `core/moderation.py`: bans, blocklists, quarantine, appeals, admin search.
- `core/storage.py`: SQLite connection, migrations, repository-style DB functions.
- `core/cleanup.py`: download-folder size enforcement.
- `core/broadcast.py`: global announcement queue and throttled delivery.

Use a shared internal request model:

```python
@dataclass
class UserRef:
    platform: Literal["telegram", "bale"]
    user_id: int
    chat_id: int | None = None
    username: str | None = None


@dataclass
class IncomingRequest:
    source: UserRef
    kind: Literal["link", "file", "key_store", "key_fetch", "hash_fetch", "auto_file"]
    text: str | None
    url: str | None
    file_ref: object | None
```

Both Telegram and Bale adapters convert incoming updates into `IncomingRequest`, then call the same pipeline. Platform adapters expose a common send interface so shared code can reply without duplicating logic.

### Databases and tables

Keep the existing cache DB for operational file caches, but add a separate moderation DB path:

- Env var: `MODERATION_DB_PATH`, default `moderation.db`.

Create tables:

- `abuse_log`
  - Primary key: `(link, file_hash, platform, user_id)`.
  - Columns: `link`, `file_hash`, `platform`, `user_id`, `chat_id`, `username`, `filename`, `file_size`, `content_type`, `first_seen_at`, `last_seen_at`, `times_seen`, `status`.
  - Search indexes on `file_hash`, `link`, `(platform, user_id)`.
- `banned_users`
  - Primary key: `(platform, user_id)`.
  - Columns: `reason`, `created_at`, `created_by_platform`, `created_by_user_id`, `expires_at`, `appeal_allowed`.
- `content_blocklist`
  - Supports blocking by `file_hash`, exact `link`, or domain.
  - Columns: `type`, `value`, `reason`, `created_at`, `created_by`, `action`.
  - `action`: `block`, `quarantine`, or `admin_review`.
- `quarantine`
  - Stores blocked/quarantined events, source user, hash/link, reason, status, and optional local temp path while review is pending.
  - Default action for matched known-bad hash/link is to block delivery and record the event.
- `registered_users`
  - Tracks users who have interacted with or enabled the bot.
  - Columns: `platform`, `user_id`, `chat_id`, `username`, `first_seen_at`, `last_seen_at`, `auto_enabled`.
- `broadcast_jobs` and `broadcast_deliveries`
  - Track global admin messages and per-user delivery state.
- `appeals`
  - Primary key: generated appeal ID.
  - Columns: `platform`, `user_id`, `message`, `status`, `created_at`, `resolved_at`, `admin_note`.
- Extend or replace `key_cache` with ownership fields:
  - `owner_platform`, `owner_user_id`, `created_at`.
  - Add indexes for owner lookup.
  - Enforce max keys per account before creating new keys.

### Moderation pipeline

Every link/file/key/auto-forward request follows this order:

1. Register/update the user in `registered_users`.
2. Check `banned_users`; banned users receive a short ban message and appeal instructions.
3. For links, check blocklist by exact link and domain before download.
4. After download or file receipt, compute SHA-256 and write `abuse_log`.
5. Check `content_blocklist` by hash.
6. If blocked or quarantined, do not upload/relay the file; create a `quarantine` record and notify the user with a neutral message.
7. If clean, continue with existing cache/upload/key/auto-forward behavior.
8. Always clean the job temp directory in `finally`, then let periodic folder cleanup handle leftovers.

For v1 NSFW detection, use hash/link/domain blocklists only. Do not add image/video ML scanning yet. Design `moderation.py` with a `ContentScanner` interface so later local NSFW model scanning can be added without touching Telegram/Bale handlers.

### Admin controls

Implement both Telegram admin commands and a REST API.

Admin authentication:

- Env var `ADMIN_TELEGRAM_IDS`: comma-separated Telegram numeric IDs allowed to run admin commands.
- Env var `ADMIN_REST_TOKEN`: bearer token for REST API.
- Optional env var `ADMIN_REST_HOST`, default `127.0.0.1`.
- Optional env var `ADMIN_REST_PORT`, default `8080`.

Telegram admin commands:

- `/admin ban <platform> <user_id> [reason]`
- `/admin unban <platform> <user_id>`
- `/admin baninfo <platform> <user_id>`
- `/admin search hash <sha256>`
- `/admin search link <url>`
- `/admin search user <platform> <user_id>`
- `/admin block hash <sha256> [reason]`
- `/admin block link <url> [reason]`
- `/admin block domain <domain> [reason]`
- `/admin unblock <hash|link|domain> <value>`
- `/admin quarantine list`
- `/admin quarantine approve <id>`
- `/admin quarantine reject <id>`
- `/admin broadcast <telegram|bale|both> <message>`
- `/admin appeals list`
- `/admin appeals accept <appeal_id>`
- `/admin appeals reject <appeal_id> [note]`
- `/admin keys <platform> <user_id>`
- `/admin keydel <key>`

REST API mirrors the same capabilities:

- `GET /health`
- `GET /logs?hash=&link=&platform=&user_id=`
- `POST /bans`, `DELETE /bans/{platform}/{user_id}`
- `POST /blocklist`, `DELETE /blocklist`
- `GET /quarantine`, `POST /quarantine/{id}/approve`, `POST /quarantine/{id}/reject`
- `POST /broadcasts`
- `GET /appeals`, `POST /appeals/{id}/accept`, `POST /appeals/{id}/reject`
- `GET /keys?platform=&user_id=`, `DELETE /keys/{key}`

Use `aiohttp` for async compatibility unless a later implementation chooses to skip REST for deployment simplicity.

### Broadcast throttling

Broadcasts run as background jobs, not inline admin commands.

Defaults:

- Telegram: 1 message per second.
- Bale: 1 message per second.
- Env vars:
  - `TELEGRAM_BROADCAST_DELAY_SECONDS=1.0`
  - `BALE_BROADCAST_DELAY_SECONDS=1.0`
  - `BROADCAST_BATCH_SIZE=100`
  - `BROADCAST_RETRY_COUNT=3`

Broadcast jobs target users from `registered_users`, filtered by platform and `auto_enabled` when requested. Each delivery is recorded as `pending`, `sent`, or `failed`, with error text.

### Download folder cleanup

Add a periodic cleanup task:

- Env var `DOWNLOAD_DIR_MAX_BYTES`, default `4294967296`.
- Env var `DOWNLOAD_CLEANUP_INTERVAL_SECONDS`, default `900`.

Every interval:

1. Calculate total size of `DOWNLOAD_DIR`.
2. If over limit, sort files and directories by oldest modification time.
3. Delete oldest items until total size is below 4GB.
4. Skip files currently inside active job directories tracked by the pipeline.
5. Log every deletion and total reclaimed bytes.

This complements existing per-job `shutil.rmtree` cleanup and handles leftovers after crashes or failed deletes.

### Key limits and user commands

Add env var:

- `MAX_KEYS_PER_USER`, default `20`.

Behavior:

- Key creation requires owner metadata.
- If a user reaches the limit, reject new key creation with a message telling them to remove old keys.
- Users can manage their own keys on both platforms:
  - `/keys` lists their keys with filename/hash if known.
  - `/keydel <key>` deletes one owned key.
  - `/keyinfo <key>` shows metadata for one owned key.
- Admins can list/delete any user's keys.

### Ban appeals

For banned users:

- Any non-admin command receives the ban notice plus appeal instruction.
- `/appeal <message>` creates or updates an open appeal.
- `/appeal status` shows current appeal state.
- Accepted appeal removes the ban.
- Rejected appeal keeps the ban and stores admin note.

Appeals are available on both Telegram and Bale through the shared pipeline.

### Easier content administration

Add quarantine review and blocklist workflow:

- Admin search results show all users who sent the same hash/link.
- Admin can block a hash directly from search results.
- Admin can ban all users associated with a confirmed abusive hash using `/admin banhash <sha256> [reason]`.
- Admin can export search results as plain text from Telegram or JSON from REST.
- Quarantined content is never auto-forwarded or cached as reusable content until approved.

### Additional anti-abuse features

Add conservative v1 protections:

- Per-user daily download limit: `MAX_DOWNLOADS_PER_USER_PER_DAY`, default `50`.
- Per-user daily bytes limit: `MAX_BYTES_PER_USER_PER_DAY`, default `10GB`.
- Per-domain cooldown after repeated failures.
- Optional domain allowlist/blocklist.
- URL normalization before logging/blocklist matching.
- Log repeated attempts to fetch blocked content.
- Admin-visible abuse score derived from recent blocked/quarantined attempts, not used for auto-ban in v1.
- Preserve existing pending queue limits.

## Test Plan

Add focused tests around shared services and parsing:

- DB migration creates all moderation tables and indexes without deleting existing cache tables.
- `abuse_log` upserts by `(link, file_hash, platform, user_id)` and increments `times_seen`.
- Search by hash, link, and user returns expected cross-platform rows.
- Banned Telegram and Bale users are blocked before download/key/auto-forward work starts.
- Blocked hash/link/domain causes quarantine and prevents upload/relay.
- Broadcast jobs throttle, retry failures, and record delivery status.
- Cleanup deletes oldest leftover files until under `DOWNLOAD_DIR_MAX_BYTES`, while skipping active job paths.
- Key limit prevents new keys after `MAX_KEYS_PER_USER`; `/keys` and `/keydel` only affect owner keys unless admin.
- Appeals can be created, viewed, accepted, and rejected.
- Existing happy paths still work: Telegram link, Bale link, hash fetch, key store/fetch, and auto-forward.

## Assumptions And Defaults

- V1 uses hash/link/domain blocklists for NSFW mitigation, not ML-based NSFW scanning.
- Suspicious or known-blocked content is quarantined/blocked, not auto-banned.
- Admin v1 includes both Telegram admin commands and REST API.
- SQLite remains the storage backend.
- Existing `hash_cache.db` behavior remains intact; moderation data goes into `moderation.db`.
- Telegram numeric IDs and Bale numeric IDs are platform-scoped and must not be treated as globally unique without the platform field.
