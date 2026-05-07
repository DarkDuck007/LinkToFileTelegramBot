# Bot Anti-Abuse and Architecture Upgrade Plan

This document outlines the planned upgrades to transition the bot into a robust, multi-class architecture with a unified pipeline for handling requests across both Telegram and Bale platforms. It addresses each of the anti-abuse requirements and provides a scalable foundation for future improvements.

## Proposed Architecture

To make the system maintainable, we will restructure `bot.py` into a modular, object-oriented package with the following core components:

*   **`CorePipeline`**: A unified request processing class. Both the Telegram client and Bale poller will translate their native events into a standard `Request` object and feed it to the `CorePipeline`. This allows all anti-abuse logic, link downloading, hashing, and database lookups to run uniformly regardless of the origin platform.
*   **`PlatformAdapter`**: Abstract base class with implementations `TelegramAdapter` and `BaleAdapter`. These handle platform-specific API calls (sending messages, sending documents, etc.) using a common interface.
*   **`DatabaseManager`**: A centralized class managing SQLite connections, schema migrations, and queries.
*   **`ModerationService`**: Handles banning, NSFW detection, and content administration checks before allowing a download or upload to proceed.

---

## Feature Implementation Plan

### 1. Audit Logging Database
We will create a new table (e.g., `download_logs`) to track who is downloading what.
*   **Schema**: `link` (TEXT), `file_hash` (TEXT), `user_id` (TEXT - platform prefixed, e.g., `tg:123`, `bale:456`), `timestamp` (DATETIME).
*   **Primary Key**: Composite key `(link, file_hash, user_id)`.
*   **Indexing**: We will create indexes on `link`, `file_hash`, and `user_id` to allow fast searching by any of these dimensions.

### 2. User Banning System
*   **Schema**: A `banned_users` table with `user_id`, `reason`, `banned_at`, `banned_by_admin_id`.
*   **Pipeline Integration**: The `CorePipeline` will check this table for every incoming request. Banned users will be ignored or receive a specific rejection message.
*   **Commands**: `/ban <user_id> [reason]`, `/unban <user_id>`.

### 3. Global Broadcast (Messaging)
*   **Table**: A `known_users` table to track every user that has interacted with the bot (ID and platform).
*   **Broadcaster Task**: A background asynchronous task that pulls users from the DB and sends messages.
*   **Throttling**: To avoid `429 Too Many Requests`, the broadcaster will use a token bucket or fixed rate limit (e.g., 20-30 messages per second for Telegram, similar for Bale).
*   **Commands**: `/broadcast <platform|both> <message>`.

### 4. NSFW Content Detection
We have a few options for detecting NSFW content. Since we are dealing with files and images:
*   **Option A (Local AI)**: Use a lightweight, local model (like `nsfwjs` ported to Python, or a small ONNX model) to scan image/video thumbnails after download. This is free but requires CPU/RAM resources.
*   **Option B (API Service)**: Integrate a 3rd party NSFW detection API (e.g., Sightengine, AWS Rekognition). Highly accurate but might incur costs depending on volume.
*   **Option C (Heuristics & Hashes)**: Maintain a database of known NSFW file hashes (PhotoDNA or exact SHA256) and ban them instantly upon detection.
*   *Note: We will need to decide which approach fits your hosting environment best.*

### 5. Automated Disk Cleanup
*   **Background Task**: A periodic `asyncio` task that runs every X hours or when a download finishes.
*   **Logic**: It sums the size of files in the `DOWNLOAD_DIR`. If `> 4GB`, it sorts the files by access time (`st_atime` or `st_mtime`) and deletes the oldest files until the total size drops below a safe threshold (e.g., 3GB).

### 6. Administration Methods
*   **In-Chat Admin Commands (Preferred)**: We will define an `ADMIN_IDS` environment variable. Users with these IDs will have access to commands like `/ban`, `/search_hash`, `/stats`, `/broadcast`. This is usually easier to maintain than a separate REST API since it utilizes the existing bot infrastructure.
*   **REST API (Optional)**: If you prefer a web dashboard, we can spin up a lightweight `FastAPI` server on a different port to expose administrative endpoints.

### 7. Easier Content Administration
*   **Inline Admin Menus**: When an admin searches for a file hash or user, the bot will return an Inline Keyboard message with quick actions: `[Ban User]`, `[Block Hash]`, `[Delete File Cache]`.
*   **Blocklist**: We will add a `blocked_hashes` table. If a file resolves to a blocked hash, the bot refuses to upload it and can automatically warn/ban the sender.

### 8. Ban Appeal Menu
*   **Appeal Workflow**: If a banned user sends a message, they receive an inline keyboard button `[Appeal Ban]`.
*   **Appeal Channel/Group**: Clicking the button forwards their appeal (along with their ID and reason) to a designated Admin Group.
*   **Admin Action**: Admins in the group can click `[Approve]` or `[Deny]` on the appeal message to unban or keep them banned instantly.

### 9. Key Creation Limits
*   **Schema**: Add a `keys_created` counter or dynamically count active keys in the `key_cache` table grouped by user.
*   **Limits**: Define a `MAX_KEYS_PER_USER` config.
*   **Commands**: `/mykeys` (lists keys with inline buttons to revoke them), `/revoke <key>`.

### 10. Additional Anti-Abuse Features
*   **Rate Limiting**: Prevent users from spamming the bot with hundreds of links per minute. Implement a strict token-bucket rate limiter per user ID.
*   **Domain Blacklisting**: Block certain domains known for generating abuse (e.g., specific file hosts or adult sites) before even attempting to download.
*   **Shadowbanning**: An option to "shadowban" users where the bot pretends to process their request but never actually uploads the file, wasting their time instead of them creating new accounts immediately.

---

> [!IMPORTANT]
> ## User Review Required
> Please review the architecture and feature plans above. Do these align with your vision for the refactor and upgrades?

> [!WARNING]
> ## Open Questions
> 1. **NSFW Detection Strategy**: Do you prefer a local AI model (requires more CPU/RAM on your server) or a 3rd party API (might have costs)? Or just a manual hash-blocking system based on reports?
> 2. **Administration**: Do you prefer sticking strictly to Telegram in-chat admin commands (with inline buttons), or do you actively want a separate HTTP REST API for a future web dashboard?
> 3. **Database Migration**: Refactoring to a composite PK and tracking platforms might require migrating the existing SQLite data. Are you okay with starting fresh for the new logging tables while keeping the existing file caches?
