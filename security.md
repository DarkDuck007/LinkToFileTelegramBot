# Security Review

Review target: current `bot.py` after the moderation/admin implementation pass.

## Remediation Status

Started in the current working tree:

- Added strict public `http`/`https` URL validation and manual redirect validation before downloads.
- Disabled `wget` redirects and added `--` before URL arguments.
- Fixed Bale link job attribution to use `sender_id` as the user ID and `chat_id` only as the reply destination.
- Added separate `ADMIN_BALE_IDS`.
- Added REST body size limiting, constant-time bearer token comparison, and stricter enum validation.
- Added shared filename sanitization for control characters, bidi controls, separators, and invalid path characters.
- Split the first low-coupling pieces out of `bot.py`: `config.py`, `models.py`, `bale.py`, `safety.py`, `download_utils.py`, and `text_commands.py`.
- Added admin URL blocklist wildcard support for entries such as `https://somebadsite.com*` and `https://*.somebadsite.com*`.

Still open:

- DNS rebinding remains possible because `wget` resolves the final hostname separately from validation. A stronger follow-up is replacing `wget` with a Python downloader that pins the already-validated resolved address while preserving the original `Host` header.
- REST admin audit logs, rate limits, and destructive-action confirmations are still needed.
- The `bot.py` split is started, but the remaining DB stores, admin/REST handlers, cleanup worker, and platform handlers should still be moved into focused modules.

## Executive Summary

The bot does not currently use `shell=True`, `os.system`, or shell-string command execution. The `wget` calls use `asyncio.create_subprocess_exec(...)` with argument lists, so classic shell metacharacter injection through URLs or filenames is not the main concern.

The higher-risk issues are:

- unrestricted server-side URL fetching (`wget`) with redirects, which creates SSRF risk;
- weak validation around URLs, REST inputs, platform/user IDs, and admin commands;
- a Bale sender attribution bug that can log/enforce against `chat_id` instead of the actual sender;
- a very large `bot.py`, which makes security review and future changes more error-prone.

## Findings

### High: SSRF Through User-Supplied URLs

Relevant code:

- `probe_response_meta(url)` runs `wget --spider --max-redirect=20 ... url`.
- `download_with_progress(url, ...)` runs `wget ... -O output_path url`.
- Telegram and Bale handlers accept any `https?://\S+` URL.

Because the bot is a public service, users can make the server fetch URLs from internal/private networks, cloud metadata services, localhost-only admin panels, or large internal resources. Redirects make this worse: an apparently public URL can redirect to `127.0.0.1`, RFC1918 IPs, link-local addresses, or metadata endpoints.

Current shell-injection exposure is low because `create_subprocess_exec` is used correctly, but network-target injection/SSRF remains high impact.

Recommended fixes:

- Parse and validate URL before both probing and downloading.
- Allow only `http` and `https`.
- Resolve hostname and block private, loopback, link-local, multicast, reserved, and unspecified IP ranges.
- Re-check every redirect target, or stop using `wget` redirects and implement controlled fetching in Python.
- Block well-known metadata addresses such as `169.254.169.254`.
- Add optional domain allowlist/blocklist.
- Consider disabling redirects or limiting them to validated public destinations.

### High: Bale Link Jobs Use `chat_id` As `user_id`

Relevant code:

- In Bale URL handling, `process_bale_link(api, chat_id, chat_id, url, ...)` passes `chat_id` as the `user_id`.
- The actual sender is available as `sender_id`.

In private chats these may be the same, but in groups/channels they can differ. This causes moderation logs, pending counters, quarantine records, and future enforcement to be attributed to the chat rather than the abusive user.

Impact:

- abusive users can be misattributed;
- innocent chat/group IDs can be logged or banned;
- hash-based investigation by user becomes unreliable;
- admin decisions may target the wrong principal.

Recommended fix:

- Pass `sender_id` to `process_bale_link`.
- Keep `chat_id` only as the reply destination.
- Use `UserRef("bale", sender_id, chat_id, username)` consistently.

### High: REST Admin API Needs Stronger Hardening

Relevant code:

- `start_rest_admin_server()`
- `require_rest_auth(request)`

The REST API is token-protected and defaults to `127.0.0.1`, which is a good baseline. However, it is an admin interface that can ban users, block content, read logs, review appeals, delete keys, and broadcast messages. It currently lacks request size limits, rate limiting, structured validation, audit logging per endpoint, and constant-time token comparison.

Recommended fixes:

- Keep `ADMIN_REST_HOST=127.0.0.1` unless behind a trusted reverse proxy.
- Use HTTPS at the reverse proxy if exposed outside localhost.
- Validate all JSON bodies and query params with strict platform/type/action enums.
- Add request body size limits.
- Add admin action audit logs.
- Use `secrets.compare_digest` for bearer-token comparison.
- Return generic auth failures and avoid leaking details.
- Consider disabling REST unless explicitly enabled by `ENABLE_ADMIN_REST=1`.

### Medium: Bale Admin Authorization Reuses Telegram Admin IDs

Relevant code:

- Bale admin check compares `sender_id` to `ADMIN_TELEGRAM_IDS`.

Telegram numeric IDs and Bale numeric IDs are not the same namespace. Reusing `ADMIN_TELEGRAM_IDS` for Bale can deny valid admins or accidentally grant admin rights if a Bale user happens to have the same numeric ID as a Telegram admin.

Recommended fixes:

- Add `ADMIN_BALE_IDS`.
- Check Telegram admins only against Telegram IDs.
- Check Bale admins only against Bale IDs.
- In shared admin logs, always store `(platform, user_id)`.

### Medium: URL Validation Is Too Loose

Relevant code:

- `URL_RE = re.compile(r"(https?://\S+)")`
- `extract_url(text)`
- `normalize_url(url)`

The regex accepts many malformed or surprising URLs. It does not enforce a valid hostname, does not reject embedded credentials, does not reject control characters after percent-decoding, and does not canonicalize ports or IDNA hostnames consistently.

Recommended fixes:

- Replace regex-only validation with `urllib.parse.urlsplit` plus strict validation.
- Reject URLs with username/password components.
- Reject empty hostnames.
- Normalize hostnames with IDNA.
- Reject decoded control characters in paths/query.
- Apply the same canonical URL to logging, blocklist matching, and download execution.

### Medium: User-Controlled Filenames Need Control-Character Sanitization

Relevant code:

- `filename_from_url`
- `filename_from_header`
- `target_name = url_name or header_name or "untitled"`
- `make_hash_caption(filename, file_hash)`

The code removes path separators and many invalid filename characters, which reduces path traversal risk. However, it does not explicitly strip ASCII control characters from filenames after URL/header decoding. A crafted filename containing newlines or bidirectional/control characters could make captions, logs, or admin output misleading.

Recommended fixes:

- Strip all characters with `ord(ch) < 32` and `ord(ch) == 127`.
- Consider replacing Unicode bidi controls and other formatting characters.
- Apply one shared `safe_display_filename` for filesystem names, captions, and admin output.

### Medium: Dynamic SQL Is Currently Controlled But Fragile

Relevant code:

- `mod_search_logs` builds the `WHERE` clause from a controlled `kind`.
- `db_update_key_entry` builds the `SET` clause from controlled field names.

Values are parameterized, so this is not currently SQL injectable through user input. The risk is maintainability: future additions could accidentally put user-controlled strings into the SQL fragments.

Recommended fixes:

- Keep all SQL fragment choices in fixed maps, for example `where_by_kind = {...}`.
- Never concatenate user-provided column, table, sort, or direction values.
- Add tests for admin search inputs containing quotes and SQL metacharacters.

### Medium: Admin Commands Need Safer Parsing And Output Limits

Relevant code:

- `handle_admin_command(text, ...)`

Admin commands currently parse text with simple `split()`. This is fine for basic commands but brittle for URLs, reasons, messages, and future inline actions. Some outputs can become long or include user-controlled values.

Recommended fixes:

- Use a small command parser with explicit arity and quoted arguments.
- Enforce output length limits and chunk long results.
- Escape or neutralize user-controlled strings in admin responses.
- Add confirmation steps for destructive commands like `banhash`.

### Medium: Broadcasts Can Be Abused If Admin Token Or Account Is Compromised

Relevant code:

- `/admin broadcast ...`
- `POST /broadcasts`

Broadcast capability is intentionally powerful. If an admin account/token is compromised, an attacker can message every registered user.

Recommended fixes:

- Require a confirmation step for large broadcasts.
- Add broadcast preview and recipient count.
- Add per-admin audit logs.
- Consider a maximum broadcast length.
- Consider a cooldown between broadcast jobs.

### Low: `wget` Option Injection Looks Unlikely, But Use `--` Anyway

Relevant code:

- `create_subprocess_exec("wget", ..., url)`

The extracted URLs must begin with `http://` or `https://`, so they cannot begin with `-` and become a `wget` option through the normal path. Still, defense-in-depth would add `--` before the URL argument.

Recommended fix:

- Use `..., "--", url` in both `wget` invocations after URL validation.

### Low: Cleanup Task Should Be Careful With Symlinks

Relevant code:

- `enforce_download_dir_limit`
- `path_size`
- `shutil.rmtree(item, ignore_errors=True)`

The cleanup logic operates inside `DOWNLOAD_DIR`, and regular symlink deletion is usually safe because unlinking removes the link rather than the target. Still, public-service file handling should explicitly avoid following symlinks while calculating sizes or deciding whether to recurse.

Recommended fixes:

- Use `Path.is_symlink()` checks.
- Unlink symlinks directly.
- Avoid recursive traversal through symlinked directories.

## Positive Findings

- No `shell=True`, `os.system`, `eval`, or `exec` execution paths were found.
- The `wget` calls use argument arrays, not shell command strings.
- File downloads are written to generated job directories under `DOWNLOAD_DIR`.
- URL/header filenames have path separators and common invalid filename characters replaced before being used as paths.
- SQLite values are mostly parameterized.
- REST admin API defaults to localhost and is disabled when `ADMIN_REST_TOKEN` is unset.

## Architecture Note: Split `bot.py`

Yes, it is better to split the large `bot.py` into multiple smaller files. The first pass now extracts configuration, shared models, Bale API helpers, URL/filename safety helpers, download helpers, and text-command parsing.

This is not only a style issue. It directly affects security:

- reviewability is worse when moderation, downloads, admin commands, REST, Bale polling, Telegram handlers, DB migrations, and cleanup all live in one file;
- cross-platform authorization mistakes become easier, as seen with the Bale/Telegram admin ID overlap;
- duplicated platform flow increases the chance that Telegram and Bale security checks diverge;
- tests are harder to write because importing one helper imports the full bot and external dependencies.

Recommended module layout:

- `config.py`: environment parsing and typed settings.
- `models.py`: `UserRef`, job/request dataclasses, enums.
- `storage/cache_db.py`: existing hash/key/auto-link cache DB.
- `storage/moderation_db.py`: abuse logs, bans, blocklist, quarantine, appeals, broadcasts.
- `services/moderation.py`: ban/block/quarantine decisions.
- `services/downloads.py`: URL validation, probing, downloading, hashing, filename safety.
- `services/cleanup.py`: download directory cleanup.
- `services/broadcast.py`: throttled broadcast worker.
- `admin/commands.py`: shared admin command parser and handlers.
- `admin/rest.py`: REST API.
- `platforms/telegram.py`: Telegram-specific event parsing and send/upload primitives.
- `platforms/bale.py`: Bale-specific polling and send/upload primitives.
- `main.py`: startup and task wiring.

Recommended order:

1. Extract config and models first.
2. Extract DB helpers into cache/moderation storage modules.
3. Extract URL validation/download helpers and add SSRF defenses.
4. Extract admin command handling and REST.
5. Leave platform adapters for last, once shared services are testable.

## Highest Priority Remediation Checklist

1. Add strict URL validation and SSRF protection before any `wget` call.
2. Pass Bale `sender_id` into `process_bale_link` instead of `chat_id`.
3. Split admin IDs into `ADMIN_TELEGRAM_IDS` and `ADMIN_BALE_IDS`.
4. Harden REST API validation, auth comparison, request size, and audit logging.
5. Strip control characters from filenames and admin/user-facing output.
6. Refactor `bot.py` into smaller modules before adding more features.
