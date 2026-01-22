# LinkToFile Userbot

Telethon userbot that downloads a link with `wget` and uploads the file back to the sender.

## Setup

Set environment variables:

- `TELETHON_API_ID`
- `TELETHON_API_HASH`
- `TELETHON_SESSION` (string session or session file name, default: `userbot`)
- `DOWNLOAD_DIR` (optional, default: `downloads`)

Install deps:

```bash
pip install -r requirements.txt
```

Run:

```bash
python bot.py
```

## Notes

- Max 10 concurrent downloads, max 3 pending per user, queue size 100.
- Replies to `ping` or `/ping` with `Pong!`.
