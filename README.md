# LinkToFile Userbot

Telethon userbot that downloads a link with `wget` and uploads the file back to the sender.

## Setup

Set environment variables:

- `TELETHON_API_ID`
- `TELETHON_API_HASH`
- `TELETHON_BOT_TOKEN`
- `TELETHON_BOT_USERNAME` (optional if the bot has a public username)
- `TELETHON_PHONE` (user account phone number, with country code)
- `TELETHON_PASSWORD` (optional, for 2FA)
- `TELETHON_SESSION` (string session or session file name, default: `userbot`)
- `DOWNLOAD_DIR` (optional, default: `downloads`)
- `BALE_BOT_TOKEN`
- `BALE_API_URL` (optional, default: `https://tapi.bale.ai`)
- `BALE_UPLOAD_TIMEOUT_SECONDS` (optional, default: `1800`)

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
- Bot account receives links; user session uploads to the bot, then the bot relays the file.
