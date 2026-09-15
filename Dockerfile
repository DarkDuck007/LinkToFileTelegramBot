FROM python:3.12-slim AS base

RUN apt-get update && \
    apt-get install -y --no-install-recommends wget && \
    rm -rf /var/lib/apt/lists/*

ARG APP_UID=1000
ARG APP_GID=1000

RUN groupadd -g "$APP_GID" botuser && \
    useradd -u "$APP_UID" -g botuser -m botuser

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py config.py models.py safety.py text_commands.py download_utils.py bale.py ./

RUN mkdir -p /data/downloads && chown -R botuser:botuser /app /data

USER botuser

ENV DOWNLOAD_DIR=/data/downloads \
    HASH_DB_PATH=/data/hash_cache.db \
    MODERATION_DB_PATH=/data/moderation.db \
    TELETHON_SESSION=/data/userbot \
    TELETHON_BOT_SESSION=/data/bot

CMD ["python", "-u", "bot.py"]
