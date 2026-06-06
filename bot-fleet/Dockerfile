# syntax=docker/dockerfile:1

FROM python:3.12-alpine

RUN addgroup -S -g 10002 bot_group \
    && adduser -S -D -H -u 10002 -G bot_group bot_user

WORKDIR /app

COPY bot-fleet/main.py /app/main.py

RUN chmod 555 /app/main.py \
    && chown -R bot_user:bot_group /app

USER 10002:10002

ENTRYPOINT ["python3", "/app/main.py"]
