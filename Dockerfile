FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml README.md /app/
COPY pebble /app/pebble

RUN pip install --no-cache-dir . \
    && groupadd --gid 10001 pebble \
    && useradd --uid 10001 --gid pebble --no-create-home pebble

USER pebble
ENTRYPOINT ["pebble"]
