# Legacy Sheets bot. The Firestore worker uses Dockerfile.web instead.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md /app/
COPY pebble /app/pebble

RUN pip install --upgrade pip \
    && pip install --no-cache-dir .

# Preserve the legacy image's existing configuration/Sheets credential contract.
COPY config-*.yaml /app/
COPY service-account.json /app/

ENTRYPOINT ["pebble"]
