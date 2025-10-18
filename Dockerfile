FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install runtime dependencies for building common Python wheels
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml /app/
COPY pebble /app/pebble

RUN pip install --upgrade pip \
    && pip install --no-cache-dir .

COPY config-TWW-S3.yaml /app/
COPY service-account.json /app/

ENTRYPOINT ["pebble"]
