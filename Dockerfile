FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install runtime dependencies for building common Python wheels
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md /app/
COPY pebble /app/pebble
COPY config.yaml.example /app/
COPY raid_time_fairness.md /app/

RUN pip install --upgrade pip \
    && pip install --no-cache-dir .

ENTRYPOINT ["pebble"]
CMD ["loop"]
