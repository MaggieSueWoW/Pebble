#!/usr/bin/env bash
# All operations target only the isolated web service. Never run legacy Compose.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ $# != 2 ]]; then
  echo "Usage: $0 PRIVATE_DEPLOY_ENV {build|check|update|stop|status|logs}" >&2
  exit 2
fi
deploy_env=$1
action=$2
compose=(docker compose --env-file "$deploy_env" -f docker-compose.web.yaml)
case "$action" in
  build)
    if [[ -n $(git status --porcelain --untracked-files=normal) ]]; then
      echo "Commit changes first so the image tag identifies its source." >&2
      exit 1
    fi
    revision=$(git rev-parse HEAD)
    docker build --platform linux/amd64 --label "org.opencontainers.image.revision=$revision" \
      -t "pebble-web:$revision" .
    ;;
  check)
    # No worker, heartbeat, recovery or service writes.
    "${compose[@]}" run --rm --no-deps -T --entrypoint python web-worker - <<'PY'
import os
from pathlib import Path
from pymongo import MongoClient
from redis import Redis
from pebble.web_worker import firestore_client, load_worker_config

cfg = load_worker_config('/run/pebble/config.yaml')
assert cfg.mongo.db.startswith('pebble_feature'), 'Use an isolated Mongo base'
assert cfg.redis.key_prefix.startswith('pebble:web:'), 'Use an isolated cache prefix'
assert cfg.wcl.client_id and cfg.wcl.client_secret, 'Missing WCL secrets'
with MongoClient(cfg.mongo.uri, serverSelectionTimeoutMS=5000) as mongo:
    mongo.admin.command('ping')
Redis.from_url(cfg.redis.url, socket_timeout=5).ping()
client = firestore_client(cfg.firestore.project_id, cfg.firestore.credentials_file)
assert client.collection('seasons').document(cfg.firestore.season_id).get(timeout=15).exists
client.close()
lock_dir = Path(os.environ['TMPDIR'])
probe = lock_dir / 'permission-check'
probe.write_text('ok')
probe.unlink()
print('Read-only dependency/auth checks and runtime permissions passed')
PY
    ;;
  update)
    if [[ ${PEBBLE_WEB_SINGLETON_CONFIRMED:-} != yes ]]; then
      echo "Stop every other web worker for this project/season, then set PEBBLE_WEB_SINGLETON_CONFIRMED=yes." >&2
      exit 1
    fi
    "${compose[@]}" stop web-worker
    "${compose[@]}" up -d --no-deps --no-build --pull never web-worker
    ;;
  stop) "${compose[@]}" stop web-worker ;;
  status) "${compose[@]}" ps web-worker ;;
  logs) "${compose[@]}" logs --tail 100 web-worker ;;
  *) echo "Unknown action: $action" >&2; exit 2 ;;
esac
