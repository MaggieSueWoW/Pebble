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
compose=(docker compose --project-name pebble-web --env-file "$deploy_env" -f docker-compose.web.yaml)

check_runtime() {
  local image revision metadata
  image=$("${compose[@]}" config --images)
  if [[ ! $image =~ ^pebble-web:([0-9a-f]{40})$ ]]; then
    echo "Select a full commit-tagged pebble-web image before deployment." >&2
    return 1
  fi
  revision=${BASH_REMATCH[1]}
  metadata=$(docker image inspect "$image" --format \
    '{{index .Config.Labels "org.opencontainers.image.revision"}} {{.Os}}/{{.Architecture}}')
  if [[ $metadata != "$revision linux/amd64" ]]; then
    echo "Image source revision or platform does not match the selected release." >&2
    return 1
  fi
  # No worker, heartbeat, recovery or service writes. Never pull a missing image.
  "${compose[@]}" run --rm --no-deps --pull never -T --entrypoint python web-worker - <<'PY'
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
}

case "$action" in
  build)
    if [[ -n $(git status --porcelain --untracked-files=normal) ]]; then
      echo "Commit changes first so the image tag identifies its source." >&2
      exit 1
    fi
    revision=$(git rev-parse HEAD)
    docker build -f Dockerfile.web --platform linux/amd64 --label "org.opencontainers.image.revision=$revision" \
      -t "pebble-web:$revision" .
    ;;
  check) check_runtime ;;
  update)
    if [[ ${PEBBLE_WEB_SINGLETON_CONFIRMED:-} != yes ]]; then
      echo "Stop every other web worker for this project/season, then set PEBBLE_WEB_SINGLETON_CONFIRMED=yes." >&2
      exit 1
    fi
    # A bad release must not take the healthy worker offline.
    check_runtime
    "${compose[@]}" stop web-worker
    "${compose[@]}" up -d --no-deps --no-build --pull never web-worker
    ;;
  stop) "${compose[@]}" stop web-worker ;;
  status) "${compose[@]}" ps web-worker ;;
  logs) "${compose[@]}" logs --tail 100 web-worker ;;
  *) echo "Unknown action: $action" >&2; exit 2 ;;
esac
