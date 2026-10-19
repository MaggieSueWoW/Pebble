#!/usr/bin/env bash
# Disposable services only: no production mounts, credentials, networks or ports.
set -euo pipefail
if [[ $# != 2 || ! -r $2 ]]; then
  echo "Usage: $0 DOCKER_CONTEXT FIRESTORE_EMULATOR_JAR" >&2
  exit 2
fi
context=$1
emulator_jar=$(cd "$(dirname "$2")" && pwd)/$(basename "$2")
cd "$(dirname "$0")/.."
docker_cmd=(docker --context "$context")
prefix="pebble-bot1-$(date +%s)-$$"
runtime_image="$prefix-runtime:local"
test_image="$prefix-runner:local"
scratch=$(mktemp -d)
created=()
images=()
cleanup() {
  set +e
  for ((index=${#created[@]}-1; index>=0; index--)); do
    "${docker_cmd[@]}" rm -f -v "${created[index]}" >/dev/null
  done
  for ((index=${#images[@]}-1; index>=0; index--)); do
    "${docker_cmd[@]}" image rm "${images[index]}" >/dev/null
  done
  rm -rf "$scratch"
}
trap cleanup EXIT

"${docker_cmd[@]}" build -t "$runtime_image" .
images+=("$runtime_image")
cat > "$scratch/Dockerfile" <<EOF
FROM $runtime_image
USER root
RUN pip install --no-cache-dir pytest freezegun
USER pebble
EOF
"${docker_cmd[@]}" build -t "$test_image" "$scratch"
images+=("$test_image")

mongo=$("${docker_cmd[@]}" create --name "$prefix-mongo" --network none --cpu-shares 256 --memory 512m mongo:latest mongod --bind_ip 127.0.0.1 --wiredTigerCacheSizeGB 0.25 --quiet)
created+=("$mongo")
"${docker_cmd[@]}" start "$mongo" >/dev/null
redis=$("${docker_cmd[@]}" create --name "$prefix-redis" --network "container:$mongo" --cpu-shares 256 --memory 128m redis:latest redis-server --bind 127.0.0.1 --save '' --appendonly no)
created+=("$redis")
emulator=$("${docker_cmd[@]}" create --name "$prefix-firestore" --network "container:$mongo" --cpu-shares 256 --memory 512m eclipse-temurin:21-jre java -Xmx256m -jar /tmp/firestore.jar --host 127.0.0.1 --port 8281 --project_id demo-pebble-bot1)
created+=("$emulator")
"${docker_cmd[@]}" cp "$emulator_jar" "$emulator:/tmp/firestore.jar"
"${docker_cmd[@]}" start "$redis" "$emulator" >/dev/null
runner=$("${docker_cmd[@]}" create --name "$prefix-runner" --network "container:$mongo" \
  --cpu-shares 256 --memory 1g \
  -e FIRESTORE_EMULATOR_HOST=127.0.0.1:8281 \
  -e PEBBLE_TEST_MONGO_URI=mongodb://127.0.0.1:27017 \
  -e PEBBLE_TEST_REDIS_URL=redis://127.0.0.1:6379/0 \
  -e PYTHONPATH=/app --entrypoint python "$test_image" \
  -m pytest -q /tmp/tests/test_web_worker.py /tmp/tests/test_web_worker_faults.py /tmp/tests/test_web_worker_lifecycle.py --tb=short -p no:cacheprovider)
created+=("$runner")
"${docker_cmd[@]}" cp tests "$runner:/tmp/tests"
"${docker_cmd[@]}" start -a "$runner"
exit_code=$("${docker_cmd[@]}" inspect "$runner" --format '{{.State.ExitCode}}')
exit "$exit_code"
