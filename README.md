# Pebble

This is a small bot to help our team with tracking bench time. It uses a list of Warcraft Logs reports to figure out who is present when we're fighting mythic bosses, computes time spent on the bench vs. being absent, and lets officers make tweaks to the results as necessary.

The web app's implementation backlog, spreadsheet parity, dependencies, and cutover
checklist live in the web rollout tracker (#14). Architecture decisions
are recorded in stack and workflow (#2),
Firestore and security contracts (#3), and
officer/public UX (#4). These reference issues include design targets;
the tracker records what is implemented and what remains.

The legacy Sheets bot and the web worker run independently during the transition.
Keep the existing spreadsheet, legacy runtime configuration, and Sheets credentials
in place. Officers will manually keep reports, roster/main-alt mappings,
availability and night overrides, and calculation settings aligned until the
agreed cutover. Neither tool automatically synchronizes edits to the other.
Merging source does not restart workers or retire the legacy workflow; deployment
and the eventual officer cutover are separate operational actions.

The [web app](apps/web/README.md) in `apps/web` connects directly to Firestore
with Google officer login. A local Python worker handles Run Pebble and publishes
complete results automatically. See [setup instructions](docs/firestore-setup.md).
Standalone sample mode remains available at `?demo=1`.

For a plain-English and formula-backed explanation of the attendance probability tables on the Google Sheet, see [Attendance_Probability.md](Attendance_Probability.md).

ChatGPT generated 95%+ of the code via Codex under my careful guidance. I've reviewed all of it and tested it extensively since TWW S2.

## Continuous processing loop

Use the `loop` command to continuously ingest new data and recompute exports. Each iteration runs the full ingest, compute, and week phases and reloads configuration so changes are picked up without restarting the process.

```bash
pebble loop --config config.yaml --trigger-timeout 180
```

The loop continues until interrupted. Use `--max-errors 0` to keep it running regardless of transient failures.

## Docker

For the isolated Firestore worker, use the [deployment runbook](docs/web-worker-deployment.md)
and `docker-compose.web.yaml`. Its helper explicitly builds `Dockerfile.web`, whose
`Dockerfile.web.dockerignore` includes only Python package inputs. It mounts private
configuration and Firestore credentials at runtime. The legacy `docker-compose.yaml`
is deliberately unchanged; do not use it to deploy the web worker.

The [worker reliability guide](docs/worker-reliability.md) describes shutdown,
recovery, processing deadlines and the disposable lifecycle/fault-test harness.

The root `Dockerfile` and `docker-compose.yaml` retain the legacy Sheets build.
That image includes `config-*.yaml` and `service-account.json`, as required by the
existing Compose command; `.env` remains supplied at runtime. The default build
context allowlist excludes unrelated private deployment files.
Private `config-web*.yaml` files are excluded from the legacy build. Both build
contexts include only Python source under `pebble/`, excluding bytecode and other
local files inside that directory.

Verify both context boundaries against synthetic fixtures with
`PEBBLE_TEST_DOCKER_BUILDS=1 python -m pytest -q tests/test_docker_build_context.py`.
This opt-in check requires Docker with BuildKit and exports scratch build contents
to a temporary directory without starting containers or reading real credentials.

For a separately authorized legacy build, the existing commands remain:

```bash
docker build -t pebble-loop .
docker run --rm \
  -v "$PWD/config.yaml:/app/config.yaml:ro" \
  -v "$PWD/service-account.json:/app/service-account.json:ro" \
  pebble-loop loop --config /app/config.yaml --interval 300
```

Mount any required credentials (e.g., Google service account JSON) and override the command arguments as needed.
The web image runs as UID/GID 10001 by default and contains no runtime configs or
credentials. The legacy image preserves its existing execution identity and file
layout. Neither build replaces a running container; keep production image tags and
services unchanged until a separate deployment is authorized.
