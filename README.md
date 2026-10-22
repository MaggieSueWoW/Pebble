# Pebble

This is a small bot to help our team with tracking bench time. It uses a list of Warcraft Logs reports to figure out who is present when we're fighting mythic bosses, computes time spent on the bench vs. being absent, and lets officers make tweaks to the results as necessary.

The web app's implementation backlog, spreadsheet parity, dependencies, and cutover
checklist live in the web rollout tracker (#14). Architecture decisions
are recorded in stack and workflow (#2),
Firestore and security contracts (#3), and
officer/public UX (#4). These reference issues include design targets;
the tracker records what is implemented and what remains.

Web-app work integrates into `web-app`; keep `main` unchanged until
that work is complete. The existing Pebble/Sheets bot on deployment host must remain
undisturbed during implementation; deployment and cutover remain future work.

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
