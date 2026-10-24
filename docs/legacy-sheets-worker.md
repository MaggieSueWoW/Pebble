# Legacy Sheets worker

Operations guide for the existing Google Sheets bot. For a project overview, see
the [main README](../README.md). For the independent web worker, use the
[web worker deployment guide](web-worker-deployment.md).

The Sheets bot continues to use its existing spreadsheet, configuration, and
credentials during the transition. Officers manually keep reports, roster/main-alt
mappings, availability and night overrides, and calculation settings aligned with
the web app until the agreed cutover. Neither tool automatically synchronizes edits
to the other. Keep the existing worker running until its retirement is explicitly
authorized.

## Continuous processing loop

Use the `loop` command to watch the spreadsheet's trigger checkbox. When checked,
the bot ingests reports, computes nightly and weekly results, and refreshes the
spreadsheet exports. It reloads configuration each iteration so changes are picked
up without restarting the process. A trigger timeout skips that iteration when
the checkbox remains unchecked.

```bash
pebble loop --config config.yaml --trigger-timeout 180
```

The loop continues until interrupted. Use `--max-errors 0` to keep it running
regardless of transient failures.

## Docker

For the isolated Firestore worker, use the [deployment runbook](web-worker-deployment.md)
and `docker-compose.web.yaml`. Its helper explicitly builds `Dockerfile.web`, whose
`Dockerfile.web.dockerignore` includes only Python package inputs. It mounts private
configuration and Firestore credentials at runtime. The legacy `docker-compose.yaml`
is deliberately unchanged; do not use it to deploy the web worker.

The [worker reliability guide](worker-reliability.md) describes shutdown,
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
  --env-file .env \
  -v "$PWD/config.yaml:/app/config.yaml:ro" \
  -v "$PWD/service-account.json:/app/service-account.json:ro" \
  pebble-loop loop --config /app/config.yaml --trigger-timeout 180
```

Mount any required credentials and override the command arguments as needed.
The web image runs as UID/GID 10001 by default and contains no runtime configs or
credentials. The legacy image preserves its existing execution identity and file
layout. Neither build replaces a running container; keep production image tags and
services unchanged until a separate deployment is authorized.
