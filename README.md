# Pebble

This is a small bot to help our team with tracking bench time. It uses a list of Warcraft Logs reports to figure out who is present when we're fighting mythic bosses, computes time spent on the bench vs. being absent, and lets officers make tweaks to the results as necessary.

The web interface, monorepo layout, Firestore data model, and officer
workflow for reports, overrides, and Run Pebble are described in
[the web app design](docs/web-app-design.md). Current status, spreadsheet parity,
deployment host deployment work, and assignable work packages are tracked in the
[implementation plan](docs/web-app-implementation.md).

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

The current Dockerfile and Compose service run the legacy Sheets loop. Web-worker
packaging and deployment host cutover are outstanding in
[OPS-1](docs/web-app-implementation.md#ops-1-container-and-deployment host-deployment);
the existing image build copies a credential file and must be changed before
building the new web-worker image.

A Docker image is provided for running the loop in a container. Build and run it with:

```bash
docker build -t pebble-loop .
docker run --rm \
  -v "$PWD/config.yaml:/app/config.yaml:ro" \
  -v "$PWD/service-account.json:/app/service-account.json:ro" \
  pebble-loop loop --config /app/config.yaml --interval 300
```

Mount any required credentials (e.g., Google service account JSON) and override the command arguments as needed.
