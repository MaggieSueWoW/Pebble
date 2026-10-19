# Isolated web worker deployment (OPS-1)

Implementation PRs target `codex/pebble-web-app`. Keep `main` and the existing
`pebble-loop` container, image, credentials, networks, Mongo database and Redis
keys unchanged. This moves the web worker; it does not retire the Sheets bot.

## Runtime and credentials

Use `docker-compose.web.yaml` exclusively, project `pebble-web`, container
`pebble-web-worker`, and commit-tagged `pebble-web:<full-commit>` images. Never
run the legacy Compose file, `down`, `--remove-orphans`, or dependency updates.
The build context allows only Python source, metadata and README; configs,
credentials, frontend, tests and virtualenvs are excluded. Docker builds on the
selected remote context, so no Mac/ARM image transfer is needed.

Create a private directory on the Linux Docker host (not just on the Mac), with
mode 700 and a writable `locks` subdirectory. Copy an edited
`deploy/config-web.yaml.example` there as `config.yaml` and the Firestore
service-account JSON as `firestore.json`, both mode 600. Use your private transfer
mechanism, such as `scp`; never print secrets. Bind sources must exist on the
remote Docker host; Compose refuses to create missing source paths.

Copy `deploy/web.env.example` to ignored `deploy/private/deploy.env` on the Mac.
Set the UID/GID to the owner of the private host files (a non-root identity).
Put only `WCL_CLIENT_ID` and `WCL_CLIENT_SECRET` into ignored
`deploy/private/wcl.env` (mode 600). `PEBBLE_WEB_ENV_FILE` is that absolute
**Mac-side** path; Compose sends its values as container environment. Do not reuse
the legacy `.env`, which may override isolation config. Do not set
`FIRESTORE_EMULATOR_HOST` for deployment. Docker administrators can inspect env
values, as in the existing private deployment mechanism.

The worker requires its Firestore `roles/datastore.user` account and WCL secrets,
not Sheets access. Retain the old Sheets account while the legacy bot or imports
need it. Removing its IAM/key is a separate retirement decision.

Host networking reaches existing published Mongo/Redis at 127.0.0.1 without
changing either dependency. Preserve any required authentication in private URI
config. Use `mongo.db: pebble_feature` and a project/season-specific
`pebble:web:...:wcl:` Redis prefix. The worker adds its web suffix to the Mongo
base. Never clear databases or flush Redis during deployment.

Mongo is a rebuildable materialization, Redis a disposable WCL cache; Firestore
holds authoritative web inputs, run history and published results. Existing host
Mongo storage and Redis persistence are reused unchanged. Maintain host backups,
verify Firestore backup/export policy separately, and keep protected recovery
copies of private configuration and credentials. Containers are not backups.

## Build, check, handoff and update

Select the intended Docker context explicitly without changing the global
selection:

```sh
export DOCKER_CONTEXT=YOUR_DEPLOYMENT_CONTEXT
scripts/web-worker.sh deploy/private/deploy.env build
# Set PEBBLE_WEB_IMAGE in deploy.env to pebble-web:<git rev-parse HEAD>.
scripts/web-worker.sh deploy/private/deploy.env check
```

Build requires a clean committed checkout. `check` starts a temporary dependency
probe, with no heartbeat, claim or recovery. It checks Mongo, Redis, Firestore
season read access, WCL secret presence, mounted file permissions and writable
locks. It does not prove WCL report access or a successful calculation.

Before initial start, confirm the Mac worker command/cwd and inspect Firestore
for active runs. SIGTERM that exact worker PID and wait for it to exit. Do not
kill unrelated Python processes. No other host may run a worker for the same
project/season: startup recovery can mark another worker's in-flight request
failed. The heartbeat is project-global; coordinate different seasons too.

The shared host lock directory is mounted as TMPDIR. It supplements operational
singleton control but cannot lock the Mac or a different Docker host. The fixed
container name prevents accidental local scaling. Keep the designated host as
sole owner. Only acknowledge singleton ownership after verifying other workers
have stopped:

```sh
export PEBBLE_WEB_SINGLETON_CONFIRMED=yes
scripts/web-worker.sh deploy/private/deploy.env update
scripts/web-worker.sh deploy/private/deploy.env status
scripts/web-worker.sh deploy/private/deploy.env logs
```

For updates, build the next commit, change the image tag, run `check`, then
`update`. Updates stop the old web container before starting its replacement.
Avoid updates during active runs. SIGTERM finishes the current computation;
idle exit can take ten seconds. Compose allows 30 minutes before SIGKILL. This
is an operational bound, not a guarantee for arbitrary report sets. Forced
termination causes startup recovery to mark interrupted work failed; retry in
the UI. Logs rotate at 10 MiB, three files. Restart policy is unless-stopped.

## Acceptance and rollback

Record image revision, dependency checks, credential permissions, process
ownership, idle minute heartbeat and idle restart. Also verify a real run's live
stages and complete public pointer, failed run preserving prior public results,
queued requests after downtime, and no duplicate workers. Coordinate real
success/failure fixtures with BOT-1/DATA-1/AUTH-1; do not alter live raid inputs
merely to manufacture failures. Dependency checks alone are not full acceptance.

```sh
scripts/web-worker.sh deploy/private/deploy.env stop
# Restore PEBBLE_WEB_IMAGE to the previous retained commit in deploy.env.
scripts/web-worker.sh deploy/private/deploy.env check
PEBBLE_WEB_SINGLETON_CONFIRMED=yes scripts/web-worker.sh deploy/private/deploy.env update
```

To return ownership to the Mac, stop the container, verify exit, then start one
Mac worker with its original config. Don't remove prior materializations or
images during rollback. Sheets remains running throughout; resuming Sheets as
editing authority requires no service restart. Officers must choose one S2
editing authority and reconcile web-only reports, roster, availability, night
overrides and calculation settings before switching. Do not overwrite web edits
with another import or assume Sheets received them. Old bot/credential retirement
awaits the agreed cutover decision.

## Deployment verification — October 19, 2026

The isolated worker is running from image
`pebble-web:9ef9a1d3074833665f3dcea95e778d58d63087e6`. Later documentation-only
commits do not change that deployed Python source. The verified Mac worker exited
before the container started. Remote runtime checks passed for Mongo, Redis,
Firestore, read-only credential/config mounts and shared-lock permissions. The
image default is UID 10001; deployment uses the private files' non-root owner.

A real Run Pebble request was queued through the officer UI while the new
container was stopped. After start it showed Fetching reports progress and
succeeded at 4:15 PM Pacific, publishing a complete result. Run ID:
`fd6e0052-d778-4774-866d-92dbbdffcff3`. Idle heartbeats advanced by 60.066 seconds
after restart; a direct container-side read confirmed freshness and no active run.
The legacy container ID and original start time were unchanged. The Python suite
passed 213 tests with two emulator-dependent skips; Compose validation, shell
syntax and image config/credential exclusion checks also passed.

OPS-1 remains open for the broader BOT-1 fault/forced-shutdown and DATA-1/AUTH-1
acceptance dependencies. No live inputs were changed to induce failures, no old
service account was removed, and Sheets retirement remains a separate decision.
