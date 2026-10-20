# Worker shutdown and recovery (BOT-1)

This contract applies to the BOT-1 feature changes for #6. The deployed worker
remains on the OPS-1 feature revision until a separate reviewed update. No test
in this work requires restarting the legacy Sheets bot or its dependencies.

## Run and publication contract

The main thread alone drains requests serially; Firestore callbacks only wake it.
Before claiming each queued request, the worker rechecks officer access. A shutdown
received during the claim reads leaves that request queued. It records
the input snapshot, versions, digest and engine version, then updates progress
at stage boundaries. It stages output under the run ID and commits the complete
result, public pointer and successful status in one transaction.

Failure recording also uses a transaction and changes only queued/running work.
An ambiguous final-commit response therefore cannot downgrade an already successful
run. Reconnect/startup recovery marks interrupted running requests failed; completed
requests never return to the queue or get computed twice. Queued requests remain
available after downtime. Incomplete uploads remain private and do not move the
previous complete public pointer; retention/cleanup remains a DATA-1 policy.

Transient Firestore, Mongo, Redis and WCL transport/rate-limit/server errors escape
processing to the listener's recovery loop. It backs off from one to 60 seconds,
resolves interrupted claims before draining again, and resumes from the queue's
initial snapshot. SDK stream reconnection also delivers events missed while offline.
An interrupted calculation is marked failed with a rerun instruction; it is not
automatically recomputed. Permanent source/input errors fail the request with a
sanitized actionable message. Cache credential failures identify Redis rather than
misreporting a WCL access problem. Fatal loop errors stop the heartbeat and exit nonzero.

Exceptions are logged by class/run ID, not raw exception text or tracebacks.
Schema errors, private input values, service-response bodies and credential-bearing
URLs are excluded from worker/WCL error diagnostics. The heartbeat/public pointer
never includes private failure details.

## Shutdown and bounds

SIGTERM and SIGINT stop accepting requests and finish the current run. The heartbeat
continues during draining, then its thread is stopped/joined; the listener unsubscribes
and CLI clients close. Idle listener checks use a one-second local wait without
idle queue reads. An outstanding service RPC can lengthen shutdown.

`run_timeout_seconds` in private worker YAML defaults to 1200 (20 minutes), accepts
integers from 1 to 1200, and covers claim, capture, computation and publication. A
POSIX main-thread alarm interrupts blocked Python HTTP operations and retry loops.
Deadline failures are settled like other failed requests. Firestore reads/writes use
10-second RPC and retry budgets; transactions retain the SDK's begin/commit/rollback
timeouts with at most three transaction attempts. Mongo uses a 10-second operation
budget and five-second connect/server selection; Redis sockets use five seconds
without internal retries. WCL retains bounded HTTP attempts under the run deadline.

Failure settlement/SDK cleanup can outlast the calculation deadline. Long native
operations can delay Python signal delivery. Keep Compose's 30-minute stop grace
as the outer operational bound; forced termination is intentionally handled by
next-start recovery, rather than promising that every possible dataset drains.
The production deployment's grace/rate settings are unchanged by these code tests.

The local flock still requires one designated host per project/season and a shared
lock directory for containers. It is not a cross-host lease. Mongo materialization
names now hash **project and season**, so the next deployment rebuilds an isolated
database rather than sharing another project's season materializations. Firestore
inputs/results remain authoritative; this does not migrate or clear Sheets data.

## Reproduce the isolated tests

Supply a Docker context and an absolute path to the Firestore emulator JAR installed
by Firebase Tools. No Firebase account, service-account file or WCL credential is
needed. The tested emulator is `cloud-firestore-emulator-v1.21.0.jar` with Java 21.

```sh
scripts/test-worker-lifecycle.sh "$TEST_DOCKER_CONTEXT" "$FIRESTORE_EMULATOR_JAR"
```

The script builds unique test images from the current Python source and creates
disposable Mongo, Redis, Java emulator and test-runner containers. All services
share a new **network-none** namespace using loopback: there are no published ports,
production networks, host mounts, credential transfers or production project IDs.
It uses synthetic `demo-` projects/seasons and fake WCL HTTP responses. Reduced CPU shares and memory
limits apply to the test containers. Cleanup removes only resources created by
that invocation, including anonymous test volumes and unique test image tags.
Run it serially; do not point other emulator tests at its service endpoints.

For already-provisioned **disposable** loopback services, opt into the Python tests:

```sh
FIRESTORE_EMULATOR_HOST=127.0.0.1:8281 \
PEBBLE_TEST_MONGO_URI=mongodb://127.0.0.1:27017 \
PEBBLE_TEST_REDIS_URL=redis://127.0.0.1:6379/0 \
python -m pytest -q tests/test_web_worker.py tests/test_web_worker_faults.py tests/test_web_worker_lifecycle.py
```

The lifecycle fixture refuses non-loopback endpoints and passes only synthetic
credentials to child workers. TCP proxies cut real service connections; WCL tests
use an in-process fake HTTP server. None of these tests contacts Warcraft Logs.

Coverage includes real idle/mid-run SIGTERM, queued work across restart, same-host
singleton rejection, SIGKILL recovery preserving prior results, HTTP deadline
interruption, Mongo/Redis/WCL outages, Firestore offline startup and missed-event
reconnection, permanent-source failure, and diagnostic redaction. Emulator fault
tests separately cover claim reads/acknowledgements, capture, materialization,
metadata/stage writes, root/batch upload, final commit, failure recording and lost
successful-commit replies. Existing tests retain serial notification processing,
minute heartbeat cadence, stage visibility, input privacy and fatal-thread cleanup.

These checks do not retire the old bot, change S2 editing authority, prove hosted
login/migration parity, verify production backups, or deploy BOT-1 code. Those
rollout decisions remain in #5/#7/#8/#14.
