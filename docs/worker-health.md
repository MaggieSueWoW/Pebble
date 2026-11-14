# Worker health monitoring

The main worker loop runs one combined probe at startup after recovery and every
five minutes thereafter, including while idle and between individual jobs. There
is no background heartbeat thread. A blocked main loop cannot keep reporting
success. Normal listener notifications still start jobs immediately; the five-minute
probe also wakes processing when it discovers a queued request.

Each probe reads the oldest queued/running request with `limit(1)`, using the
existing status/requestedAt index, checks MongoDB with `ping` and Redis with
`PING`, and checks request age. On success it writes `bot/status` with a server
timestamp, then pings Healthchecks after Firestore acknowledges that write. An
empty queue is healthy. No WCL or Sheets requests are made by the probe.
This adds about 288 bounded Firestore queries and 288 heartbeat writes per idle
day, plus listener startup/reconnection reads. The website considers the bot
online for ten minutes after its last successful probe. A failed Healthchecks
delivery does not invalidate an otherwise successful local health probe.

## Timing and failure policy

- Runs default to a 120-second deadline covering claim, capture, fetching,
  calculation and publication. Config may lower this deadline but cannot exceed
  120 seconds. Settlement and native/SDK cleanup can outlast it; the deployment's
  existing outer stop grace still applies. Validate a representative cold-cache
  workload before deploying the shorter deadline.
- A queued request is stale at ten minutes old only after the healthy worker has
  observed that same request across at least five minutes and attempted to claim
  it. Probe failures and processing reconnection reset that observation. This
  gives requests left through downtime an opportunity to run after recovery.
  While giving an old request that opportunity, success is withheld too: repeated
  crashes before claiming must not renew health on every restart. Once that work
  clears, the next passing probe can report success.
- A running request is stale at three minutes after `startedAt`. A missing or
  invalid request timestamp also prevents a healthy result. Ages compare
  Firestore timestamps with the snapshot's server read time.
- Stale requests send `/fail` immediately when observed by a probe and do not
  renew the site heartbeat. Detection runs at the five-minute probe cadence;
  the three-minute threshold is not a three-minute alert-delivery guarantee.
  The oldest outstanding request is checked first; later requests are considered
  as earlier work clears. The probe never deletes, fails or requeues a request.
- Dependency probe failures withhold success and log only the exception class.
  Healthchecks' missing-ping deadline covers sustained outages, a frozen worker,
  host loss and loss of Internet connectivity.
- Input errors and normally settled failed runs remain visible in the site. They
  do not keep worker availability down until the next weekly successful run.
  Claimed runs are not automatically recomputed: restart/reconnection recovery
  marks interrupted runs failed, and an officer submits a new request.
- Success resumes only after a fresh probe passes, including request-age checks
  and the Firestore write. HTTP delivery is bounded, does not follow redirects,
  sends no request data or diagnostics, and never fails a calculation.

## Healthchecks setup

Create one production worker check in the intended Healthchecks project. Use a
Simple schedule with Period **5 minutes** and Grace **60 minutes**: alerts occur
65 minutes after the last successful ping. Use Grace **55 minutes** if the desired
deadline is exactly 60 minutes since the last success. `/fail` bypasses this grace.
Enable the intended down/recovery notification integration and verify its delivery.

Set `PEBBLE_HEALTHCHECKS_PING_URL` in the worker's private environment to the check's
`https://hc-ping.com/<check-uuid>` URL. Treat it as a secret. No management API key is
needed by the worker. Only the designated production worker should have this
value: development workers must leave it unset. Emulator and `--once` executions
never send Healthchecks signals. With the URL unset, local health checks and site
heartbeats still operate, but external notifications are disabled.

During rollout verify an initial successful probe in both Firestore and
Healthchecks, the next probe after five minutes, and notification delivery using a
separate disposable check. Never use the production ping URL in automated tests.
Do not induce failures by modifying live requests or shutting down shared services.
