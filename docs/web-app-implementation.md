# Pebble web app implementation plan

Updated October 19, 2026. Working branch: `design/pebble-web-app`.

The web app's core workflow works against Firebase with the bot running on the
Mac. The next milestone is a reliable worker on deployment host and enough spreadsheet
parity to make the web app the team's everyday interface. This plan records the
implemented baseline, remaining work, and independently assignable work packages.
It is the implementation backlog; [the design](web-app-design.md) describes the
product and data contracts, and [setup](firestore-setup.md) contains current commands.

## Decisions to preserve

- Keep the monorepo: Python in `pebble/`, React in `apps/web/`, rules in `firebase/`.
- The browser connects directly to Firebase Auth and Firestore. No hosted API.
- Officers save reports/overrides, then click Run Pebble. Success automatically
  updates public results. No night approval, draft review, or publish ceremony.
- Public completed results require no login. Inputs, notes, run history, and
  diagnostics are officer-only. Officer membership is keyed by Firebase Auth UID.
- One worker processes requests serially. Keep the existing calculation rules,
  Pacific dates, Tuesday game weeks, and roster membership semantics.
- Listen for queued requests instead of polling. Idle queue-health checks are
  local. Heartbeat writes occur once per minute, with no extra heartbeat writes
  when a run starts or finishes. The UI's offline threshold is 150 seconds.
- Preserve immediate progress updates: queued, validating, fetching, calculating,
  updating results, then success/failure. Progress writes are independent of the
  heartbeat. Keep previous results visible while work runs and after failures.
- Warnings are informative. Reasons/notes remain optional. Preserve negative
  legacy minute overrides as the explicit `unavailableFor` UI mode.
- Changes to schemas or projections need matching rules, adapters, and tests.
  Do not add event buses, distributed leases, approval workflows, or generic
  administration frameworks for this single-worker V1.

## Current implementation and environment

| Area | Implemented baseline | Remaining limitation |
| --- | --- | --- |
| Frontend | Reports, inclusion toggles, availability and night-timing overrides, Run Pebble, live stage strip, run history, public season/night results and player expansion | Roster/settings editors, weekly/attendance views, fuller explanations and browser regression coverage |
| Login | Google popup login, own membership listener, officer-only editing, sign-out, anonymous results; access-loss cleanup and stale-callback guards with regression tests; grant/revoke runbook | Real second-officer onboarding/revocation and production domain/browser checks |
| Firestore | Strict input rules, version checks, server timestamps/UID attribution, create-only queued runs, queue index, public complete-only results | Broader rule/schema parity coverage, retention/size review, hosted app deployment and migration rehearsal |
| Worker | Shared Python fairness engine, consistent input snapshots, WCL/Redis reuse, isolated web Mongo materializations, transactional claims, serial drain, listener/retry, restart recovery | Remote service packaging, dependency outage and shutdown hardening, deployment-level singleton guarantee |
| Publication | Stage output under a new run ID; final transaction marks complete, moves public pointer, marks success | Interrupted-upload cleanup policy and larger-season load checks |
| Migration | S2 roster/reports/availability imported without changing Sheets; timing normalization and signed minute semantics retained | Full results comparison and cutover checklist; no automatic ongoing Sheets synchronization |
| Tests | Python calculation regressions; worker/schema and emulator tests; frontend model tests; security-rule emulator tests | Full browser workflow, cross-user edits/revocation, host/container failure scenarios |

Firebase project: `YOUR_FIREBASE_PROJECT_ID`, Firestore `us-west1`, season
`midnight-s2` (Midnight S2). Google sign-in is enabled. The first officer is
`OFFICER_ACCOUNT`. Project and season are configuration, not code dependencies.
The imported baseline contains 28 players, three reports, 19 availability overrides,
no character mappings, and no night overrides. These are migration counts, not a
promise that live data stays unchanged.

The frontend is served by local Vite at `http://127.0.0.1:5173`; Firebase Hosting is
configured in `firebase.json` but a hosted web release is not part of this baseline.
Standalone `?demo=1` mode remains separate from the live app. Google login was
verified in real Chrome; the in-app browser hit a Google sign-in loop.

The web worker currently runs from this worktree on the Mac:

```sh
.venv/bin/python -m pebble.cli web worker --config config-web.yaml
```

That is an interactive process, not an installed service. Its PID and terminal
session are intentionally not deployment configuration. Local config, `.env`, the
worker service-account JSON, and `apps/web/.env.local` are ignored by Git. Public
Firebase web keys belong in frontend config; service-account/WCL secrets do not.
The current worker service account has `roles/datastore.user`; browser rules do
not constrain that trusted server identity.

### deployment host observation

Read-only inspection on October 19 confirmed:

- Current Docker context: `deployment host`; server name `deployment host`, Linux x86_64,
  Docker Engine 24.0.2.
- Existing container `pebble-loop` runs `pebble loop --config
  /app/config-Midnight-S2.yaml --trigger-timeout 5`: this is still the Sheets bot.
- Containers `mongodb-misc` (`mongo`) and `redis1-misc` (`redis:latest`) are running.
  Their suitable networks, authentication, storage, and backup arrangement still
  need verification; container-local `localhost` will not reach those services.
- No containers, credentials, networks, or service settings were changed during
  this inspection. The new web worker has not been deployed there.

The existing Dockerfile is unsuitable for the new service as-is: it copies
`service-account.json` and all matching season configs into the image. The existing
`.dockerignore` also omits exclusions for `.venv`, Node dependencies, and the new
worker credential/config files. Fix packaging before building an image from a
credential-bearing checkout. Do not push the existing image as the web worker.

## Spreadsheet capability inventory

This inventory follows the repository's Sheets ingestion/export contracts and
[documented Apps Script probability model](../Attendance_Probability.md). It covers
the known controls and derived tables. Before cutover, confirm additional hand-made
formulas, charts, or layout-only conveniences in the live sheet with an officer.

Status meanings: **covered** = connected workflow exists; **partial** = some data
or behavior exists but parity is incomplete; **missing** = no connected web feature;
**configuration** = retain outside everyday officer UI unless a real need appears.

| Spreadsheet capability | Web coverage today | Status and remaining work |
| --- | --- | --- |
| Add WCL report links/codes | Validated multi-line entry; normalize codes; duplicate handling; explicit Save/Add | **Covered**; add browser regression cases |
| Report inclusion | Explicit include/exclude switches, save selection, rerun | **Covered**; excluded data removed on recompute |
| Report name/date/source/status | Bot-derived title/night/link and processed state | **Partial**; show last checked, owner/creator where useful, source errors/warnings and start/end bounds |
| Report notes | Schema/import preserve private report notes | **Partial**; connected report-note editor is absent |
| Break and mythic start/end overrides on report rows | Night timing form with Pacific datetime inputs and reset-to-automatic | **Covered with changed granularity**; one override per night applies to all reports; import rejects conflicting report-level overrides |
| Availability pre/post booleans | Automatic, available, unavailable for each half | **Covered** |
| Positive and negative minute availability overrides | Available for N (`partial`), unavailable for N (`unavailableFor`) | **Covered**; parity fixtures must include caps and short halves |
| Availability reason | Optional private note, save/cancel/reset | **Covered**; never publish the note |
| Roster main name/class color, active, join/leave dates | Imported; validated input schemas and worker consumption | **Partial**; no browser roster editor; preserve existing membership behavior |
| Alt-to-main roster mapping | Character collection and importer; worker name resolver | **Partial**; no browser mapping editor; test realm/region collisions and short aliases |
| Trigger checkbox to run ingest/compute/week | Run Pebble queues one request; live progress and completed-run history | **Covered and improved**; keep stage updates immediate |
| Last processed timestamp | Last successful completion shown with current result set | **Covered**; history carries timing/status, but requester detail is not rendered |
| Night QA table | Timing summary and some unknown-character/no-Mythic warnings exist; full QA stays in Mongo | **Partial**; officer diagnostics for source coverage, inferred boundaries, candidate gaps, override provenance |
| Bench Night Totals: total played/bench | Select a night; player totals and comparison bars | **Covered** for totals |
| Bench Night Totals: pre/post played, pre/post bench, inferred availability, status source | Pre/post bench and an adjusted indicator are exposed | **Partial**; project pre/post played, availability/inference explanation and provenance safely |
| Bench Week Totals | Weekly totals are computed and uploaded under each result run | **Partial**; frontend does not load/render weeks yet |
| Bench Rankings | Season played/bench totals, ascending bench ordering and numbered rows | **Partial**; explicit bench:played ratio is absent; verify membership eligibility, rounding, ties, zero totals against existing rankings |
| Attendance table | Existing Python attendance builder remains in Sheets path | **Missing** in web output/UI: played, bench, possible, attendance rate, weekly P/B/O grid |
| Attendance probability and roster-size what-if tables | Documented Google Apps Script; no web equivalent | **Missing**; port model with numeric fixtures after attendance projection; show assumptions |
| Calculation controls in Settings cells | Imported calculation document: break window/bounds, default start, post extension, timezone | **Partial**; no validated officer settings form |
| Sheet names, table offsets, trigger/processed cell locations | Unnecessary for a web-managed season | **Configuration**; keep legacy Sheets adapter config separate |
| Team sharing | Anonymous read of complete result runs; inputs/private notes denied | **Partial**; working locally, static hosted URL and navigation/deep links still needed |
| Spreadsheet filtering/copy/export and custom charts | Basic period selector and player expansion | **Missing** general export/filter/chart parity; prioritize demonstrated team needs |
| Officer edit access via spreadsheet sharing | Google Auth + enabled UID membership; CLI grants/revokes | **Covered** baseline; no self-service privilege grant; test full session revocation |

A public result's played percentage is played / (played + bench), not attendance.
Do not label it attendance or substitute it for the existing possible-time model.
The shared engine alone does not prove full projection/UI parity: current public
season rows sum nightly materializations for active players, while the legacy
rankings also apply their own membership eligibility checks.

## Work packages and ownership

Packages below retain outstanding acceptance work; AUTH-1 has the completed subset
recorded below. Priority **P0** blocks operational cutover;
**P1** completes normal officer use and core spreadsheet parity; **P2** restores
secondary planning/convenience features. Each package can be assigned separately.
An assignee should update its status, acceptance evidence, and follow-up items in
this document when finished. Do not mark a package complete from code alone when
it includes a real deployment or browser acceptance step.

| ID | Priority | Work package | Dependencies | Primary files |
| --- | --- | --- | --- | --- |
| OPS-1 | P0 | Safe container packaging and deployment host deployment | BOT-1 shutdown contract; final cutover after DATA-1/AUTH-1 checks | `Dockerfile`, `.dockerignore`, `docker-compose.yaml`, deployment runbook |
| BOT-1 | P0 | Worker reliability and lifecycle | Existing run/result contract | `pebble/web_worker.py`, `web_cli.py`, worker tests |
| DATA-1 | P0 | Data/rules consistency and migrated-season parity | Coordinate additions with UI-2/UI-3 | `web_models.py`, `web_import.py`, `firestore.rules`, rule tests |
| AUTH-1 | P0 | Officer access and production login readiness | Coordinate auth cleanup with UI-1; hosted smoke after WEB-1 | `firebase.ts`, auth/membership effects in `useFirestore.ts`, admin CLI, auth runbook |
| UI-1 | P1 | Core workflow reliability and polish | Current schemas | `App.tsx`, `Root.tsx`, `useFirestore.ts`, browser tests |
| UI-2 | P1 | Roster, character mappings, calculation settings | DATA-1 confirms schema/rules; AUTH-1 permissions | New officer components; narrowly scoped input adapter changes |
| UI-3 | P1 | Results, weekly totals, attendance and explanations | DATA-1 parity fixtures and additive output contract | Result projection, attendance builder reuse, result components/adapter |
| WEB-1 | P0 | Static release and environment separation | AUTH-1 domain review, DATA-1 rules; UI improvements can ship later | Hosting/config docs, build/deploy workflow |
| PLAN-1 | P2 | Attendance probability planner and optional exports | UI-3 attendance data | Python probability module/tests and dedicated UI |

### OPS-1 Container and deployment host deployment

- Build a Python-only image without embedded config or credentials. Tighten the
  build context exclusions; ensure package metadata/build inputs are sufficient.
  Use a non-root runtime if compatible with the lock and mounted file permissions.
- Add a web-worker service/Compose profile using `web worker --config ...`.
  Mount config and Firebase credentials read-only at runtime; supply WCL secrets
  through the established private deployment mechanism. Do not commit secrets.
- Account for remote Docker bind mounts: source paths are on deployment host, not the
  Mac. Decide how the image is built/transferred without transferring credentials.
- Verify Mongo/Redis endpoints, credentials and network reachability on deployment host.
  Preserve the existing Sheets database and unrelated containers. Set a deliberate
  web database base/cache prefix and document storage/backup expectations.
- Set restart policy, log rotation, and a termination grace period longer than
  both idle shutdown latency and the expected in-flight run duration. Decide
  whether shutdown should finish a run or intentionally interrupt/recover it.
- Guarantee one web worker per project/season across hosts. `flock` protects only
  one host/lock namespace; it does not prevent Mac + deployment host or two containers
  with separate temp directories from racing. Use operational singleton control
  (and a shared lock mount if needed), not a new distributed lease system.
- During cutover stop the Mac web worker before starting the deployment host web worker.
  Decide with the team when to retire `pebble-loop` on deployment host. Sheets and web
  are independent input sources after import; there must be one editing authority
  for S2. Do not silently stop the old service as a documentation task.

Acceptance: build and run on deployment host; verify idle minute heartbeat, live progress,
queued request after downtime, successful/failed run behavior, container restart,
log access, credential mount permissions, and no duplicate web workers. Record
exact deployment/start/stop/rollback commands. Rollback must explain how to resume
Sheets if needed and reconcile edits made only in the web app after migration.

### BOT-1 Robust worker behavior

- Exercise startup/offline/reconnect behavior against Firestore, WCL, Mongo, and
  Redis outages. SDK stream recovery and listener recreation already exist;
  a defunct-listener emulator test is not a complete network-failure test.
- Verify SIGTERM both idle and mid-run, forced termination, next-start recovery,
  and cleanup of listener/heartbeat threads. Idle SIGTERM latency is currently up
  to ten seconds; run duration and external request timeouts need bounded behavior.
- Handle errors during claim, capture, stage update, upload, final transaction,
  failure recording, and heartbeat without corrupting the public pointer or
  silently abandoning requests. Resolve ambiguous final-commit responses safely.
- Keep progress writes at meaningful stage transitions and heartbeat at most once
  per minute while running. Avoid per-fight/per-player progress writes or idle
  queue reads. A heartbeat thread being alive must not mask a dead work loop.
- Review retry classification: invalid inputs/source access errors should be
  actionable failed runs; retryable service outages should recover without a
  tight retry loop. Sanitize validation errors/logs to avoid credentials or raw
  private payloads leaking into public data or inappropriate logs.
- Include project as well as season in local materialization isolation if two
  configured projects may share Mongo; the current database suffix hashes only
  the season. Keep run ID/snapshot identity useful for troubleshooting.

Acceptance: fault tests for each boundary, no double computation for completed
requests, previous public results preserved on failure, serial processing under
repeated notifications, recovery after missed events, and documented shutdown and
retry semantics. Keep the current simple request/recompute workflow.

### DATA-1 Firestore rules, contracts and migration parity

- Compare actual migrated S2 results against the established Sheets calculation
  output. Add representative fixtures for multi-report nights, excluded reports,
  no-Mythic nights, positive/negative minute overrides, active/join/leave eligibility,
  zero played time, short halves, rounding, aliases, and Pacific/DST boundaries.
- Cover every writable schema in rule tests, especially roster, characters and
  calculation settings. Test forged metadata, unknown fields, nonexistent
  references, invalid dates/order, stale versions and revocation. Rules can reject
  shape/types; the worker must still validate actual calendar and domain semantics.
- Review alias realm/region identity and case normalization; region exists in the
  schema but is not represented in the worker's current alias dictionary key.
- Fix import gaps deliberately: reject/flag missing references instead of silently
  discarding important overrides; test legacy time and numeric formats. Generic
  JSON import should validate the full set before partial writes are possible.
- Define additive public fields needed for UI-3 using existing engine outputs.
  Exclude notes, UIDs, raw input snapshots and private errors. Current nightly
  player breakdowns are embedded arrays; each weekly doc also contains an array.
  Verify representative maximum size and Firestore batch/transaction bounds.
- Decide a small manual or scheduled retention procedure for abandoned staging
  results, old results/runs and Mongo snapshots. Keep the current/latest result
  protected; do not build a retention service without a demonstrated need.
- Verify deployed rules/index match the repo and test moving project/season in an
  isolated environment. Migration is explicit; configuration changes do not copy data.

Acceptance: parity differences resolved or explicitly accepted, schema/rule tests
cover all input families, no anonymous private/staging reads, no browser writes to
membership/output/status, and successful output remains internally consistent.

### AUTH-1 Officer access and login

Status: **access-loss cleanup chunk implemented and tested October 19, 2026;
AUTH-1 remains open for real-account and hosted acceptance.**

Completed in this chunk:

- Observed membership revocation, missing membership, membership-listener errors,
  sign-out and account changes clear private inputs/notes, roster, history,
  versions, consumed input versions, bot heartbeat and pending submission state.
  Private permission-denied errors also close the officer session. Public
  completed results remain available.
- Private listeners unsubscribe on access loss. Session guards reject old
  listener callbacks, save/run actions and asynchronous completions; these cannot
  restore private data, surface an old private error, or clear a newer request's
  busy state. Saves recheck access after transaction reads and on retries; run
  submission rechecks after its lookup. Server rules still decide any write
  already sent before revocation; the worker rechecks queued requesters at claim.
- The app remounts for each access generation, discarding unsaved forms even when
  access changes are batched. Re-grants load fresh inputs; old callbacks and
  pending request IDs remain invalid. Signed-in non-officers see their access
  state and UID for administrator onboarding.
- The [setup runbook](firestore-setup.md#officer-onboarding-and-revocation) records
  administrator authority, explicit grant/revoke commands and acceptance steps.

Verification: 21 frontend tests passed (nine existing model tests and 12 new
hook/component lifecycle regressions using mocked Firebase callbacks); all seven
Firestore rules tests passed on isolated `demo-pebble-auth1` at `127.0.0.1:8180`.
The added rules regression uses the same authenticated client before/after revoke,
checks server-side private read/write denial and public results for revoked,
non-officer and anonymous readers, then checks re-grant. Rules tests accept
`FIRESTORE_EMULATOR_HOST` and `PEBBLE_RULES_TEST_PROJECT` for isolated execution.
The production TypeScript/Vite build and `git diff --check` passed; the existing
large-bundle warning remains. No live memberships, rules, data or workers changed.

Remaining acceptance: onboard and revoke a real second Google officer; verify
popup blocked/cancelled, wrong-account recovery, real session refresh and queued
request revocation end to end; check production authorized domains/consent and
hosted signed-out/signed-in flows after WEB-1; review worker/admin least privilege
and credential mounts with OPS-1. The automated UI tests run in jsdom, not a real
browser; the rules tests use authenticated test contexts, not Google sign-in.
The full AUTH-1 requirements remain below.

- Keep UID membership and Google login. Document add/revoke steps and the separate
  authority required for officer administration; do not permit self-enrollment.
- Implemented in this chunk: clear private input/run data and stale editable forms
  when revocation is observed, including outstanding listeners and stale callbacks.
  Confirm the behavior in the real-account browser acceptance above.
- Test signed-out, signed-in non-officer, officer, wrong account, popup blocked or
  cancelled, session refresh, revoked access during editing, and revoked queued
  request. The worker already rechecks the requester before claiming a run.
- Validate authorized production/localhost domains and Google consent configuration
  with the deployed host. Handle sign-in and permission errors without a permanent
  loading screen. Use Chrome for the known-good setup flow; provide clear recovery
  rather than relying on the in-app browser's Google login behavior.
- Revisit least-privilege worker/admin credentials and their runtime handling during
  OPS-1. Keep admin identity, trusted worker identity, and browser membership distinct.

Acceptance: a second officer can be onboarded and revoked using the documented
procedure; revoked/non-officer users cannot read private data or change inputs;
anonymous team members can still view completed public results.

### UI-1 Core officer workflow and progress

- Add browser acceptance coverage for adding/re-enabling reports, duplicate/invalid
  input, selection saves, availability/timing edits, cancel/reset, save conflicts,
  queued/offline bot, completion, failure, and edits during a running computation.
- Preserve real stages and previous results. Check that completion does not claim
  fresh results before the matching public result set has loaded. Handle result
  subscriptions/network errors and user changes while a result fetch is in flight.
- Keep Run Pebble disabled for unsaved edits and pending submission/run. Test a
  double click or response lost during submission. Review the bounded recent-run
  query so a busy queue cannot hide the active request or last successful baseline.
- Improve empty/error states, source warning links, report last-checked details,
  default night selection, and selection after roster changes. Hide empty/unknown
  role decoration where it adds noise. Give non-officers a clear access state.
- Verify keyboard operation, focus, screen-reader announcements for stages/errors,
  small screens, and no sample fixtures or simulated timers in connected mode.
- Split `App.tsx` into a few feature components when doing the relevant feature,
  avoiding a broad UI rewrite. Keep Firebase adapter and sample calculations separate.

Acceptance: an officer can add a report, adjust availability, save and recompute
without extra approval steps; progress remains clear and responsive, and failure
preserves the last completed result view.

### UI-2 Roster and settings editors

Add simple officer forms for mains, class/role, active and membership dates;
character-to-main mappings; and existing calculation settings. Use stable IDs and
expected-version writes. Extend the adapter, which currently saves only reports,
availability and timing; rules/schema support alone is not a working editor.
Keep disable/archive and membership dates explicit; rules currently deny deletes.
Clarify rename/alias effects before changing identity. Optional notes stay private.

Acceptance: officers can manage the roster/mappings/settings without Firebase
console edits, and recompute through Run Pebble. Multi-officer conflicts do not
silently overwrite changes; invalid relationships fail with useful field feedback.

### UI-3 Result parity and attendance

Reuse existing computation and attendance functions rather than a second browser
fairness engine. Load/display stored weekly totals; expose nightly pre/post played
and bench, inferred availability, safe override/source explanation, and useful
private night diagnostics. Add the attendance rate, possible time and weekly P/B/O
view. Preserve membership eligibility, ascending bench ranking/ties and undefined
ratios; do not conflate played share with attendance. Establish stable public routes
or shareable links for season/night/player views. The current page switching does
not implement all routes shown in the design.

Acceptance: the capability inventory's core results rows are covered, totals and
eligibility match parity fixtures, public projections contain no officer notes, and
an anonymous team member can follow a shared link to the intended result view.

### WEB-1 Hosted app and environment readiness

Build the static app using public Firebase config; deploy Hosting, rules and index
with explicit project IDs. Record live and demo/emulator environments and authorized
domains. Check SPA routes, stale bundle/cache behavior, login, public reads, officer
writes, and local/remote worker connection. Add proportionate CI checks for Python,
web build/tests and rules/emulator tests; avoid accidental production deployments.
Keep Firebase rules tests and worker emulator tests sequential when they share a
project: rules tests clear that project's documents.

Acceptance: a shareable HTTPS URL works signed out and signed in, no private key
is in the bundle/image/repo, rules are deployed, and the chosen worker processes a
request submitted from that host. Record release and rollback commands.

### PLAN-1 Attendance planning and conveniences

Port the documented Apps Script model with numeric regression cases for uniform
and individual rates, smaller/larger rosters, highest/lowest drop modes, synthetic
new-player rate, exact counts, and at-least probabilities. Expose assumptions in
a focused planner after attendance exists. Add CSV/export/filter/chart features
only for identified team needs; they do not block the core workflow cutover.

Acceptance: values match known Apps Script fixtures and changing planner assumptions
does not alter real attendance inputs or published fairness results.

## Coordinating multiple agents

Use these IDs as task boundaries. Default to a small number of agents with disjoint
files; do not have several agents edit `App.tsx`, `useFirestore.ts`, or the worker
at once. An implementation document enables delegation; it does not start agents
or deploy services by itself.

A practical first parallel wave is OPS-1 packaging/runbook, BOT-1 worker lifecycle,
DATA-1 rules/parity tests, and UI-1 core browser tests/polish. Assign AUTH-1 with the
UI adapter owner or after their edits land. UI-2 and UI-3 should start after the
required data contract is agreed; PLAN-1 follows attendance. WEB-1 can prepare the
release steps early but finishes only after auth/rules readiness.

Before editing shared contracts, agree the exact document paths/fields, public vs
private projection, error semantics and test fixture with affected owners. Keep
changes additive and small. Use separate branches/worktrees for concurrent code
work and integrate reviewed commits; the current branch holds the shared baseline.
Never start a second live worker during development tests. Use demo Firebase
projects and isolated Mongo data, not live S2, for fault injection.

Each handoff should identify: package ID and scope; files owned/shared; dependencies;
implemented behavior; tests/commands and results; unresolved issues; and any live
resource changes. Merge one contract change before dependent UI/worker changes.

## Verification and cutover checklist

Already exercised: actual Chrome officer login; S2 import; a browser-created run
processed locally against WCL; automatic public results; override save/restore and
recompute; signed-out reads; listener reconnection; immediate stage writes without
heartbeat; one-minute heartbeat cadence. These do not replace the remaining host,
permission-revocation and spreadsheet-parity acceptance checks above.

Commit checkpoint on October 19: 215 Python tests passed with the Firestore
emulator enabled, nine frontend tests passed, six security-rule tests passed, and
the production web build passed. Black checks passed for the new Python modules;
`git diff --check` and local documentation link checks passed. The build reports a
large JavaScript chunk warning; measure and split the bundle as part of WEB-1 if
it affects real loading performance. Existing Python datetime deprecation warnings
remain outside this interface migration.

Run from the repo root unless noted:

```sh
# Start isolated auth/Firestore emulators first.
firebase emulators:start --only auth,firestore --project demo-pebble

# In another terminal: full Python suite, including worker emulator tests.
FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 .venv/bin/python -m pytest -q

# Then security rules; do not clear shared emulator data during worker tests.
(cd firebase && npm ci && npm test)

(cd apps/web && npm ci && npm test && npm run build)
git diff --check
```

Before declaring the web app the S2 editing authority: finish P0 packages; resolve
core parity differences; confirm which remaining P1/P2 gaps the officers accept;
onboard another officer; deploy static hosting; stop the Mac web worker and start
exactly one deployment host worker; perform a real run from the hosted UI; verify public
results, fast progress and recovery; and record how the old Sheets loop and any
post-import edits are handled. No additional officer approval of individual nights
is part of that cutover or the ongoing workflow.
