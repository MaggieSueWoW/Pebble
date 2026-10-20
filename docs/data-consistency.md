# Data contracts and S2 recomputation (DATA-1)

Implementation targets `codex/pebble-web-app`; no production deployment or legacy
bot restart is part of this change. Season 2 uses the intended web inputs and
reprocesses all included Warcraft Logs reports whenever Run Pebble is requested.
Cached WCL bundles may avoid upstream reads, but calculations rebuild the web
materializations. Set roster, character mappings, eligibility, availability,
settings and report selection in the officer workspace. The web workflow has no
Sheets import command and requires no Sheets credentials or output migration.
Existing live inputs are not reset by this change. Legacy bot retirement and the
editing-authority cutover remain separate decisions in #14.

## Reference calculation evidence

A read-only reference comparison on October 19, 2026 found zero numeric differences
between existing S2 Sheets output and published web run
`72d7f351-e451-4770-91c8-f72c8dad585e`: 75 nightly rows, 81 weekly rows and 28 season
totals. The web view includes nine additional all-zero nightly presentation rows,
not extra played/bench minutes. Recalculating the same existing web inputs and
three cached WCL bundles with this branch in in-memory Mongo also matched.
No inputs or results were imported from Sheets for those checks. Private extracts
are not committed; the comparison is historical reference evidence, not a future
S2 setup dependency.

Fixtures compare the web adapter with the established calculation engine for
overlapping multi-report nights, exclusions, no-Mythic nights, signed minute
overrides, join/leave/active eligibility, zero played time, short halves, rounding,
aliases and Pacific boundaries. Separate tests reject nonexistent dates, DST
ambiguity/gaps and out-of-night timing. Positive partial availability larger than
a half is clamped to that half, matching the established engine; timed
unavailability retains its negative-minute semantics. The Sheets engine itself
is unchanged.

Main names are season-wide identities. Explicit character aliases match
**region + realm + name**, with casefolding for names/realms and lowercase region
codes. Empty realm means an intentional name-only alias within that region.
Mappings in different regions cannot overwrite one another. Explicit aliases win
before the main-name fallback. Realm mismatches do not acquire a name-only alias
unless one was explicitly configured. Cached WCL data is copied before mapping;
missing report region with configured aliases fails with an actionable message
rather than guessing US. Refresh only the affected web cache entry if an old
bundle lacks region; never flush legacy Redis keys.

## Input and rule boundaries

The optional administrator JSON command validates schemas, calendar/domain
constraints and references against the entire effective target season before
any writes. Missing input documents are created in one Firestore transaction;
existing inputs win. This is not a synchronization or overwrite command and is
not required for normal S2 setup or reprocessing. It does not read Sheets or
write calculation settings. Configure calculation settings in the UI.

JSON files contain only domain fields; version, actor and server timestamp
metadata are authored by the importer. Imports are limited to 400 writes per
transaction. Split larger files into reference-complete groups (players before
their characters/availability); do not silently chunk partially valid seasons.
Concurrent target changes cause transaction retry instead of partial application.

Rules cover reports, players, characters, night overrides, availability overrides
and calculation settings. They enforce allowed keys/types, attributable server
timestamps, consecutive versions, reference existence, enum/range bounds and
lexical date/time ordering. Actual calendar dates, DST, uniqueness and computed
half-length constraints belong to the worker, since rules are not a domain
calculation engine. For example, a calendar-shaped February 30 may pass rules but
must fail worker validation. Anonymous readers cannot access private inputs,
runs/status, incomplete output or output whose root is absent. Browser clients
cannot write membership, worker status, public pointers or any result family.

## Public result shape and storage bounds

Public player rows contain identity/display fields and numeric played/bench
aggregates. `prePlayed` and `postPlayed` are additive fields for UI breakdowns;
`played == prePlayed + postPlayed` and `bench == preBench + postBench`. Notes,
UIDs, attribution, raw snapshots and private failure errors stay out of output.
Weekly rows are projected through an explicit allowlist of engine numeric fields.
Nightly/weekly arrays remain appropriate for this raid; no new collection model
is needed. Public warnings contain authored data-quality summaries only.

Season player rows may additionally contain `attendance` with `played`, `bench`
and `possible` minutes, `rate`, and ordered `weeks: [{week, status}]`. This
projection calls the existing attendance statistics builder; its eligibility,
per-half possible-minute flooring, minute normalization and Tuesday week IDs
match the Sheets attendance output. `rate` is the raw `(played + bench) / possible`
fraction before presentation rounding, or `null` when possible time is zero.
Week statuses retain the established P/B/O order, including empty strings when
that week has no applicable event. The nested minute values reflect eligible
membership nights; they need not equal the season row's existing aggregate fields.

For newly computed results, helper-ineligible rows omit `attendance` (for example,
active membership ended before the latest observed night or starts afterward).
Absence alone must be shown as unavailable, not zero attendance or a guessed
eligibility reason, because older results also lack this field. Only helper-eligible
mains already present in the season player output receive the field; nightly and
weekly result documents do not change. Older published runs legitimately lack
this additive field until a reviewed worker release and normal recomputation. No release or
live recomputation accompanies this source change. Notes and private attribution
are never serialized. The existing atomic publication and size preflight apply to
the entire nested attendance payload.

Before creating staging output, publication measures every payload using the
Firestore protobuf encoder. Documents exceeding a conservative **900,000-byte**
budget fail without moving the public pointer; batches are limited to **400
writes and 8,000,000 bytes**. The write count is an application limit; byte
budgets leave headroom below Firestore's documented 1 MiB document and 10 MiB
request limits ([Firebase limits](https://firebase.google.com/docs/firestore/quotas)).
Final publication remains the existing three-document transaction: complete root, public pointer and successful run.

The actual S2 replay's largest night measured 8,402 bytes and its largest week
4,722 bytes, including 1 KiB path/metadata headroom. Synthetic 200-player Unicode
rows and 520 night documents exercise document and batch sizing; an oversized
array is rejected before staging. This is a tested envelope, not unlimited scale.

## Small manual retention procedure

Review monthly; no scheduled service is needed for V1. **Do not run cleanup as
part of deployment or this PR.** Retention is an administrator operation with a
reviewed dry-run inventory and normal backup/recovery procedures.

1. Read the public pointer, run statuses/timestamps, result roots and Mongo
   `input_snapshots` run IDs. Record a dry-run candidate list and counts without
   dumping private snapshots. Protect the pointed-to result/run/snapshot and the
   newest successful result even if their timestamps are old. Keep queued/running
   requests and anything without a trustworthy age out of cleanup.
2. Retain at least 90 days of terminal runs/complete results and at least the 50
   newest successes. Incomplete staging is eligible after seven days only when
   its run is terminal; orphan staging requires manual confirmation that no
   worker is processing it. Establish age from run timestamps or document create
   time, not Mongo ObjectId guesses.
3. Pause only the designated **web** worker during an approved cleanup window,
   then re-read the public pointer and statuses and subtract protected IDs again.
   Do not touch `pebble-loop`, shared Mongo/Redis configuration or legacy data.
4. Delete each approved result's players/nights/weeks subcollections before its
   root; deleting a Firestore parent does not delete children. Delete the matching
   old terminal run and web Mongo input snapshot by the reviewed run ID. Limit
   batches to the documented budgets. Never clear a whole database or cache.
5. Verify the protected complete result and public pointer still match, record
   counts, and resume the designated web worker. If the pointer/status changed or
   a candidate is ambiguous, abort rather than widening the deletion scope.

## Deployment audit and isolated project/season checks

The read-only audit found deployed rules **exactly matched the feature base** and
the `runs(status ASC, requestedAt ASC)` collection index was `READY` (the service
adds an implicit document-name field). Worker credentials intentionally lack rules
administration access; the existing signed-in administrator performed the rules
read. No IAM grant or rule/index deployment was made. This branch tightens rules,
so those changes still need review and an explicit feature deployment followed
by the same read-only comparison; do not claim the new rules are live.

Emulator tests explicitly import into a second `demo-` project and a different
season, verify both start empty, and prove source documents stay unchanged.
Changing project/season configuration does not copy anything. A new destination
requires its intended inputs, separate membership/auth setup, destination indexes
and rules, and a destination calculation. Enter inputs in the officer workspace
and reprocess the logs; no Sheets migration is needed. Do not copy a public
pointer without its complete results or assume that moving input documents transfers Authentication
users or officer authorization. Coordinate actual cutover through #14.

Reproduce Python tests with a dedicated demo Firestore emulator and
`FIRESTORE_EMULATOR_HOST=127.0.0.1:8281 python -m pytest -q`. Run security tests with
`FIRESTORE_EMULATOR_HOST=127.0.0.1:8281 PEBBLE_RULES_TEST_PROJECT=demo-pebble-data1 npm --prefix firebase test`.
Use separate synthetic projects and run these suites serially; never target live
Firestore or change the live browser's environment to run emulator acceptance.


## Recorded availability explanations and private night diagnostics

Newly calculated nightly player rows with a computed bench-night record include
`availability: {pre: {available, mode}, post: {available, mode}, source}`. The
booleans copy the engine's half availability flags; the modes copy the captured
per-half input (`automatic`, `available`, `unavailable`, `partial`, or
`unavailableFor`). An available flag can represent partial availability and does
not promise a full half of available time. Recorded played/bench minutes remain
the numeric result. `source` copies the engine's whole-row priority (`override`,
`last_fight`, `blocks`, or `none`), not a separately inferred cause for each half.
No calculation changes or extra upstream reads are involved. Rows without computed
bench data and older publications omit the field; absence means not recorded,
not automatic availability. Season and weekly rows do not acquire half sources.

The existing officer-only run document additionally carries `nightDiagnostics`,
keyed by processed night. Its allowlist contains source-report and Mythic-fight
counts, unmapped-character count, nullable recorded break minutes, and timing
source labels. Start is `override`, `configured` (the worker's default-start
setting actually applied), or `detected`; end is `override` or `detected`; break
is `override`, `detected`, or `none`. These labels come from computed night QA
and captured timing inputs. No raw candidates, unknown-character names, notes,
credentials or service errors are copied. The diagnostic map is never written to
a public result document.

Only officer metadata for the displayed completed run may be shown beside its
selected night. Access changes clear it; old runs without diagnostics show not
recorded. The run document, including existing metadata and the new map, is
preflighted against the existing document-size budget before its metadata update;
oversize failure leaves the prior completed public pointer intact. Public nested
availability remains covered by the existing result publication preflight.

These are additive source changes for a reviewed release. No running worker is
updated, deployed or asked to recompute as part of preparing this contract.
