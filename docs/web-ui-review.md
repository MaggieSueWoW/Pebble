# Web UI review — October 22, 2026

Review covers the connected React app: Reports, Results, availability/timing
Overrides, Roster, Settings, shared navigation, account controls, public result
loading, input mutations and the accompanying Firestore rules. The refinement
branch descends from the accepted web-app source; the full app and its real-data
local configuration are retained.

## Findings fixed

- **Results lost failed-run feedback when its banner was removed.** A failed run
  could leave only the previous publication's timestamp visible. Centralized run
  status selection now supplies all pages, retaining failure, offline queue,
  submission, queue availability, loading and stale-input messages. The full
  browser run-submission scenario reproduced the failure before the fix and
  passes afterward. A component regression checks failed status on every page.
- **Roster delete/swap operations were outside the editor-save navigation guard.**
  These operations set connection busy state rather than an editor's saving
  state. Navigation, sign-out and browser traversal now wait for that state too;
  reload uses the existing before-unload protection while a mutation is pending.
  Component coverage checks blocked navigation, player selection, sign-out and
  reload, then successful navigation once the operation finishes.
- **The availability conflict browser test expected a removed label.** Replaced
  its old Automatic text assertion with the current absence-of-Override signal,
  preserving its concurrent-save and expected-version assertions.
- **Sort indicators were duplicated across three lists.** Results, Overrides and
  Roster now share `apps/web/src/SortButton.tsx`. Existing column ordering,
  direction defaults, active indicators and accessibility semantics are retained.
- Updated stale account-recovery and empty-report copy. Reconciled the web README
  with implemented editors, log-derived nights, season/night selection, duration
  presentation, disclosures, sorting, account actions and accepted attendance.

## Remaining findings and release work

### P2 — concurrent main deletion can leave linked records behind (#47)

`deletePlayer()` enumerates character mappings and availability overrides before
its transaction, then checks only those known documents. A new dependency created
between enumeration and commit is not observed by the transaction. Current rules
allow main deletion without a check over the linked collections. A synthetic
emulator reproduction confirmed that a newly created alt survives deletion of its
main. Worker input validation can then reject the orphaned mapping.

A fix needs coordination between linked writers and deletion, with deterministic
concurrent-client coverage for both linked families and alt reassignment. This
was tracked in #47 rather than hidden inside presentation cleanup. The follow-up
fix now coordinates browser linked writes, rules and administrative imports via
the main's existing version; see [the write protocol](data-consistency.md#concurrent-main-deletion-and-linked-writes).
Regression coverage now includes 14 linked-change-first races (seven operations
at two transaction boundaries) and four deletion-first races. The full 80-case
browser run plus those four additional cases passed, alongside 184 component,
22 rules and 17 Python input/import tests. It requires a coordinated
source/rules/importer release and has not been deployed.

### Roster deletion requires its matching rules release

The refinement branch includes officer main/alt deletion rules and passing
acceptance coverage. The connected environment still needs those rules released
before deletion can work there. A frontend-only release cannot enable that action.
Coordinate the release with #47 and the rollout tracker (#14); this review did
not deploy rules or Hosting, modify production data, or restart a worker.

### Existing integration decisions remain open

#14 still owns review/integration of separate Docker build paths and eventual
feature-to-main integration. UI refinement does not authorize those merges or a
deployment. The optional probability planner/export scope in #13 remains deferred;
these refinements implement no planner and do not change calculation semantics.

## Verification

- 178 unit/component tests across 23 files passed.
- All 66 browser acceptance tests passed against synthetic Auth/Firestore
  emulators, including auth/revocation, five-page workflow, version conflicts,
  denied/held saves, publication consistency, shared links, keyboard interaction,
  narrow layouts, main/alt actions and queued/failed runs.
- 20 Firestore security rules tests passed in a separate synthetic project.
- 5 release-tooling tests passed, including temporary HTTP smoke checks.
- Production build, strict unused-local/parameter TypeScript checks and whitespace
  checks passed.
- The deletion-race reproduction used a dedicated synthetic emulator project and
  removed its fixture data afterward. No live data was used for write tests.

The build retains its existing large-chunk warning (approximately 1.03 MB raw,
276 KB gzip). Loading officer editors on demand is a possible later performance
improvement; no performance regression or new bundle-growth issue is claimed.
Actual screen-reader listening and hosted Google consent were not repeated; the
review reuses prior hosted acceptance and exercises their local/emulator paths.
