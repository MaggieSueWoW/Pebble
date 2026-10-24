# Pebble web app

React + TypeScript frontend for Reports, Review Night, Overrides, Results, Roster, Officers and Settings.
The public About page explains Pebble’s purpose, how the team uses bench time,
and how reports become shared results. It is available from the sidebar to all
readers and directly at `?view=about`, without signing in.
Run Pebble is available in the sidebar throughout the officer flow. Reports also offers a Next action that runs Pebble and opens Review Night after success. Google officer login, Firestore, and the officer-access callable provide the hosted services;
the Python bot continues running locally. Successful runs automatically update
public results. There is no approval or publish step.

## Local setup

Requires Node.js 22.13+ on the 22.x line, or Node.js 24+.

```sh
cd apps/web
npm ci
cp .env.example .env.local
npm run dev
```

Fill in `.env.local` with your Firebase web app configuration. These browser keys
identify the project; authorization comes from Authentication and Firestore rules.
Never place a service account or WCL secret in a `VITE_` variable.

Open http://127.0.0.1:5173. Google sign-in requires this host in Firebase Auth's
authorized domains. Officers can save inputs and queue runs. Everyone can read
the last complete result set without login. Signed-in accounts without officer
access see a read-only explanation and a Sign out action.
Account details keeps the setup ID available without showing it in the main message.
Sign out, then use Officer sign-in to choose another Google account. Login failures show recovery
instructions; failed access verification returns to public results with a retry
action instead of leaving the page connecting indefinitely.

See [Hosting and release](../../docs/web-hosting.md) for environment separation,
explicit release/rollback commands, cache checks and hosted acceptance.

See [Firebase and worker setup](../../docs/firestore-setup.md) for backend setup,
credentials, officer onboarding/revocation, emulator checks, and changing projects.

```sh
npm test
npm run build
```

Application code lives in `src/`, unit and component tests mirror its folders in
`tests/`, and browser workflow tests stay in `e2e/`. For example,
`src/features/roster/RosterList.tsx` has its component tests in
`tests/features/roster/RosterList.test.tsx`. `npm test` runs `tests/` through Vitest,
and the build type-checks both application code and unit/component tests.

## Local checks

```sh
npm run check        # ESLint, CSS lint, TypeScript and Prettier verification
npm test            # Unit and component tests
npm run test:browser # Browser workflows against isolated emulators
npm run build       # Lint, type-check and build the production app
```

Checks run locally. The GitHub Actions Checks workflow is disabled and its
configuration has been removed.

`npm run lint` covers TypeScript, React hook usage/dependencies, Vite Fast Refresh
exports and CSS. Warnings fail the command. `npm run typecheck` checks application
and unit-test types. `npm run format:check` checks formatting across the web app,
including browser tests, config and documentation. Generated files, browser traces
and local environment files are excluded. Use `npm run lint:fix` for ESLint fixes
and `npm run format` to apply Prettier formatting. Review effect-dependency changes
carefully: listeners and timers should depend on their identity, not progress
updates or a new object created on every render.

The web rollout tracker (#14) contains the implementation backlog
and acceptance status. Architecture references cover stack/workflow (#2),
Firestore/security (#3), and officer/public UX (#4).

## Code map

- `app/Root.tsx` handles Firebase setup/loading and officer-access remounts. `app/App.tsx`
  selects the connected workspace or the sample app.
- `app/Workspace.tsx` coordinates navigation, editor hooks, draft guards, account
  actions and run submission. Editors stay mounted here across page changes so
  switching pages cannot silently discard a draft. `features/results/useResultsView.ts`
  similarly keeps result sorting and link selection alive across navigation.
- `app/` also owns the shell, sidebar, navigation and run history. `components/`
  contains shared controls, tables, panel headers, status and feedback components.
- `features/` groups Reports, Results, Overrides, Roster and Settings. Each folder
  owns its page, feature components, editor hooks, domain helpers and `styles.css`.
  Pages receive typed data, editor state and callbacks; saves and guarded
  transitions go through the workspace. Shared UI belongs in `components/`;
  feature-specific UI stays with its feature.
- `demo/` owns sample fixtures, illustrative calculations, browser persistence,
  simulated runs and prototype controls. Only `DemoApp` mounts `useDemoSession`;
  the connected workspace never starts sample timers or reads sample storage.
  Sample-only timing validation and the sample report link are supplied explicitly
  to the shared UI. The existing sample storage key and schema are preserved.
- `shared/model.ts` contains shared types, input validation/parsing and formatting.
  `shared/useVersionedEditor.ts` supplies the common versioned-draft lifecycle.
- `data/firebase.ts` configures the Firebase client. `data/useFirestore.ts` is the
  connected-data facade. Its `LiveConnection` and `Versions` exports remain the
  public contract used by the UI. `data/usePublicResults.ts`
  independently loads the public publication; private implementation lives in
  `data/firestore/`:
  - `useOfficerAccess.ts` owns Auth, server-confirmed officer membership, access
    generations and write authorization checks.
  - `usePrivateState.ts` holds private buffers and the shared reset boundary.
    Access invalidation stops guarded subscriptions and clears those buffers
    before a new session can use them.
  - `usePrivateSubscriptions.ts` watches inputs, recent history and bot heartbeat.
    `useResultMetadata.ts` follows the displayed publication's private metadata.
  - `useRunQueue.ts` owns active-queue observation, request submission and direct
    tracking through terminal status. A retry after an uncertain acknowledgement
    reuses the request ID; recent history is not the authority for queue state.
  - `useWorkspaceData.ts` converts private snapshots to UI models. `documents.ts`
    contains timestamp, metadata and ordering helpers.
  - `useInputWrites.ts` handles input/settings transactions; `useRosterWrites.ts`
    keeps main/alt writes and their linked-player conflict protocol together.

Keep session assertions before and after asynchronous transaction reads. Stale
callbacks must check the captured access generation before updating private state.
The public-hook tests and browser workflows exercise these boundaries together.

## Current interface

The sidebar holds navigation, the season, Run Pebble, bot status and account actions.
Page headers contain only the page name. Results and Overrides show run status and
the displayed publication's completion time in their toolbars. Reports, Roster and
Settings use a compact status line. Saving never starts a calculation.

Results rows expand by clicking the row or either detail button. Bench precedes
Played; zero durations use `-` and durations below an hour omit `0h`. Player,
Bench, Played and Time Played sort by their displayed values; the default preserves
the published ranking. The `#` value stays with the published season player.

Overrides lists players alphabetically with Bench and Played before/after the break.
All five data columns are sortable; zero and unavailable recorded values remain
distinct. Applied overrides highlight the affected halves. Raid timing uses a
compact detected/override table.

Reports sort by raid night, oldest first by default. Click the Raid night header
to reverse the order; reports awaiting metadata stay last. Result warnings link
to the reports that caused each warning for officers. Source links require a run
with the updated worker; report IDs remain in private run metadata.

Roster lists Player, Joined, Left and comma-separated Alts. Each data column sorts;
Alts uses the alphabetically ordered visible names. Empty dates and alt lists sort
last in either direction. Expanded details combine main editing, alt editing,
adding/deleting alts, main deletion and a confirmed main/alt swap. Swaps preserve
the player ID, membership, class, history and the promoted character's mapping.
Roster deletion requires the accompanying Firestore rules to be released; a frontend
release alone does not enable it in an environment with older rules.

Every sortable list marks its active direction, including the default. Sorting is
local to the displayed list and makes no writes or run requests. Settings field
labels and detailed descriptions are editable in `src/features/settings/calculationFields.ts`.

## Styling

`src/styles.css` is the import entry point. Keep styles in these layers, in order:

- `styles/tokens.css`: the palette, five typography sizes, spacing scale and shared
  dimensions. Change `--text-body` for the default text size, `--row-height` for
  desktop list density, and `--panel-padding` for panel spacing.
- `styles/base.css`: element defaults, inherited typography, focus rings and small
  utilities. Controls and table data inherit the body size.
- `styles/components.css`: reusable controls, panels, toolbars, notices, tables,
  disclosures and dialogs.
- `styles/layout.css`: the application shell, sidebar and shared run history.
- `features/*/styles.css`: page layout and content-specific details, including their
  responsive rules. Prefer arranging shared components here over restyling them.

Use `DataTable` for a scrolling table with shared headers and row density;
`summary-table` styles nested breakdowns. `PanelHeader`, `RunStatus` and
`SelectControl` provide the repeated header, timestamp and dropdown markup.
Keep native table/form semantics and existing accessible labels when composing
these components. Inline styles are reserved for data-driven values such as
player class colors and percentage bars.

Results, Roster and Overrides use `--row-height` for their collapsed player rows,
including borders and detail controls. The token is 28px on desktop and changes
to 36px below 40rem for touch use; both values are defined in `tokens.css`.
Expanded details keep their own content spacing.

For new UI, start with existing components and tokens. Add a shared pattern when
it is useful across pages; keep one-off layouts in their page file. Avoid chains
of overrides that change typography or control padding separately on each page.
The cascade layers establish precedence without increasingly specific selectors.

`npm run lint:css` checks for duplicate rules/properties, excessive specificity,
`!important`, and literal font sizes, font families and hex colors outside the
tokens file. It also runs as part of `npm run build`. Run `npm test` and
`npm run test:browser` after structural changes; the browser suite includes
keyboard behavior and narrow-screen overflow checks.

## Sample mode

Open http://127.0.0.1:5173/?demo=1 for the original standalone sample prototype.
It uses localStorage and illustrative calculations, independently of Firestore.
Prototype controls simulate offline/failing runs and reset sample data. The
production Python fairness engine remains authoritative in the connected app.

Sample mode illustrates Reports, Overrides and Results. The connected officer
app additionally exposes the real Roster and Settings editors.

## Officer workflow browser acceptance

With Chrome, Java 21+ and the Firebase CLI installed (validated with CLI 15.22.4):

```sh
npm run test:browser
```

This starts a separate Vite server on port 5174 and Auth/Firestore emulators on
9199/8180 using `firebase.browser.json`. It always uses the
`demo-pebble-reports` project and synthetic officers/reports, overriding local
Firebase configuration. It clears only that isolated emulator's Firestore data;
keep these ports free. No worker is started. Editor tests assert zero run requests;
workflow tests create only their expected synthetic requests in this demo
project. Failure traces stay in ignored `test-results/`.

Shared browser helpers live in `e2e/support/`. Call `setupEmulator()` once at
the top level of each spec, before scenario hooks: it loads the real rules,
clears Firestore before each test, and cleans up SDK clients after the spec.
Use its `admin` callback for fixture writes and `environment` for authenticated
or anonymous rule checks. Keep scenario-specific documents and assertions in
the spec. `fixtures.ts` provides version metadata and bot heartbeats;
`officer.ts` signs in through the Google emulator popup and grants officer access.
Tests run with one worker because they share the disposable emulator database.

Auth coverage adds onboarding, session refresh, revocation during editing,
re-grant without stale drafts, sign-out, popup blocked/cancelled recovery, and
account switching through cancellation and an unsaved-draft guard. It
uses emulated Google accounts; real Google consent and hosted-domain acceptance
are separate checks recorded with AUTH-1 (#8).

Coverage includes report validation/deduplication, add/re-enable, selection
save/cancel, delayed and denied saves, concurrent version conflicts, draft
navigation protection, keyboard dialog focus and narrow-screen overflow.
Report details coverage checks empty lists, blank-title fallbacks, recorded Pacific
check times, missing timestamps and metadata updates during selection drafts.
Its metadata scenario seeds one completed history fixture and asserts no extra
run records; it does not queue a request or contact Warcraft Logs.
Availability coverage adds
mode/minute validation, save/cancel/reset, delayed/denied saves, version conflicts,
newer edits during saves, and player/night/tab/page navigation. Each availability
case verifies unrelated reports, timings and player/night overrides stay unchanged.
Roster coverage checks late arrival and clean removal, search-independent selection,
missing class/role labels, and retention of a removed player's draft until explicit
cancel or roster return. The editor waits for pending saves before choosing a
replacement. An empty officer roster stays separate from historical result players.

Overrides initially selects the newest night identified by fetched report metadata
or completed results. An untouched default follows arriving metadata; choosing a
night or starting an edit keeps that night selected. Custom dates are not offered.
With no known nights, the app explains how to add reports and run Pebble first.
Date coverage checks late metadata, ordered options, explicit choices,
draft-discard guards and held saves on the original night.

Timing coverage adds save/cancel/reset, blank and detected values, ordering and
paired break validation, delayed/denied commits, version conflicts, newer edits,
and night/tab/page navigation. It also checks Pacific daylight-saving validation,
wall-time presentation in a non-Pacific browser timezone, narrow-screen keyboard
operation and standalone sample mode. Timing cases verify reports, availability,
other nights and the empty run queue stay unchanged.

Run-submission coverage includes held/repeated clicks, denied writes, a committed
request whose acknowledgement is replaced with an error, recovery without duplicate
creation, offline heartbeat messaging, progress stages, terminal failure and
tracking beyond the recent-history limit. Active queued/running requests are read
separately from recent history, so reloads and other officer sessions still find
older work. Run stays disabled until the server confirms queue state; a terminated
queue listener offers Retry run queue. Browser coverage checks queued and running
reloads, another officer session and terminal details outside recent history. Test fixtures advance the emulator queue
and heartbeat; they never start or contact a worker. Previous completed results
and unrelated input versions are checked throughout these cases.

Officers follow a four-step guide: Add report → Review Night → Overrides →
Results. The numbered navigation and next-step buttons sit alongside
free sidebar navigation. The run step happens inline on Reports; its Next button
sits below the add-report form and above the list, which defaults to newest raid
night first. Reports does not show a selected night until the logs have been
processed. Successful guided runs open the newly fetched report’s night for review.
Failed submissions/runs stay on Reports, and an offline worker leaves the request
queued with visible status. Review Night groups timing checks/corrections, missing-player
warnings, source links and private night diagnostics for the selected raid night.
Review Night places the night selector, update status, and Next on one toolbar
row above a compact summary of start/end/break times, raid duration, report count,
and encounter count.
Timing sources appear under each time; durations round to whole minutes.
Timing overrides are collapsed by default behind “Times look wrong? Adjust timing”
in the summary box, with an Unsaved badge when there is a timing draft. Times
use Pacific, as in the timing editor. Terse warnings and compact
per-character action rows follow. Officers can
ignore a character for this season, add a main with the reviewed night as its join
date, or map an alt to an existing main without leaving the page. Character
identities come from private completed-run diagnostics, preserving exact names,
realms and regions. Older runs require a new worker run before inline actions are
available. Player forms share the workspace draft and save guards. Main creation
uses the logged class as its default without asking for a role. Ignore requires
an inline confirmation; Cancel leaves the character unchanged. Player actions
keep one row per character, with horizontal scrolling on narrow screens.

Overrides keeps the player list and editor side by side, with Next beside the raid night selector above both.
The list shows sortable before/after-break and total bench minutes; played minutes appear in
the selected player's details. The “Overrides only” filter and adjusted-player
count use current saved availability modes, including resets before a rerun.
Available includes time spent benched. Before/after-break choices sit together,
optional private notes start collapsed, and Save availability, Cancel, and Reset
to detected share one action group. Saved changes recalculate through the top
Next button before Results opens.

Results keeps the main table with Bench, Played, and Played %; displayed durations
round to whole minutes while sorting and percentages retain published precision.
Adjusted badges appear only for a selected night. Its period breakdown marks
adjusted times and includes per-period reasons directly in the table; missing
provenance stays unknown. Season player details have no nested disclosures or
adjustment tags: attendance shows the published percentage alongside eligible
played/bench/possible time, followed by a compact weekly table using full status
words. Attendance details remain season-only. The top toolbar groups the period picker,
update status, and Copy result link. Reports remains available in the sidebar. Officers see a persistent Nights to review sidebar list, grouped newest first
by date with a warning count and direct navigation to that night. General warnings
appear once under Other warnings. The list follows the displayed completed run,
remains during pending/failed updates, and clears when a clean run loads. Public
readers retain the published warning text. Results has no extra readiness line;
its existing run status still reports loading, failures, and stale inputs.

Ignores use versioned character inputs with `ignored: true` and an empty `playerId`.
They use the existing character subscription and input-version freshness checks.
The worker removes ignored actors from participation while retaining fight times;
explicit main/alt mappings take precedence over ignores. Main and alt actors mapped
to the same main count once per fight. Browser rules keep decisions officer-only;
new ignore inputs require the matching worker and rules release. Overrides focuses
on player availability. Saved changes are recalculated before advancing from review
to overrides or from overrides to the selected night's results; failed runs leave
the officer on the current step. Results retains a compact link back to review
when warnings remain. Public readers continue to see recorded warning text without
private source links or officer navigation. Warning text is rendered from the
completed publication, and source links/diagnostics use its existing private
metadata subscription, with no additional service reads.

Result-loading coverage holds result reads, rejects incomplete output for officers
and public readers, exercises retry (including first-load errors), and supersedes
a delayed fetch with a newer publication. Warning coverage checks retained and
replaced warnings, missing warnings, keyboard retry and narrow layout. It checks
that rows, result periods and totals stay consistent, and that an edit saved during computation still needs a
new run after older captured input versions arrive. Queue/completion/publication
transitions are synthetic emulator fixtures; one freshness case submits a single
expected emulator request, with no worker process.

Access coverage signs in a synthetic non-officer, opens account details with the
keyboard, grants and revokes officer membership, and signs out through the visible
Sign out action. Published results remain readable while private controls
and unsaved private drafts disappear on revocation. These cases create no requests
or input changes; all sign-in helpers use the visible account-details disclosure.

Empty-results coverage distinguishes no completed result set from a completed
period with no player time. Public guidance stays read-only; officers can use
Reports in the sidebar to review inputs without a write or run request. Empty recent
history has an explicit message while queued progress and previous results remain
available. Loading and result errors retain their separate states.

Review and overrides use the same workspace draft/save guards as the sidebar,
numbered steps and browser history. Keyboard and narrow-layout acceptance checks
exercise page navigation, dialog focus restoration and pending saves.

When a completed result has warnings, officers can follow source report
links on Review Night using that run's recorded titles and report codes. The existing private
run-metadata listener supplies them; newer input/history metadata cannot relabel
an older displayed result. Missing titles fall back to codes and missing metadata
omits the list. Public readers retain warning text without private source links.
Acceptance checks replacement/loading, stale history, fallback labels, public
visibility, keyboard link focus and narrow layout without contacting Warcraft Logs.

Accessibility/error coverage links both result-detail buttons to stable hidden
breakdown content and labels the discard dialog. Report and availability validation
is associated with its field; Run status announces only its changing copy, outside
the action buttons. Disappearing result/queue retries recover heading focus when
focus would otherwise be on the page body; officer-access retry restores focus
after the workspace reloads. Keyboard and narrow-layout scenarios exercise details,
validation, dialogs and retry recovery. Temporary permission denial is restricted
to the demo emulator, with its original rules restored afterward.

These automated checks verify DOM/accessibility semantics, browser focus and
keyboard behavior. Actual screen-reader listening remains part of final acceptance.

Results offers season totals and individual raid nights. The toolbar shows the
completion time for the displayed results and the season's raid-night count.
Loading and retry preserve the previous completed results and their timestamp.
Run Pebble is available in the sidebar throughout the officer flow. Reports also offers a Next action that runs Pebble and opens Review Night after success. Older weekly result links fall back to season
totals; stored weekly projections remain part of the published data schema.

Season and nightly result details show the worker's recorded pre/post played
minutes beside the existing pre/post bench minutes. Zero is displayed as `-`;
a missing played half is labelled Not recorded, without splitting or inferring
from the total. The recorded per-row adjustment flag retains its existing general
explanation; no per-half availability or private provenance is invented. The
existing atomic completed-run loader supplies the fields with no extra reads.

Played-breakdown coverage checks recorded values, zero/missing halves,
public/officer visibility, period changes, keyboard details and narrow
layout. The illustrative sample calculation supplies its own played halves only
in sample mode. Published attendance, possible time and weekly P/B/O appear in
season details; explanation and weekly tables use disclosures. Safe availability
explanations, private officer diagnostics on Review Night and shareable links are implemented
and accepted in #11.

Officer Settings edits the existing calculation parameters in Pacific time. Missing
settings show the worker defaults; they are written only after an explicit edit
and save. Saves use the document version, preserve stale drafts on conflicts and
leave results unchanged until Run Pebble. Public and sample views do not expose
this editor. Unit and isolated browser tests cover defaults, validation, guarded
navigation, targeted saves, conflict recovery and narrow layouts.

Result links identify a view within the deployment's configured season:

- `?view=results&period=season` — season totals.
- `?view=results&period=2026-10-14` — a recorded raid night.
- Older `period=week%3A2026-10-13` links fall back to season totals with a notice.
- Add `&player=player-id` to expand that published player's details.

The existing selector and detail controls update browser history; refresh and
Back/Forward restore the selection. Linked selections wait through pending reads
and read failures. After completed results are ready, missing periods fall back
to season totals and missing players to the period's full table, with an explicit
message. Links follow the latest completed publication rather than pinning a run.

Copy result link copies only the public view/period/player selection. Connected
links exclude sample mode, configuration and auth parameters; explicit sample
mode produces a sample link. The existing draft dialog and pending-save guard
also protect browser Back/Forward, keeping both the editor and address unchanged
when navigation is canceled or a save is still pending. Result navigation and
sharing create no input writes or run requests. UI-3 attendance, explanations,
private diagnostics and parity acceptance are complete; see #11 and #14.

Season player rows optionally include the established materialized ranking:
`ranking: {rank, played, bench, ratio}`. Explicit `null` excludes the main from
season rankings; absence means an older publication has no recorded ranking
metadata. The frontend follows recorded season ranks, including zero-time ranked
mains, rather than inferring eligibility from the live roster or attendance.
Equal bench totals retain the established binary name order and sequential ranks.
Night views retain their recorded coverage and existing visibility. Weekly
projections remain in the data contract for attendance; they are not selector options.

The Time Played bar and percentage show played / (played + bench); zero total
time says No recorded time. The redundant numeric Bench:played ratio is not
shown. Published ranking metadata remains unchanged by presentation sorting.
Played share and attendance retain separate labels and meanings. The worker
projection uses existing materialized ranking documents; parity checks verify
ranking minutes equal the existing season player sums across representative cases.

Ranking acceptance covers actual calculation/materialization for active, former,
future-join, zero-played, tied and no-Mythic cases; component/browser checks cover
recorded eligibility/order, undefined ratios, historical night/week visibility,
keyboard details and narrow layout. New ranking metadata requires a future normal
successful worker run after the reviewed worker source is deployed. This change
does not trigger a live run or deploy anything.

### Officer access management

Current officers can open **Officers** to see enabled accounts and pending
invitations, invite a Google account, cancel an invitation, or remove access.
Recipients accept after Google sign-in, using the shared link or a matching
invitation shown on normal sign-in. All officers may manage access; the last
one cannot leave. Invitations last seven days and are delivered manually.

Install callable dependencies with `npm ci --prefix firebase/functions` from the
repository root before running browser acceptance (which now starts the Functions
emulator too). Local Functions uses port 5001; browser acceptance uses 5101.
See [deployment, recovery, and cost controls](../../docs/officer-access.md).
