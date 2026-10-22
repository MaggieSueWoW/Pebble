# Pebble web app

React + TypeScript frontend for Reports, Overrides, Results, Roster and Settings.
Run Pebble is available in the sidebar on every officer page. Google officer login and Firestore are the only hosted services;
the Python bot continues running locally. Successful runs automatically update
public results. There is no approval or publish step.

## Local setup

Requires Node.js 22.12+ on the 22.x line, or Node.js 24+.

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

The web rollout tracker (#14) contains the implementation backlog
and acceptance status. Architecture references cover stack/workflow (#2),
Firestore/security (#3), and officer/public UX (#4).

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

Roster lists Player, Joined, Left and comma-separated Alts. Each data column sorts;
Alts uses the alphabetically ordered visible names. Empty dates and alt lists sort
last in either direction. Expanded details combine main editing, alt editing,
adding/deleting alts, main deletion and a confirmed main/alt swap. Swaps preserve
the player ID, membership, class, history and the promoted character's mapping.
Roster deletion requires the accompanying Firestore rules to be released; a frontend
release alone does not enable it in an environment with older rules.

Every sortable list marks its active direction, including the default. Sorting is
local to the displayed list and makes no writes or run requests. Settings field
labels and detailed descriptions are editable in `src/calculationFields.ts`.

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

Published result warnings appear on Results for officers and public readers. They
stay with the displayed completed run during loading or failure, change with a
successful result replacement, and disappear when the new result has no warnings.
Warning text is shown as recorded, without new source fetches.

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

Overrides tabs use Left/Right and Home/End to move focus without changing editors.
Enter/Space or a click activates the focused tab through the existing draft/save
guards. Both tabs link to the visible, labelled tab panel; only one tab is in the
keyboard tab order. Acceptance checks keyboard focus, Escape/discard restoration,
pending-save activation, panel relationships and narrow layout.

When a completed result has warnings, officers can follow Source reports for this
run links using that run's recorded titles and report codes. The existing private
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
Run Pebble is available in the sidebar on every officer page. Older weekly result links fall back to season
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
explanations, private officer diagnostics and shareable links are implemented
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
