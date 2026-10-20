# Pebble web app

React + TypeScript frontend for reports, availability/timing overrides, results,
and Run Pebble. Google officer login and Firestore are the only hosted services;
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
access see a read-only explanation and a Sign out to switch accounts action.
Account details keeps the setup ID available without showing it in the main message.
Switch Google account opens the account chooser. Login failures show recovery
instructions; failed access verification returns to public results with a retry
action instead of leaving the page connecting indefinitely.

See [Firebase and worker setup](../../docs/firestore-setup.md) for backend setup,
credentials, officer onboarding/revocation, emulator checks, and changing projects.

```sh
npm test
npm run build
```

The web rollout tracker (#14) contains the implementation backlog
and acceptance status. Architecture references cover stack/workflow (#2),
Firestore/security (#3), and officer/public UX (#4).

## Sample mode

Open http://127.0.0.1:5173/?demo=1 for the original standalone sample prototype.
It uses localStorage and illustrative calculations, independently of Firestore.
Prototype controls simulate offline/failing runs and reset sample data. The
production Python fairness engine remains authoritative in the connected app.

Roster and calculation settings have validated Firestore schemas and an import
command; dedicated browser editors for them remain future work.

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

Overrides initially selects the newest known report/result night, or today's date
in Pacific time if none is known. An untouched default follows arriving metadata;
choosing a date or starting an edit keeps that date selected. Custom dates remain
available in the picker. Date coverage checks late metadata, ordered options,
explicit/custom choices, draft-discard guards and held saves on the original night.


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
account-switch action. Published results remain readable while private controls
and unsaved private drafts disappear on revocation. These cases create no requests
or input changes; all sign-in helpers use the visible account-details disclosure.


Empty-results coverage distinguishes no completed result set from a completed
period with no player time. Public guidance stays read-only; officers can use
Manage reports to review inputs without a write or run request. Empty recent
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


Weekly result periods load the worker's existing `weeks` collection in the same
completed-run read as players and nights. All views stay on the previous complete
publication until every collection succeeds; weekly read failure uses the existing
result retry. The selector lists recorded Tuesday game-week IDs, with minutes and
pre/post bench totals copied from the published projection. No weekly calculations,
input writes or new worker output are introduced.

Weekly rows retain recorded zero-time and historical players. Names come from the
weekly output; class/color metadata is used only when the same completed run has
a matching player name. Missing metadata stays unspecified. Weekly output does not
record per-night adjustment explanations, so aggregates omit inferred/adjusted
availability labels and the per-night Edit availability action. Played share remains
a time split, not attendance; zero time is labelled No recorded time.

Weekly acceptance covers stored totals, historical/zero rows, safe metadata mapping,
weekly read failure/retry, officer/public access, keyboard details, narrow layout
and atomic publication replacement. Attendance, possible time, weekly P/B/O, richer
nightly explanations/diagnostics and shareable routes remain tracked in #11.
