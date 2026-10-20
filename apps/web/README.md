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
the last complete result set without login.

See [Firebase and worker setup](../../docs/firestore-setup.md) for backend setup,
credentials, importing Sheets inputs, emulator checks, and changing projects.

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

Coverage includes report validation/deduplication, add/re-enable, selection
save/cancel, delayed and denied saves, concurrent version conflicts, draft
navigation protection, keyboard dialog focus and narrow-screen overflow.
Availability coverage adds
mode/minute validation, save/cancel/reset, delayed/denied saves, version conflicts,
newer edits during saves, and player/night/tab/page navigation. Each availability
case verifies unrelated reports, timings and player/night overrides stay unchanged.
Roster coverage checks late arrival and clean removal, search-independent selection,
missing class/role labels, and retention of a removed player's draft until explicit
cancel or roster return. The editor waits for pending saves before choosing a
replacement. An empty officer roster stays separate from historical result players.


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


Result-loading coverage holds result reads, rejects incomplete output for officers
and public readers, exercises retry (including first-load errors), and supersedes
a delayed fetch with a newer publication. It checks that rows, result periods and
totals stay consistent, and that an edit saved during computation still needs a
new run after older captured input versions arrive. Queue/completion/publication
transitions are synthetic emulator fixtures; one freshness case submits a single
expected emulator request, with no worker process.
