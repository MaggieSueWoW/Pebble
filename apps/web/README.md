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
keep these ports free. No worker is started, and tests assert that no runs were
queued. Failure traces stay in ignored `test-results/`.

Coverage includes report validation/deduplication, add/re-enable, selection
save/cancel, delayed and denied saves, concurrent version conflicts, draft
navigation protection, keyboard dialog focus and narrow-screen overflow.
Availability coverage adds
mode/minute validation, save/cancel/reset, delayed/denied saves, version conflicts,
newer edits during saves, and player/night/tab/page navigation. Each availability
case verifies unrelated reports, timings and player/night overrides stay unchanged.


Timing coverage adds save/cancel/reset, blank and detected values, ordering and
paired break validation, delayed/denied commits, version conflicts, newer edits,
and night/tab/page navigation. It also checks Pacific daylight-saving validation,
wall-time presentation in a non-Pacific browser timezone, narrow-screen keyboard
operation and standalone sample mode. Timing cases verify reports, availability,
other nights and the empty run queue stay unchanged.
