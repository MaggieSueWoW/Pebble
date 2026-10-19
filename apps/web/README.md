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
