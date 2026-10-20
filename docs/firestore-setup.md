# Firestore and local worker setup

See the web rollout tracker (#14) for current status,
remaining deployment host deployment work, and spreadsheet parity.

## Current environment

- Firebase project: `YOUR_FIREBASE_PROJECT_ID` (Pebble), managed by the project administrator.
- Season ID: `midnight-s2`; display name: Midnight S2.
- Firestore location: `us-west1`; Firebase Authentication uses Google sign-in.
- First officer: `OFFICER_ACCOUNT`, with membership keyed by Firebase Auth UID.
- Existing S2 Sheets inputs were copied into Firestore; the original Sheet is unchanged.

The frontend talks directly to Firebase Authentication and Firestore. No hosted
API is required. Rules validate browser writes and restrict them to enabled
officers. The local worker validates the full input snapshot, fetches WCL data,
uses the shared Python engine, and uploads public-safe results.

## Run locally

Install Python 3.12+, local MongoDB and Redis, and the Python project dependencies.

```sh
python3.12 -m venv .venv
.venv/bin/pip install -e '.[dev]'
cp config-web.yaml.example config-web.yaml
```

Configure `firestore.project_id`, `season_id`, and optional `credentials_file` in
`config-web.yaml`. Put WCL_CLIENT_ID and WCL_CLIENT_SECRET in `.env`. The config
loader also accepts FIREBASE_PROJECT_ID and PEBBLE_SEASON_ID overrides.
MONGODB_URI and REDIS_URL override their local connection URLs.

The worker uses Application Default Credentials or a local service-account JSON.
The current worker identity has `roles/datastore.user`. Keep credential JSON,
`.env`, and local config out of Git. Use a separate administrator identity to
manage Firebase Authentication, officer membership, IAM, and rule deployments.

```sh
.venv/bin/python -m pebble.cli web worker --config config-web.yaml
```

Leave this process running. A Firestore snapshot listener wakes it when queued
requests change; it handles one run at a time and sleeps between notifications.
The SDK resumes transient stream failures. If the listener stops, the worker
recreates it with exponential retry delays from one second up to one minute;
the initial snapshot picks up requests queued while disconnected. Startup and
processing connection failures are retried too. Local listener-health checks do
not query Firestore. The sole periodic Firestore write is a heartbeat once per minute for the UI;
starting and finishing runs do not add heartbeat writes. The UI allows 150 seconds
before showing the bot offline.
SIGTERM requests a graceful stop after the current run, with up to ten seconds of
idle shutdown latency. Ctrl-C closes the listener; an interrupted run is recovered
on the next start. `--once` processes at most one request without a listener. Queued requests wait while it is offline. A restarted
worker marks interrupted runs failed; an officer can click Run Pebble again.
The previous complete results remain visible after a failed run.

MongoDB computation data uses a dedicated season database derived from the config
base name, keeping it separate from the Sheets pipeline. Redis caches WCL calls.
Input snapshots remain private in local MongoDB; run records include input versions.

## Officer onboarding and revocation

Use a separate administrator identity with Firestore IAM write access to manage
`officers/{uid}`. Browser officer membership does not grant that authority, and
browser rules deny membership writes, including self-enrollment. The CLI uses
trusted server credentials and bypasses browser rules. Keep the administrator
credential separate from the worker credential; review its IAM scope with OPS-1.
The examples below use a private `config-web-admin.yaml` configured with the target
`firestore.project_id` and the administrator's `credentials_file` (or administrator
Application Default Credentials). Never commit that config or its credentials.
Check any `FIREBASE_PROJECT_ID` override before running the command. Membership is
project-wide, not limited to the config's season.

1. Have the new officer sign in with the intended Google account once. A signed-in
   non-officer sees an access notice and their Firebase UID in the app. Verify the
   account and UID against Firebase Authentication in the target project's console;
   use the Firebase UID, not their email address or Google account ID.
2. Enable membership using the administrator config:

   ```sh
   .venv/bin/python -m pebble.cli web set-officer --config config-web-admin.yaml --uid FIREBASE_AUTH_UID --enabled
   ```

3. The open app should load officer inputs automatically. Verify the officer can
   save an intended input change and queue a run; use a demo project for acceptance
   checks that should not change live inputs.
4. Revoke access with the same UID and target project:

   ```sh
   .venv/bin/python -m pebble.cli web set-officer --config config-web-admin.yaml --uid FIREBASE_AUTH_UID --disabled
   ```

5. With that account still signed in and a draft open, confirm the app switches to
   public results, removes the private forms/history and discards the draft without
   a refresh. Private server reads/writes and new queued requests must be denied.
   Completed public results must still work for this user and a signed-out user.
   Re-enable only if intended; the app loads fresh inputs without restoring drafts.

The app clears private state when it observes membership loss or a private
permission-denied error. Membership-listener errors also close officer editing and
show an error; refresh to reconnect if that listener has terminated. Account changes
and sign-out invalidate prior callbacks and pending save/run UI state. An already
submitted write is governed by server rules, and the worker rechecks membership
before claiming a queued run. Revoking membership does not cancel a run already
claimed by the worker or remove the user's Google/Firebase account.

Automated acceptance covers hook/component state cleanup, late callbacks/actions,
re-grant, auth changes and server-side revocation rules. Real second-officer Google
onboarding/revocation, popup errors and hosted-domain acceptance remain pending;
see AUTH-1 status (#8).

## Initialize a different project or season

Create a Firebase project and web app, enable Google sign-in and Firestore, and
add the app hosts to Authentication's authorized domains. Copy the new public web
configuration into `apps/web/.env.local`, and update the worker config. Frontend
season variables are `VITE_PEBBLE_SEASON_ID` and `VITE_PEBBLE_SEASON_LABEL`.
Restart Vite or rebuild after changing browser environment variables.

Deploy rules and the queue index with an explicit project ID:

```sh
firebase deploy --only firestore --project YOUR_PROJECT_ID
.venv/bin/python -m pebble.cli web init-season --config config-web.yaml --name 'Midnight S2'
```

Officer membership is administered separately from browser sign-in; see
[onboarding and revocation](#officer-onboarding-and-revocation) above.

To import existing Sheets inputs without changing the Sheet:

```sh
.venv/bin/python -m pebble.cli web import-sheets --config config-web.yaml \
  --sheets-config PATH_TO_SEASON_CONFIG --sheets-credentials PATH_TO_SHEETS_CREDENTIALS
```

This creates missing input documents and updates calculation settings. It does not
replace existing reports, roster entries, or overrides. It preserves positive
available-minute and negative unavailable-minute overrides and normalizes Sheets
time formats. Both import paths validate the effective season before committing
missing inputs in one transaction (at most 400 writes). Invalid references or
legacy values fail without partial writes. See [data contracts and parity](data-consistency.md)
for supported formats, regional aliases, size bounds and manual retention.
Importing does not start a run. The generic `web import-inputs
--file inputs.json` command can load validated input collections from JSON.

There is no hardcoded backend project dependency. Changing projects requires new
credentials/membership and migrating any desired data; changing IDs alone does
not copy existing data.

## Verify with emulators

Use an isolated demo project. Keep live `.env.local` unchanged unless deliberately
switching the browser to emulator mode. Emulator browser configuration uses
`VITE_FIREBASE_USE_EMULATORS=true`, an API key such as `demo-key`, and the demo
project ID. Use the Authentication emulator UI to create test users; seed their
membership with administrator SDK/emulator credentials.

```sh
firebase emulators:start --only auth,firestore --project demo-pebble
cd firebase
npm ci
npm test
```

From the repository root, with the emulators running:

```sh
FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 .venv/bin/python -m pytest -q
```

The Python emulator integration test exercises snapshot capture, claims,
revocation, recovery, and atomic public-pointer updates. Rules tests cover
anonymous/non-officer denial, officer validation, versions, and incomplete output.
Frontend checks run with `npm test` and `npm run build` in `apps/web`.

## Static hosting

```sh
cd apps/web
npm ci
npm run build
cd ../..
firebase deploy --only hosting --project YOUR_PROJECT_ID
```

Hosting serves only the frontend. The local worker must still run to process
requests. Published results remain readable while it is offline. Officer notes,
raw input records, private run errors, credentials, and UIDs are excluded from
public result documents.
