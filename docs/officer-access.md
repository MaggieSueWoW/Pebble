# Officer access

Google sign-in stays the only sign-in method. All enabled officers have equal,
project-wide authority to invite or remove officers. The Officers page lists
names, Google account emails, grant attribution, and pending invitations. These
records and the audit collection are readable only by current officers. A user
can still read their own membership after revocation to observe access loss.

## Invitation lifecycle

1. An officer enters a Google account email and confirms the access grant.
2. The callable creates a random invitation ID valid for seven days. Duplicate
   active invitations return the existing link. The officer copies and shares it;
   no email delivery service is involved.
3. A signed-in recipient can inspect their own invitations or a specific linked
   invitation. A wrong account sees the expected email and can switch Google
   accounts. Possessing a link alone never grants access.
4. Acceptance requires a verified Google sign-in email matching the invitation
   (trimmed and lowercased, without Gmail alias/dot rewriting). It atomically
   enables `officers/{uid}`, consumes the invitation, and records an audit event.
   The inviting officer must still have access at acceptance time.
5. Officers can cancel invitations or remove access. Removing an officer also
   cancels their unaccepted invitations. Existing promoted officers keep access.
   Expired and cancelled links cannot be reused; renewals get new IDs.

The app observes membership changes through its existing server-confirmed
listener. Revocation clears private workspace data and returns to public results.
The callable trusts verified Firebase authentication context, never client-supplied
identity. All membership mutations share the `officerManagement/state` transaction
lock and recheck the caller inside the transaction. This serializes concurrent
removals and ensures at least one officer remains, independently of UI checks.
No browser may write membership, invitations, audit, or the lock.

`officerAudit` is append-only through the callable and records actor, action,
target/invitation, and server time. `officers` retains disabled membership and grant
metadata. An account's display identity refreshes on sign-in only when changed.
Expired invitations are rejected by server time, without a scheduled cleanup job.
The page labels expired invitations and allows cancellation.

## Deployment and cost controls

Requires a Firebase Blaze billing account, Node.js 22, and Firebase CLI 14+.
Use an explicitly selected project with administrator deployment credentials:

```sh
npm ci --prefix firebase/functions
scripts/deploy-officers.sh FIREBASE_PROJECT_ID
```

The script deploys only the `officers` Functions codebase and Firestore rules, then
applies a one-day Artifact Registry image cleanup policy in `us-central1`.
Deployment uses `--force` so Firebase can create its initial image cleanup policy
without exiting before the explicit retention step.
Cleanup affects old deployment images, not deployed running functions. A cleanup
failure fails the script; fix it before considering rollout complete. This is a
repository-wide policy for function images in that region, so review it if other
function codebases are added later. Hosting must also be built/deployed using the
normal [web hosting workflow](web-hosting.md).

The single `officerAccess` second-generation callable has explicit `minInstances: 0`,
`maxInstances: 2`, 256 MiB memory, fractional CPU, concurrency 1, and a 30-second
request timeout. The web client uses the same fixed `us-central1` region. No warm
instances, polling function, scheduled expiry job, or paid email service is used.
Cold starts are acceptable for this low-volume administration flow. Instance
limits constrain scale but are not a hard dollar spending cap.

## Existing officers and recovery

Before rollout, backfill existing UID-only memberships using administrator ADC
with Firebase Authentication read and Firestore write permissions:

```sh
node firebase/functions/seed-identities.js FIREBASE_PROJECT_ID
```

The script updates only display name and email; it does not grant or revoke
access. Missing/deleted Auth accounts are counted without exposing identifiers.
If not backfilled, existing officers acquire identity metadata on their next
sign-in; the directory labels missing identity until then. Administrator-origin
grants are displayed as such when no historical grant timestamp is available.

For the first officer, or if every enabled account is inaccessible, use the
administrator `web set-officer` CLI described in
[bootstrap and recovery](firestore-setup.md#officer-onboarding-and-revocation).
Verify the account in Firebase Authentication first. This trusted recovery path
bypasses application protections; do not run it concurrently with normal access
administration. Keep credentials out of the browser and the repository.

Existing logical backups include membership documents. Pending invitations and
callable audit records are additional collections; preserve them separately with
an administrative export if their history is required during disaster recovery.
Cancel/reissue invitations after a membership restore rather than relying on old
links.

## Verification

Install dependencies in `firebase/functions`, `firebase`, and `apps/web`. Use only
the demo project for emulator tests:

```sh
firebase emulators:exec --project demo-pebble-reports --config firebase.browser.json --only auth,firestore,functions 'npm --prefix firebase/functions test'
firebase emulators:exec --project demo-pebble-reports --config firebase.browser.json --only auth,firestore,functions 'cd apps/web && npx playwright test e2e/officers.test.ts e2e/auth.test.ts'
```

Callable integration tests exercise actual Firestore transactions, Google-only
identity checks, email binding, expiry, cancellation, duplicate creation, audit
attribution, stale acceptance replay, and concurrent final-officer removal.
Firestore rule tests cover private directory/invitation/audit reads and denied
browser writes. Browser tests cover the real callable, multiple Google sessions,
normal sign-in discovery, wrong-account recovery, expiration, and live revocation.
