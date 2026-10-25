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

## Callable dependency review (#52, 2026-10-25)

The callable now requires `firebase-admin ^14.5.0` and
`firebase-functions ^7.4.0`. The lockfile resolves Admin 14.5.0, Functions 7.4.0,
Firestore 9.4.0 and Storage 8.4.0. Updates use the supported upstream dependency
ranges, without forced audit fixes or transitive overrides. The deployment runtime
remains Node 22.

Reviewed compatibility changes:

- [Admin 14](https://firebase.google.com/support/release-notes/admin/node)
  requires Node 22+, removes legacy namespace/Instance ID/messaging APIs and
  expands error handling. This code already uses modular imports and none of
  those removed services. The identity-seeding helper's `auth/user-not-found`
  check remains valid under the documented
  [service-prefixed error codes](https://firebase.google.com/docs/reference/admin/error-handling).
  Callable authorization continues to throw Functions `HttpsError` values.
- [Functions 7](https://github.com/firebase/firebase-functions/releases/tag/v7.0.0)
  removes `functions.config()`, Node 16 support and the v1 `Event` type, raises
  TypeScript requirements and changes unhandled async `onRequest` emulator
  errors. This is JavaScript ESM using v2 `onCall`, without runtime config or v1
  events. Functions 7.4.0 explicitly supports Admin 14 as a peer dependency.
- [Firestore 8](https://github.com/googleapis/nodejs-firestore/releases/tag/v8.0.0)
  changes TypeScript data types and requires Node 18+. The subsequent
  [Firestore 9](https://github.com/googleapis/google-cloud-node/blob/main/handwritten/firestore/CHANGELOG.md),
  [Storage 8](https://github.com/googleapis/google-cloud-node/blob/main/handwritten/storage/CHANGELOG.md)
  and [Google Auth 11](https://github.com/googleapis/google-cloud-node/blob/main/core/packages/google-auth-library-nodejs/CHANGELOG.md)
  major releases require Node 22+. The callable uses ordinary Firestore
  transactions, queries and timestamps; it does not use Storage APIs or custom
  auth transports.

Before updating, `npm --prefix firebase/functions audit --omit=dev` reported
8 moderate findings and no high/critical findings. All eight package entries
traced to [GHSA-w5hq-g745-h8pq](https://github.com/advisories/GHSA-w5hq-g745-h8pq):
`uuid` v3/v5/v6 methods can silently partially write a caller-provided buffer with
invalid bounds. Affected paths ran through Admin's Firestore → google-gax and
Storage → retry-request/teeny-request, with gaxios also depending on uuid. The
callable does not directly use uuid or accept UUID output buffers from clients;
this limits direct exposure, but retaining vulnerable transitive packages was
unnecessary. The upgraded tree contains no uuid package, and a fresh production
audit reports **0 vulnerabilities**. No advisories remain to defer.

Validation used the isolated demo emulators with Firebase CLI 15.22.4 and Node
24.11.1: 8 callable transaction tests, 24 Firestore rules tests, and 7 officer/auth
browser tests. These cover Google-only verified identities, email-bound
acceptance, audit attribution, stale replay, concurrent final-officer protection,
private reads, denied browser authority writes, invitation recovery and live
revocation. The deployment-script test also passes. Inspection of the generated
callable endpoint confirms region `us-central1`, 0 minimum/2 maximum instances,
256 MiB memory, `gcf_gen1` CPU, concurrency 1 and a 30-second timeout.

This is a dependency maintenance change. Backend deployment is a separate
operation requiring explicit authorization; emulator checks do not establish
hosted acceptance or change the rollout status of #14.
