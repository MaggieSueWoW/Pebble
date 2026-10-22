# Web hosting and release (WEB-1)

Work targets `web-app`. Hosting commands do not restart, replace or
reconfigure either worker, MongoDB or Redis. Main stays unchanged. Review the PR
before the first hosted release; this runbook is a release procedure, not evidence
that the live or preview URL has been published or accepted.

## Environments and authority

| Environment | Browser/backend | Credentials and writes |
| --- | --- | --- |
| Local demo | `?demo=1`, illustrative localStorage data | No Firebase or worker |
| Automated acceptance | Demo project, loopback Auth/Firestore emulators, synthetic accounts/data | No live credentials; disposable services for worker faults |
| Hosted test | Separate real Firebase test project and isolated worker/data | Public test web config; administrator maintains test Auth/rules |
| Hosted live | Intended Firebase project/season and existing designated web worker | Public live web config; normal officer writes reach live inputs/queue |

A Hosting preview channel isolates frontend versions, **not backend data**. A
preview built with live config shares live Auth, Firestore and the chosen worker.
Use a separate Firebase project/worker for destructive or synthetic acceptance.
Do not point an isolated worker at live Firestore. Do not stop the live worker to
manufacture an offline scenario. The existing Sheets bot remains independent.

Only Firebase web app fields enter a release. Service-account keys, WCL credentials
and administrator/worker configs stay outside the browser build. Use an existing
administrator Firebase CLI login for manual deployment. Do not grant the worker
Hosting, rules, Auth or IAM administration permissions. CI has read-only repository
permissions, synthetic config and no deployment secrets or publishing step.

## Prepare a release

Use Node 24+, Java 21+ for emulators, and Firebase CLI 15.22.4 (validated version).
Install `apps/web` dependencies with `npm ci`. Copy
`deploy/web-release.example.json` to ignored `deploy/private/web-release.json`.
Fill in public config for the exact project, Hosting site and season. Set
`environment` to `live` or `test`; the marker describes the chosen config and does
not isolate resources. Obtain the API key, app ID and Auth hostname from that
project's Firebase web app settings. Never put server credentials in this file.
Keep actual project/account identifiers and hosted URLs in private release records,
not repository documentation, issue bodies or PR text.

From a clean committed checkout of the reviewed source:

```sh
node scripts/web-release.mjs deploy/private/web-release.json build
node scripts/web-release.mjs deploy/private/web-release.json check
```

The build supplies an allowlist of public variables, forces emulator use off and
disables local dotenv loading. It creates `apps/web/dist/release.json` containing
the source commit, environment, public-config digest and output-file hashes.
`check` rejects source/config drift, changed/extra output files, symlinks and
recognizable server credentials. These checks supplement review; do not treat a
pattern scan as proof that arbitrary private data is safe to publish.

The entrypoint, release metadata and SPA deep links use `Cache-Control: no-cache`.
Content-hashed assets use a one-year immutable cache lifetime. There is no service
worker; a reload revalidates HTML and loads the corresponding hashed assets.
An already-open tab continues its current bundle until reloaded.

## Preview, release and rollback

Preview the exact checked artifact:

```sh
node scripts/web-release.mjs deploy/private/web-release.json preview review
```

This creates/updates an expiring channel. It passes `--no-authorized-domains`:
the Auth administrator must review and authorize the exact preview hostname before
Google sign-in acceptance. Do not assume a configured Google provider proves
consent works at a new host. Live default Firebase domains may already be authorized;
custom domains must be checked too. Check any API-key HTTP-referrer restrictions.

Record the previous live Hosting version ID, source commit and public config before
replacing an existing release. Confirm that the previous version is retained under
the site's Hosting version-retention policy; keep a private copy of each checked
artifact and config as an additional recovery option. Then release and verify:

```sh
node scripts/web-release.mjs deploy/private/web-release.json deploy
node scripts/check-web-host.mjs https://HOSTED_ORIGIN EXPECTED_FULL_COMMIT deploy/private/web-release.json
```

The preview/deploy commands first verify the site belongs to the explicit project.
The deploy command stages only verified static files and a Hosting-only config,
passes an explicit project and site, and invokes `firebase deploy --only hosting`.
It does not deploy Firestore rules/indexes or change Auth domains. Before the first
release there is no prior version to restore; record that condition rather than
inventing a rollback target. For later releases, restore a retained version to the
**existing live channel** (version-source syntax verified with CLI 15.22.4):

```sh
firebase hosting:clone SITE_ID@PREVIOUS_VERSION_ID SITE_ID:live --project PROJECT_ID
node scripts/check-web-host.mjs https://HOSTED_ORIGIN PREVIOUS_FULL_COMMIT deploy/private/previous-web-release.json
```

Do not create a backup preview channel through `hosting:clone` as part of this
procedure: the CLI automatically adds newly created channel domains to Auth.
Restoring a version to the existing live channel avoids that side effect. If the
old Hosting version was deleted, rebuild the previous committed source using its
recorded public config, check the resulting artifact and redeploy it through the
same explicit release command. Keep that source/config available before publishing.

Firebase's [channel/release documentation](https://firebase.google.com/docs/hosting/manage-hosting-resources)
explains how channels, releases and retained versions relate. The
[Hosting configuration](https://firebase.google.com/docs/hosting/full-config) and
[cache documentation](https://firebase.google.com/docs/hosting/manage-cache) describe
SPA rewrites and headers. Retain the private config and release record alongside
source revisions so both release and rollback can be repeated.

## Firestore rules and index

Use the reviewed `firebase/firestore.rules` and `firebase/firestore.indexes.json`.
Inspect the target's deployed source and ready queue index first; a Hosting release
does not change them. If a reviewed rule/index deployment is required, run it as a
separate, explicit administrator operation:

```sh
firebase deploy --only firestore:rules,firestore:indexes --project PROJECT_ID --config firebase.json
```

Verify the released rules match source and the queue index is ready. Record the
previous ruleset/index definitions before changes. A frontend rollback does not
roll back data schemas, rules, indexes, officer membership or a worker. Do not run
`init-season`, import Sheets or reseed live data as part of a static release.

## Hosted acceptance and closure evidence

Record the Hosting version/channel, source commit, environment, authorized hostname,
prior version/rollback target and HTTP/cache check results privately. Issue summaries must
omit private identifiers. `check-web-host.mjs` only makes HTTP GET requests; it does
not sign in, edit inputs or submit a worker request. Supply the intended release's
public config file so the check rejects a different Firebase/site/season build of
the same source commit. For rollback, use the previous release's recorded config.

In Chrome, verify the shareable HTTPS URL and a deep link load without local files.
Confirm `/release.json` matches the intended source, a reload loads the new bundle,
and an old open tab transitions correctly after reload. Compare deployed HTML/assets
and cache headers, not just a successful upload message.

Complete AUTH-1 (#8) on the hosted release: real Google consent/login, signed-out
public results, signed-in non-officer denial, officer access, wrong-account switching,
popup blocked/cancelled recovery, refresh, and revocation during editing. Confirm
server denial for private reads/writes after revoke and continued public results.
Use the documented administrator grant/revoke procedure; restore intended access.
The passing local real-account, browser-emulator and isolated queued-revocation
checks remain useful evidence but do not substitute for this hosted check.

For WEB-1, prove officer writes and a request submitted from the hosted URL reach
the chosen worker and publish a complete result. Use an intended normal run when
live operations are authorized, or a hosted test project with isolated worker/data.
Do not mutate live inputs solely to manufacture test results. Keeping the live
worker untouched means no stop/restart/reconfiguration for this acceptance.
Record observed progress, terminal status and complete public pointer, then verify
public results in a separate signed-out context. Leave #12/#8 open until their
respective hosted acceptance is actually observed.

## Checks without deployment

```sh
node --test scripts/web-release.test.mjs
npm --prefix apps/web test
npm --prefix apps/web run test:browser
```

The GitHub Actions workflow runs Python, web build/tests, release checks, a Hosting
emulator HTTP smoke and browser acceptance. Rules and worker emulator tests run
sequentially when using the same demo project, because rules tests clear its data.
It never publishes a release. Lifecycle tests requiring separate disposable Mongo/
Redis services remain opt-in; skipped fault tests are not hosted acceptance.
