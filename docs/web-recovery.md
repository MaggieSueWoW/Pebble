# Web Firestore recovery

`pebble web backup-firestore` makes a small logical backup of one configured
season during an idle period. `restore-firestore` restores and verifies it in a
different, completely empty project. Neither command starts a worker, submits a
run, contacts WCL/Mongo/Redis, or changes the legacy bot.

## Scope

The backup contains all project officer-membership, invitation, callable-audit
and recovered invitation-history documents, the configured
season root, reports/roster/mappings/availability/timing/settings, terminal run
history, complete result sets with their child collections, and that season's
public pointer. Incomplete staging and derived `bot/status` health are omitted.
Other seasons are outside this command's scope.

Firestore values use the existing SDK's protobuf representation, preserving
native types and timestamp nanoseconds rather than converting dates to plain
strings. Document create/update metadata is assigned by the destination; stored
fields such as `updatedAt`, attribution and captured input versions are preserved.

These files contain private notes, identities, invitation links, audit attribution
and officer-only run metadata.
The command creates a new file exclusively with mode **600**, refuses overwrites
and prints only a document count. `.pebble-backups/` is ignored by Git. Keep the
parent directory protected and include these files in the separately protected
host/off-host backup arrangement; do not attach them to issues or PRs.

This is a quiescent logical backup, not a managed point-in-time Firestore export.
The operator must avoid edits, imports and administrative cleanup while it runs.
It refuses queued/running requests and rechecks officer membership, invitation,
audit, recovered-history, input, run and pointer state
before writing a file. Completed results are read once under their immutable
publication contract. A changed source causes a failed backup, not a partial file.
There is no need to stop an idle worker; heartbeat updates are excluded.

## Officer history and retention policy

Each backup includes **all available** project-wide `officerInvitations` and
`officerAudit` documents, without filtering by age, status or season. It also
includes `officerInvitationHistory` from any prior recovery. There is no TTL,
automatic pruning, or live-data deletion in these tools. Take a fresh protected
backup after officer-access changes and before planned recovery operations.
Keep the two most recent verified recovery generations in the protected off-host
backup arrangement, plus the last pre-incident generation until recovery is
accepted. Verify a replacement before deleting an older copy. These are manual
retention instructions; the CLI does not schedule exports or upload files.

Restore preserves every audit record and its attribution exactly. Invitations
are preserved with their original IDs, statuses, timestamps and fields under
`officerInvitationHistory/{id}`, **never** in `officerInvitations/{id}`. This
administrator-only collection is denied to every browser role by the existing
rules and is not read by the officer callable. Thus even a formerly pending
invitation cannot grant access in the recovered project; consumed, cancelled,
expired and revoked-inviter links remain unusable. Existing history is retained
on subsequent backup/restore cycles. A collision between a live invitation ID
and an existing history ID fails restore before any writes, rather than silently
discarding either record; retain the original archive for administrative review.

Memberships are restored as captured, including disabled memberships; a backup
predating a revocation cannot contain that later decision. Before any real
cutover, reconcile membership grants/revocations since the snapshot and review
the complete history. Cancel/reissue any required invitations with fresh links
through the normal officer workflow. Never copy recovered invitations back into
the active collection. The original source project and its links are untouched
by this tooling; retiring its access requires a separate operational decision.

New archives use `pebble-firestore-backup/v2`. Older v1 files do not contain the
complete officer history and are rejected by this restore command. Keep their
protected copies and use the matching historical tooling only for separately
reviewed recovery; take a fresh v2 backup for current rehearsals.

## Create a backup

Use the existing protected worker/admin configuration and Firestore credentials.
The output directory must already exist; choose a new filename each time.
For example, from the repository with its Python environment active:

```sh
mkdir -m 700 .pebble-backups
pebble web backup-firestore \
  --config /protected/source-web.yaml \
  --file .pebble-backups/season-backup-01.json
```

For a real source, ensure `FIRESTORE_EMULATOR_HOST` is unset. For a demo source,
set it explicitly to the intended isolated emulator. Do not print configuration
or credential contents to check this. On failure, resolve access/configuration or
wait for an idle period and retry with a new output filename.

## Restore verification in isolation

Create a protected target configuration with a distinct `demo-` project ID,
the original season ID, and disposable placeholder Mongo/WCL settings. Only
Firestore configuration is used by these commands. Restore preserves the season
ID and document paths except for the deliberate invitation-to-history mapping
above; it does not rename or migrate a season.

With the dedicated Firestore emulator already running on its assigned port:

```sh
FIRESTORE_EMULATOR_HOST=127.0.0.1:8182 \
  pebble web restore-firestore \
  --config /protected/recovery-web.yaml \
  --file .pebble-backups/season-backup-01.json \
  --target-project demo-pebble-recovery
```

The explicit project must match the target configuration and differ from the
backup source. The destination Firestore must have no collections, including
orphaned child documents. All archive paths/payloads and publication-pointer
consistency are checked before writes. Batches use create-only preconditions;
existing documents are never overwritten, even if another writer races the
empty-target check. Document/batch budgets reuse the publication limits.

After creating the documents, the command reads the destination and compares
exact encoded field values and document counts against the archive. It succeeds
only after that verification. Record the protected backup's age and the restore
result/count in the recovery inventory without copying document contents.
Do not start a worker or connect the app to the destination during the rehearsal.

A failed multi-batch restore can leave a partial destination for inspection.
The tool never clears it or resumes into it: use another fresh empty destination
for the next attempt. For a real replacement project, the same explicit guards
apply; actual recovery writes require their own operational authorization. This
command cannot overwrite or repair an existing production project in place.

## Separate recovery items

Firestore officer membership is **not Firebase Authentication**. Authentication
accounts need separate recovery with preserved UIDs, or reviewed membership
recreation. Rules, indexes, hosting/auth-domain setup, credentials and private
runtime configuration also need their own protected copies and restore steps.
This command does not export or deploy them.

Mongo web materializations can be rebuilt from Firestore inputs and logs; Redis
is a cache. Historical Mongo `input_snapshots` are separate audit data and need
separate retention/backup when required. A Firestore restore does not recreate
those historical snapshots or restore the legacy Sheets database.

Review the recovered project, permissions and complete public result before any
worker start or app cutover. Backup rehearsal and a prior-image rollback are
separate checklist items in #5; final hosting and editing authority remain #14.
