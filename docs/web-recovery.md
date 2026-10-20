# Web Firestore recovery

`pebble web backup-firestore` makes a small logical backup of one configured
season during an idle period. `restore-firestore` restores and verifies it in a
different, completely empty project. Neither command starts a worker, submits a
run, contacts WCL/Mongo/Redis, or changes the legacy bot.

## Scope

The backup contains all project officer-membership documents, the configured
season root, reports/roster/mappings/availability/timing/settings, terminal run
history, complete result sets with their child collections, and that season's
public pointer. Incomplete staging and derived `bot/status` health are omitted.
Other seasons are outside this command's scope.

Firestore values use the existing SDK's protobuf representation, preserving
native types and timestamp nanoseconds rather than converting dates to plain
strings. Document create/update metadata is assigned by the destination; stored
fields such as `updatedAt`, attribution and captured input versions are preserved.

These files contain private notes, membership IDs and officer-only run metadata.
The command creates a new file exclusively with mode **600**, refuses overwrites
and prints only a document count. `.pebble-backups/` is ignored by Git. Keep the
parent directory protected and include these files in the separately protected
host/off-host backup arrangement; do not attach them to issues or PRs.

This is a quiescent logical backup, not a managed point-in-time Firestore export.
The operator must avoid edits, imports and administrative cleanup while it runs.
It refuses queued/running requests and rechecks officer/input/run/pointer state
before writing a file. Completed results are read once under their immutable
publication contract. A changed source causes a failed backup, not a partial file.
There is no need to stop an idle worker; heartbeat updates are excluded.

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
ID and document paths; it does not rename or migrate a season.

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
