"""Small logical Firestore backups for an idle web season."""

from __future__ import annotations

import base64
import json
import os
from datetime import UTC, datetime
from pathlib import Path

from google.cloud.firestore_v1._helpers import decode_dict, encode_dict
from google.cloud.firestore_v1.types import Document

from .web_models import ID, INPUT_MODELS
from .web_publication import publication_batches

FORMAT = "pebble-firestore-backup/v1"
INPUT_COLLECTIONS = (*INPUT_MODELS, "settings")


class RecoveryError(ValueError):
    """An actionable recovery failure whose message contains no private data."""


def _wire(data: dict) -> bytes:
    """Keep SDK-native Firestore types, including timestamp nanoseconds."""
    return Document(fields=encode_dict(data))._pb.SerializeToString(deterministic=True)


def _walk(collection):
    for snapshot in collection.stream(timeout=30):
        yield snapshot
        for child in snapshot.reference.collections(timeout=30):
            yield from _walk(child)


def _controls(client, season_id):
    season = client.document(f"seasons/{season_id}")
    snapshots = list(client.collection("officers").stream(timeout=30))
    for name in (*INPUT_COLLECTIONS, "runs"):
        snapshots.extend(season.collection(name).stream(timeout=30))
    snapshots.extend(
        ref.get(timeout=30)
        for ref in (season, client.document(f"publicSeasons/{season_id}"))
    )
    records = {s.reference.path: s for s in snapshots if s.exists}
    if any(
        s.to_dict().get("status") not in {"succeeded", "failed"}
        for path, s in records.items()
        if path.startswith(f"seasons/{season_id}/runs/")
    ):
        raise RecoveryError("Wait until every run is terminal before backing up.")
    return records


def _fingerprint(records):
    return {
        path: (snapshot.update_time, _wire(snapshot.to_dict()))
        for path, snapshot in records.items()
    }


def _check_pointer(records, season_id):
    pointer = records.get(f"publicSeasons/{season_id}")
    if pointer is None:
        return
    run_id = pointer.to_dict().get("latestRunId")
    result = records.get(f"seasons/{season_id}/results/{run_id}")
    run = records.get(f"seasons/{season_id}/runs/{run_id}")
    if (
        not run_id
        or result is None
        or result.get("complete") is not True
        or run is None
        or run.get("status") != "succeeded"
    ):
        raise RecoveryError(
            "The public pointer must match a complete successful result."
        )


def backup_firestore(client, season_id: str, filename: str | Path) -> int:
    """Read one idle season and officer membership into a new mode-600 file.

    Completed results are immutable. Control documents are re-read to reject
    edits or worker transitions during the export; operators must also avoid
    administrative cleanup during this small logical backup.
    """
    if not ID.fullmatch(season_id):
        raise RecoveryError("Invalid season ID.")
    controls = _controls(client, season_id)
    if not any(path.startswith(f"seasons/{season_id}") for path in controls):
        raise RecoveryError("Configured season has no data to back up.")
    records = dict(controls)
    results = client.collection("seasons").document(season_id).collection("results")
    for result in results.stream(timeout=30):
        if result.to_dict().get("complete") is not True:
            continue
        records[result.reference.path] = result
        for collection in result.reference.collections(timeout=30):
            for child in _walk(collection):
                records[child.reference.path] = child
    _check_pointer(records, season_id)
    if _fingerprint(controls) != _fingerprint(_controls(client, season_id)):
        raise RecoveryError(
            "Source inputs or run state changed; retry during an idle period."
        )
    archive = {
        "format": FORMAT,
        "sourceProject": client.project,
        "seasonId": season_id,
        "createdAt": datetime.now(UTC).isoformat(),
        "documents": [
            {
                "path": path,
                "fields": base64.b64encode(_wire(s.to_dict())).decode("ascii"),
            }
            for path, s in sorted(records.items())
        ],
    }
    destination = Path(filename)
    # O_EXCL refuses both existing files and symlinks. Never replace a backup.
    try:
        descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        raise RecoveryError("Backup file already exists; choose a new file.") from None
    try:
        with os.fdopen(descriptor, "w") as stream:
            os.fchmod(stream.fileno(), 0o600)
            json.dump(archive, stream, separators=(",", ":"))
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        destination.unlink(missing_ok=True)
        raise
    return len(records)


def _allowed_path(path, season_id):
    if not isinstance(path, str):
        return False
    parts = path.split("/")
    if any(not part for part in parts) or len(parts) % 2:
        return False
    if len(parts) == 2:
        return parts[0] == "officers" or path in {
            f"seasons/{season_id}",
            f"publicSeasons/{season_id}",
        }
    return parts[:2] == ["seasons", season_id] and (
        (len(parts) == 4 and parts[2] in (*INPUT_COLLECTIONS, "runs"))
        or (len(parts) >= 4 and parts[2] == "results")
    )


def _read_archive(client, filename, target_project):
    archive = json.loads(Path(filename).read_text())
    if not isinstance(archive, dict) or archive.get("format") != FORMAT:
        raise RecoveryError("Unsupported Firestore backup format.")
    season_id = archive.get("seasonId")
    if not isinstance(season_id, str) or not ID.fullmatch(season_id):
        raise RecoveryError("Invalid backup season.")
    if not target_project or client.project != target_project:
        raise RecoveryError(
            "Explicit target project must match the target configuration."
        )
    if not archive.get("sourceProject") or target_project == archive["sourceProject"]:
        raise RecoveryError(
            "Restore requires a different project from the backup source."
        )
    if not isinstance(archive.get("documents"), list):
        raise RecoveryError("Invalid backup documents.")
    prepared = {}
    for record in archive["documents"]:
        if not isinstance(record, dict) or not _allowed_path(
            record.get("path"), season_id
        ):
            raise RecoveryError(
                "Backup contains a document outside the supported web scope."
            )
        path = record["path"]
        if path in prepared:
            raise RecoveryError("Backup contains duplicate document paths.")
        fields = Document.deserialize(
            base64.b64decode(record["fields"], validate=True)
        ).fields
        prepared[path] = decode_dict(fields, client)
    # Reject queued work and incomplete pointer data before creating any target doc.
    for path, data in prepared.items():
        if path.startswith(f"seasons/{season_id}/runs/") and data.get("status") not in {
            "succeeded",
            "failed",
        }:
            raise RecoveryError("Backup must not contain queued or running work.")
    pointer = prepared.get(f"publicSeasons/{season_id}")
    if pointer is not None:
        run_id = pointer.get("latestRunId")
        if not (
            prepared.get(f"seasons/{season_id}/results/{run_id}", {}).get("complete")
            is True
            and prepared.get(f"seasons/{season_id}/runs/{run_id}", {}).get("status")
            == "succeeded"
        ):
            raise RecoveryError(
                "Backup pointer does not match a complete successful result."
            )
    return prepared


def restore_firestore(client, filename: str | Path, target_project: str) -> int:
    """Create and verify an archive in an explicitly named empty, different project.

    Create-only batches never overwrite an existing document. A failed partial
    restore is left for inspection; use a fresh empty target for another attempt.
    No worker is started and no request is submitted.
    """
    prepared = _read_archive(client, filename, target_project)
    if next(client.collections(timeout=30), None) is not None:
        raise RecoveryError(
            "Restore target must be entirely empty; no documents were written."
        )
    batches = publication_batches(
        [(client.document(path), data) for path, data in prepared.items()]
    )
    for writes in batches:
        batch = client.batch()
        for ref, data in writes:
            batch.create(ref, data)
        batch.commit(timeout=30)
    actual = {}
    for collection in client.collections(timeout=30):
        for snapshot in _walk(collection):
            actual[snapshot.reference.path] = _wire(snapshot.to_dict())
    expected = {path: _wire(data) for path, data in prepared.items()}
    if actual != expected:
        raise RecoveryError("Restored document values/counts did not match the backup.")
    return len(prepared)
