"""Protected logical backup/restore acceptance using isolated demo projects."""

from __future__ import annotations

import base64
import json
import os
import stat
from uuid import uuid4

import pytest
from click.testing import CliRunner
from google.api_core.datetime_helpers import DatetimeWithNanoseconds
from google.api_core.exceptions import AlreadyExists
from google.cloud.firestore_v1._helpers import decode_dict
from google.cloud.firestore_v1.types import Document

from pebble import web_backup, web_cli
from pebble.web_backup import backup_firestore, restore_firestore
from pebble.web_worker import firestore_client

SEASON = "recovery"
STAMP = DatetimeWithNanoseconds.from_rfc3339("2026-10-19T12:34:56.123456789Z")


@pytest.fixture
def projects():
    if not os.getenv("FIRESTORE_EMULATOR_HOST"):
        pytest.skip("Requires the isolated Firestore emulator")
    suffix = uuid4().hex[:8]
    source = firestore_client(f"demo-recovery-s-{suffix}")
    target = firestore_client(f"demo-recovery-t-{suffix}")
    source.document("officers/test-officer").set({"enabled": True})
    source.document(f"seasons/{SEASON}").set({"displayName": "Example season"})
    source.document(f"seasons/{SEASON}/players/main").set(
        {
            "name": "Example",
            "className": "Druid",
            "role": "Healer",
            "active": True,
            "joinDate": "",
            "leaveDate": "",
            "notes": "Private notes",
            "version": 2,
            "updatedAt": STAMP,
            "updatedBy": "test-officer",
        }
    )
    source.document(f"seasons/{SEASON}/runs/published").set(
        {
            "status": "succeeded",
            "completedAt": STAMP,
            "requestedAt": STAMP,
            "requestedBy": "test-officer",
            "inputVersions": {"players/main": 2},
            "warnings": [],
        }
    )
    source.document(f"seasons/{SEASON}/runs/failed").set(
        {"status": "failed", "error": "Example failure", "completedAt": STAMP}
    )
    source.document(f"seasons/{SEASON}/results/published").set(
        {"complete": True, "completedAt": STAMP, "reportCount": 1}
    )
    source.document(f"seasons/{SEASON}/results/published/players/main").set(
        {
            "id": "main",
            "name": "Example",
            "played": 60,
            "bench": 10,
            "attendance": {
                "played": 60,
                "bench": 10,
                "possible": 100,
                "rate": 0.7,
                "weeks": [{"week": "2026-10-13", "status": "PB"}],
            },
        }
    )
    source.document(f"seasons/{SEASON}/results/published/nights/2026-10-15").set(
        {"night": "2026-10-15", "players": [], "duration": 70}
    )
    source.document(f"seasons/{SEASON}/results/published/weeks/2026-10-13").set(
        {"players": [{"main": "Example", "played_min": 60, "bench_min": 10}]}
    )
    source.document(f"seasons/{SEASON}/results/interrupted").set({"complete": False})
    source.document(f"seasons/{SEASON}/results/interrupted/players/staged").set(
        {"played": 1}
    )
    source.document(f"publicSeasons/{SEASON}").set({"latestRunId": "published"})
    source.document("bot/status").set({"lastHeartbeatAt": STAMP})
    yield source, target
    source.close()
    target.close()


def test_sdk_wire_codec_keeps_nanoseconds_and_native_scalar_and_nested_types():
    data = {
        "stamp": STAMP,
        "integer": 9223372036854775807,
        "fraction": 1.25,
        "flag": True,
        "nothing": None,
        "nested": {"array": [1, False, "é"]},
        "bytes": b"example",
    }
    encoded = web_backup._wire(data)
    decoded = decode_dict(Document.deserialize(encoded).fields, None)
    assert decoded["stamp"].nanosecond == 123456789
    assert decoded["integer"] == 9223372036854775807
    assert isinstance(decoded["integer"], int)
    assert isinstance(decoded["fraction"], float)
    assert decoded["bytes"] == b"example"
    assert decoded["nested"] == {"array": [1, False, "é"]}
    assert web_backup._wire(decoded) == encoded


def test_protected_roundtrip_retains_inputs_history_publication_and_source(
    projects, tmp_path
):
    source, target = projects
    filename = tmp_path / "backup.json"
    expected_stamp = (
        source.document(f"seasons/{SEASON}/players/main").get().get("updatedAt")
    )
    assert backup_firestore(source, SEASON, filename) == 10
    assert stat.S_IMODE(filename.stat().st_mode) == 0o600
    archive = json.loads(filename.read_text())
    paths = {record["path"] for record in archive["documents"]}
    assert "bot/status" not in paths
    assert not any("interrupted" in path for path in paths)
    assert restore_firestore(target, filename, target.project) == 10
    player = target.document(f"seasons/{SEASON}/players/main").get().to_dict()
    assert player["name"] == "Example" and player["version"] == 2
    assert player["notes"] == "Private notes"
    assert player["updatedAt"].nanosecond == expected_stamp.nanosecond
    result = (
        target.document(f"seasons/{SEASON}/results/published/players/main")
        .get()
        .to_dict()
    )
    assert result["attendance"]["rate"] == 0.7
    assert result["attendance"]["weeks"] == [{"week": "2026-10-13", "status": "PB"}]
    assert (
        target.document(f"seasons/{SEASON}/runs/failed").get().get("status") == "failed"
    )
    assert (
        target.document(f"publicSeasons/{SEASON}").get().get("latestRunId")
        == "published"
    )
    assert target.document("officers/test-officer").get().get("enabled") is True
    assert not target.document("bot/status").get().exists
    assert source.document(f"seasons/{SEASON}/players/main").get().to_dict() == player
    with pytest.raises(ValueError, match="entirely empty"):
        restore_firestore(target, filename, target.project)


@pytest.mark.parametrize("status", ["queued", "running"])
def test_backup_refuses_active_work_without_creating_file(projects, tmp_path, status):
    source, _ = projects
    source.document(f"seasons/{SEASON}/runs/active").set({"status": status})
    filename = tmp_path / "backup.json"
    with pytest.raises(ValueError, match="terminal"):
        backup_firestore(source, SEASON, filename)
    assert not filename.exists()


def test_backup_refuses_changed_inputs_bad_pointer_and_file_overwrite(
    projects, tmp_path, monkeypatch
):
    source, _ = projects
    filename = tmp_path / "backup.json"
    original = web_backup._controls
    calls = 0

    def changing_controls(client, season):
        nonlocal calls
        calls += 1
        if calls == 2:
            source.document(f"seasons/{SEASON}/players/main").update(
                {"name": "Changed", "version": 3}
            )
        return original(client, season)

    monkeypatch.setattr(web_backup, "_controls", changing_controls)
    with pytest.raises(ValueError, match="changed"):
        backup_firestore(source, SEASON, filename)
    assert not filename.exists()
    monkeypatch.setattr(web_backup, "_controls", original)
    source.document(f"publicSeasons/{SEASON}").set({"latestRunId": "missing"})
    with pytest.raises(ValueError, match="pointer"):
        backup_firestore(source, SEASON, filename)
    source.document(f"publicSeasons/{SEASON}").set({"latestRunId": "published"})
    filename.write_text("existing protected backup")
    with pytest.raises(ValueError, match="already exists"):
        backup_firestore(source, SEASON, filename)
    assert filename.read_text() == "existing protected backup"


def test_restore_requires_named_different_empty_project_including_orphan_children(
    projects, tmp_path
):
    source, target = projects
    filename = tmp_path / "backup.json"
    backup_firestore(source, SEASON, filename)
    with pytest.raises(ValueError, match="match the target"):
        restore_firestore(target, filename, "demo-wrong-target")
    with pytest.raises(ValueError, match="different project"):
        restore_firestore(source, filename, source.project)
    target.document("unrelated/missing/children/existing").set({"preserve": True})
    with pytest.raises(ValueError, match="entirely empty"):
        restore_firestore(target, filename, target.project)
    assert (
        target.document("unrelated/missing/children/existing").get().get("preserve")
        is True
    )
    assert not target.document("officers/test-officer").get().exists


def test_restore_preflights_paths_and_active_work_before_writes(projects, tmp_path):
    source, target = projects
    filename = tmp_path / "backup.json"
    backup_firestore(source, SEASON, filename)
    archive = json.loads(filename.read_text())
    archive["documents"].append(
        {
            "path": "bot/status",
            "fields": base64.b64encode(
                web_backup._wire({"secret": "not imported"})
            ).decode(),
        }
    )
    filename.write_text(json.dumps(archive))
    with pytest.raises(ValueError, match="outside"):
        restore_firestore(target, filename, target.project)
    archive["documents"].pop()
    run = next(
        record
        for record in archive["documents"]
        if record["path"].endswith("runs/failed")
    )
    run["fields"] = base64.b64encode(web_backup._wire({"status": "queued"})).decode()
    filename.write_text(json.dumps(archive))
    with pytest.raises(ValueError, match="queued"):
        restore_firestore(target, filename, target.project)
    assert not list(target.collections())


def test_create_only_batch_never_overwrites_a_concurrent_target_document(
    projects, tmp_path, monkeypatch
):
    source, target = projects
    filename = tmp_path / "backup.json"
    backup_firestore(source, SEASON, filename)
    original = target.batch

    def racing_batch():
        race = original()
        race.set(
            target.document("officers/test-officer"),
            {"enabled": False, "preserve": True},
        )
        race.commit()
        return original()

    monkeypatch.setattr(target, "batch", racing_batch)
    with pytest.raises(AlreadyExists):
        restore_firestore(target, filename, target.project)
    assert target.document("officers/test-officer").get().to_dict() == {
        "enabled": False,
        "preserve": True,
    }
    assert not target.document(f"seasons/{SEASON}").get().exists


@pytest.mark.parametrize("cause", [OSError, ValueError])
def test_cli_failures_do_not_print_private_provider_messages(
    monkeypatch, tmp_path, cause
):
    def unavailable(_config):
        raise cause("private credential details")

    monkeypatch.setattr(web_cli, "resources", unavailable)
    runner = CliRunner()
    result = runner.invoke(
        web_cli.web, ["backup-firestore", "--file", str(tmp_path / "out.json")]
    )
    assert result.exit_code != 0
    assert cause.__name__ in result.output and "private credential" not in result.output
