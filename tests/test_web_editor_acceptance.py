"""Editor/worker acceptance against a dedicated demo Firestore project."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from uuid import uuid4

import mongomock
import pytest
from google.cloud import firestore

from pebble.web_models import WorkerConfig, wall_ms
from pebble.web_worker import Worker, calculate_snapshot, firestore_client


def test_editor_inputs_are_captured_at_run_and_publish_one_consistent_result(
    monkeypatch,
):
    if not os.getenv("FIRESTORE_EMULATOR_HOST"):
        pytest.skip("Requires the isolated Firestore emulator")
    project = "demo-pebble-editor-acceptance"
    season_id = f"editors-{uuid4().hex}"
    client = firestore_client(project)
    season = client.document(f"seasons/{season_id}")
    config = WorkerConfig(
        firestore={"project_id": project, "season_id": season_id},
        mongo={"uri": "mongodb://localhost:27017", "db": "editor_acceptance"},
        wcl={"client_id": "fixture", "client_secret": "fixture"},
    )
    worker = Worker(config, client, mongomock.MongoClient())
    code = "AAAAAAAAAAAAAAAA"

    def meta(version=1, **value):
        return {
            **value,
            "version": version,
            "updatedBy": "test-officer",
            "updatedAt": datetime.now(timezone.utc),
        }

    original = mongomock.collection.BulkOperationBuilder.add_update

    def compatible_update(self, *args, sort=None, **kwargs):
        return original(self, *args, **kwargs)

    monkeypatch.setattr(
        mongomock.collection.BulkOperationBuilder, "add_update", compatible_update
    )
    start = wall_ms("2026-10-15T19:00")
    fetched = []

    def fetch(_client, requested_code):
        fetched.append(requested_code)
        return {
            "title": "Editor acceptance raid",
            "startTime": start,
            "endTime": start + 4_200_000,
            "region": {"compactName": "US"},
            "masterData": {
                "actors": [
                    {
                        "id": 1,
                        "name": "Loggedalt",
                        "server": "EXAMPLE REALM",
                        "type": "Player",
                        "subType": "Druid",
                    }
                ]
            },
            "fights": [
                {
                    "id": 1,
                    "encounterID": 123,
                    "difficulty": 5,
                    "startTime": 0,
                    "endTime": 600_000,
                    "friendlyPlayers": [1],
                },
                {
                    "id": 2,
                    "encounterID": 123,
                    "difficulty": 5,
                    "startTime": 3_600_000,
                    "endTime": 4_200_000,
                    "friendlyPlayers": [1],
                },
            ],
        }

    monkeypatch.setattr("pebble.web_worker.WCLClient.fetch_report_bundle", fetch)
    player = dict(
        name="Example",
        className="Druid",
        role="Healer",
        active=True,
        joinDate="",
        leaveDate="",
        notes="Private editor notes",
    )
    settings = dict(
        tz="America/Los_Angeles",
        breakWindowStart="20:50",
        breakWindowEnd="21:30",
        minBreakMinutes=10,
        maxBreakMinutes=30,
        defaultMythicStart="",
        postExtensionMinutes=5,
    )
    try:
        client.document("officers/test-officer").set({"enabled": True})
        season.collection("reports").document(code).set(meta(enabled=True))
        main = season.collection("players").document("stable-main")
        main.set(meta(**player))
        calculation = season.collection("settings").document("calculation")
        calculation.set(meta(**settings))
        run = season.collection("runs").document("accepted")
        run.set(
            {
                "status": "queued",
                "requestedBy": "test-officer",
                "requestedAt": firestore.SERVER_TIMESTAMP,
            }
        )
        # Saves after submission, but before claim/capture, must be consumed.
        player["name"] = "Renamed"
        main.set(meta(2, **player))
        season.collection("characters").document("stable-alias").set(
            meta(
                name="loggedalt",
                realm="Example Realm",
                region="us",
                playerId="stable-main",
            )
        )
        settings["postExtensionMinutes"] = 0
        calculation.set(meta(2, **settings))

        def calculate_captured(config, db, raw, *, stage):
            assert raw["players"]["stable-main"]["name"] == "Renamed"
            assert raw["characters"]["stable-alias"]["playerId"] == "stable-main"
            assert raw["settings"]["calculation"]["postExtensionMinutes"] == 0
            # Newer live inputs must not leak into this immutable captured run.
            main.set(meta(3, **{**player, "name": "Later"}))
            calculation.set(meta(3, **{**settings, "postExtensionMinutes": 30}))
            return calculate_snapshot(config, db, raw, stage=stage)

        monkeypatch.setattr("pebble.web_worker.calculate_snapshot", calculate_captured)
        assert worker.process_next()
        completed = run.get().to_dict()
        assert completed["status"] == "succeeded", completed
        assert completed["inputVersions"] == {
            f"reports/{code}": 1,
            "players/stable-main": 2,
            "characters/stable-alias": 1,
            "settings/calculation": 2,
        }
        saved = worker.db.input_snapshots.find_one({"run_id": "accepted"})["inputs"]
        assert saved["players"]["stable-main"]["name"] == "Renamed"
        assert saved["settings"]["calculation"]["postExtensionMinutes"] == 0
        result = season.collection("results").document("accepted")
        assert result.get().to_dict()["complete"] is True
        assert (
            client.document(f"publicSeasons/{season_id}").get().to_dict()["latestRunId"]
            == "accepted"
        )
        row = result.collection("players").document("stable-main").get().to_dict()
        assert row["name"] == "Renamed"
        # Existing participation blocks bridge these two pulls with no missed boss.
        assert row["played"] == 70  # The logged alt resolves to the renamed main.
        assert "notes" not in row
        night = result.collection("nights").document("2026-10-15").get().to_dict()
        assert night["timing"]["end"] == "2026-10-15T20:10"  # Captured extension=0.
        assert fetched == [code]
        assert main.get().to_dict()["version"] == 3
        assert calculation.get().to_dict()["postExtensionMinutes"] == 30
        assert not worker.process_next()
    finally:
        # Dedicated demo project + unique season: no shared fixture clearing.
        client.close()
