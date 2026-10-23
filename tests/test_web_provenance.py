"""Recorded public availability and officer-only diagnostics do not recalculate time."""

import json
from types import SimpleNamespace

import mongomock
import pytest
import test_web_worker as fixtures
from google.cloud import firestore

from pebble.web_provenance import availability_explanation, night_diagnostics
from pebble.web_worker import Worker, calculate_snapshot

config = fixtures.config
wcl = fixtures.wcl
emulator = fixtures.emulator


def qa(**changes):
    return dict(
        reports=["PRIVATE-REPORT"],
        mythic_fights=8,
        not_on_roster_mains=["PRIVATE-NAME", "PRIVATE-ALT"],
        break_duration_min=15,
        mythic_override_start_ms=None,
        mythic_override_end_ms=None,
        break_override_start_ms=None,
        break_start_ms=100,
        **changes,
    )


def test_public_explanation_copies_flags_modes_and_whole_row_source_only():
    row = dict(
        avail_pre=True,
        avail_post=False,
        status_source="override",
        notes="PRIVATE",
        main="PRIVATE",
    )
    override = SimpleNamespace(
        pre=SimpleNamespace(mode="partial"),
        post=SimpleNamespace(mode="automatic"),
        notes="PRIVATE",
    )
    assert availability_explanation(row, override) == {
        "pre": {"available": True, "mode": "partial"},
        "post": {"available": False, "mode": "automatic"},
        "source": "override",
    }
    automatic = availability_explanation({**row, "status_source": "last_fight"})
    assert automatic["pre"]["mode"] == automatic["post"]["mode"] == "automatic"
    assert automatic["source"] == "last_fight"
    assert "PRIVATE" not in json.dumps(automatic)


def test_private_diagnostics_allowlist_counts_without_names_notes_or_raw_candidates():
    source = qa()
    source.update(notes="PRIVATE", gap_candidates=[{"error": "PRIVATE"}])
    value = night_diagnostics(source)
    assert value == {
        "reportCount": 1,
        "mythicFights": 8,
        "unmappedCharacters": 2,
        "breakMinutes": 15,
        "timing": {"start": "detected", "end": "detected", "break": "detected"},
    }
    assert "PRIVATE" not in json.dumps(value)
    source.update(
        mythic_override_start_ms=1, mythic_override_end_ms=2, break_override_start_ms=3
    )
    assert night_diagnostics(source)["timing"]["start"] == "configured"
    assert night_diagnostics(source, SimpleNamespace(start="2026-10-15T19:00"))[
        "timing"
    ] == {"start": "override", "end": "override", "break": "override"}
    source.update(
        break_override_start_ms=None, break_start_ms=None, break_duration_min=None
    )
    assert night_diagnostics(source)["timing"]["break"] == "none"
    assert night_diagnostics(source)["breakMinutes"] is None


def test_adapter_explains_recorded_nightly_flags_and_keeps_private_diagnostics_separate(
    config, wcl
):
    raw = fixtures.snapshot()
    raw["nightOverrides"]["2026-10-15"] = fixtures.meta(
        start="2026-10-15T18:55",
        end="2026-10-15T20:15",
        breakStart="2026-10-15T19:10",
        breakEnd="2026-10-15T19:20",
        notes="PRIVATE TIMING",
    )
    db = mongomock.MongoClient().db
    output = calculate_snapshot(config, db, raw)
    for night in output["nights"]:
        stored = {
            row["main"]: row
            for row in db.bench_night_totals.find({"night_id": night["night"]})
        }
        for row in night["players"]:
            if row["name"] not in stored:
                assert "availability" not in row
                continue
            computed = stored[row["name"]]
            assert row["availability"]["pre"]["available"] == computed["avail_pre"]
            assert row["availability"]["post"]["available"] == computed["avail_post"]
            assert row["availability"]["source"] == computed["status_source"]
    adjusted = next(n for n in output["nights"] if n["night"] == "2026-10-15")
    row = next(p for p in adjusted["players"] if p["id"] == "solace")
    assert row["availability"]["pre"]["mode"] == "partial"
    assert row["availability"]["post"]["mode"] == "automatic"
    assert output["nightDiagnostics"]["2026-10-15"]["timing"] == {
        "start": "override",
        "end": "override",
        "break": "override",
    }
    assert all("availability" not in p for p in output["players"])
    assert "PRIVATE" not in json.dumps(output, default=str)


def test_configured_default_timing_is_recorded_as_configured(config, wcl):
    raw = fixtures.snapshot()
    raw["settings"] = {"calculation": fixtures.meta(defaultMythicStart="18:55")}
    output = calculate_snapshot(config, mongomock.MongoClient().db, raw)
    assert all(
        n["timing"]["start"] == "configured"
        for n in output["nightDiagnostics"].values()
    )


def queue(season, name):
    ref = season.collection("runs").document(name)
    ref.set(
        {
            "status": "queued",
            "requestedBy": "test-officer",
            "requestedAt": firestore.SERVER_TIMESTAMP,
        }
    )
    return ref


def test_publication_keeps_diagnostics_in_private_run_and_public_explanations_in_nights(
    config, emulator, wcl
):
    client, season = emulator
    worker = Worker(config, client, mongomock.MongoClient())
    ref = queue(season, "provenance-complete")
    assert worker.process_next()
    data = ref.get().to_dict()
    assert data["status"] == "succeeded"
    assert data["nightDiagnostics"]["2026-10-15"]["reportCount"] == 1
    root = season.collection("results").document(ref.id)
    public_docs = [root.get().to_dict()] + [
        d.to_dict()
        for name in ("players", "nights", "weeks")
        for d in root.collection(name).stream()
    ]
    assert all("nightDiagnostics" not in doc for doc in public_docs)
    nights = list(root.collection("nights").stream())
    assert any(
        "availability" in player
        for night in nights
        for player in night.to_dict()["players"]
    )


def test_oversized_private_diagnostics_fail_before_metadata_upload_and_preserve_completed_pointer(
    config, emulator, wcl, monkeypatch
):
    client, season = emulator
    worker = Worker(config, client, mongomock.MongoClient())
    previous = queue(season, "provenance-previous")
    assert worker.process_next()
    prior_diagnostics = previous.get().to_dict()["nightDiagnostics"]
    ref = queue(season, "provenance-oversized")
    monkeypatch.setattr(
        "pebble.web_worker.calculate_snapshot",
        lambda *args, **kwargs: {
            "nightDiagnostics": {"huge": "x" * 1_000_000},
            "reportSummaries": {},
            "warnings": [],
            "players": [],
            "nights": [],
            "weeks": [],
            "reportCount": 0,
        },
    )
    assert worker.process_next()
    failed = ref.get().to_dict()
    assert failed["status"] == "failed"
    assert "size budget" in failed["error"]
    assert "nightDiagnostics" not in failed
    assert not season.collection("results").document(ref.id).get().exists
    assert (
        client.document("publicSeasons/worker-tests").get().to_dict()["latestRunId"]
        == previous.id
    )
    assert previous.get().to_dict()["nightDiagnostics"] == prior_diagnostics


def test_missing_identity_actions_keep_exact_realms_and_ignore_only_the_chosen_character(
    config, wcl, monkeypatch
):
    from pebble.web_worker import WCLClient

    fetch = WCLClient.fetch_report_bundle

    def bundle(client, code):
        value = fetch(client, code)
        value["region"] = {"compactName": "EU"}
        value["masterData"]["actors"].append(
            {
                "id": 99,
                "name": "Extra",
                "server": "Azjol-Nerub",
                "type": "Player",
                "subType": "Mage",
            }
        )
        for fight in value["fights"]:
            fight["friendlyPlayers"] = [*fight.get("friendlyPlayers", []), 99]
        return value

    monkeypatch.setattr(WCLClient, "fetch_report_bundle", bundle)
    raw = fixtures.snapshot()
    raw["reports"][fixtures.CODES[1]]["enabled"] = False
    db = mongomock.MongoClient().working
    first = calculate_snapshot(config, db, raw)
    assert first["nightDiagnostics"]["2026-10-15"]["missingPlayers"] == [
        {"name": "Extra", "realm": "Azjol-Nerub", "region": "eu", "className": "Mage"}
    ]
    raw["characters"] = {
        "ignore-extra": fixtures.meta(
            name="Extra", realm="Azjol-Nerub", region="eu", playerId="", ignored=True
        )
    }
    ignored = calculate_snapshot(config, db, raw)
    assert ignored["warnings"] == []
    assert ignored["nightDiagnostics"]["2026-10-15"]["missingPlayers"] == []
    assert ignored["nights"] == first["nights"]
    del raw["characters"]
    raw["players"]["extra"] = fixtures.meta(
        name="Extra",
        className="Mage",
        role="Damage",
        active=True,
        joinDate="2026-10-15",
        leaveDate="",
    )
    added = calculate_snapshot(config, db, raw)
    assert added["warnings"] == []
    assert any(
        row["name"] == "Extra" and row["played"] > 0
        for row in added["nights"][0]["players"]
    )
    del raw["players"]["extra"]
    raw["characters"] = {
        "alt-extra": fixtures.meta(
            name="Extra", realm="Azjol-Nerub", region="eu", playerId="maggie"
        )
    }
    mapped = calculate_snapshot(config, db, raw)
    assert mapped["warnings"] == []
    assert mapped["nightDiagnostics"]["2026-10-15"]["missingPlayers"] == []
