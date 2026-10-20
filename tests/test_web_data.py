"""DATA-1 import atomicity, identities, domain validation and storage budgets."""

import json
import os
import uuid
from copy import deepcopy
from datetime import datetime, timezone

import pytest

from pebble.web_data import import_domain
from pebble.web_identity import mapped_bundle
from pebble.web_import import sheet_availability, sheet_day, sheet_time
from pebble.web_models import CalculationInput, validate_snapshot, wall_ms
from pebble.web_publication import document_bytes, publication_batches
from pebble.web_worker import firestore_client


def meta(**values):
    return dict(
        version=1,
        updatedBy="test-admin",
        updatedAt=datetime.now(timezone.utc),
        **values,
    )


def player(name="Example"):
    return dict(
        name=name,
        className="Druid",
        role="Healer",
        active=True,
        joinDate="",
        leaveDate="",
    )


@pytest.fixture
def db():
    if not os.getenv("FIRESTORE_EMULATOR_HOST"):
        pytest.skip("A dedicated emulator is required")
    client = firestore_client("demo-pebble-data1")
    season = "data-" + uuid.uuid4().hex
    yield client, season
    client.close()


def test_import_validates_entire_effective_season_and_commits_atomically(db):
    c, s = db
    with pytest.raises(ValueError):
        import_domain(
            c,
            s,
            {"players": {"p1": player()}, "reports": {"invalid": {"enabled": True}}},
        )
    assert not list(c.collection("seasons").document(s).collection("players").stream())
    with pytest.raises(ValueError, match="unknown player"):
        import_domain(
            c,
            s,
            {
                "players": {"p1": player()},
                "characters": {
                    "c": dict(
                        name="Alt", realm="Realm", region="us", playerId="missing"
                    )
                },
            },
        )
    assert not list(c.collection("seasons").document(s).collection("players").stream())
    assert import_domain(c, s, {"players": {"p1": player()}}) == {"players": 1}
    assert import_domain(
        c,
        s,
        {
            "characters": {
                "c": dict(name="Alt", realm="Realm", region="us", playerId="p1")
            }
        },
    ) == {"characters": 1}
    # An existing input wins; migration is not a synchronization command.
    assert import_domain(c, s, {"players": {"p1": player("Changed")}}) == {"players": 0}
    assert c.document(f"seasons/{s}/players/p1").get().to_dict()["name"] == "Example"


def test_import_rejects_metadata_unknown_collections_and_write_limit(db):
    c, s = db
    for values in (
        {"officers": {}},
        {"players": {"p": meta(**player())}},
        {"players": []},
    ):
        with pytest.raises(ValueError):
            import_domain(c, s, values)
    reports = {f"{n:016d}": {"enabled": True} for n in range(401)}
    with pytest.raises(ValueError, match="400"):
        import_domain(c, s, {"reports": reports})
    assert not list(c.collection("seasons").document(s).collection("reports").stream())
    with pytest.raises(ValueError):
        import_domain(c, s, {"players": {"p": player()}}, {"breakWindowStart": "25:00"})
    assert not list(c.collection("seasons").document(s).collection("players").stream())


def test_switching_project_or_season_does_not_migrate_data(db):
    c, season = db
    import_domain(c, season, {"players": {"p1": player()}})
    other = firestore_client("demo-pebble-data1-other")
    try:
        assert not list(
            other.collection("seasons").document(season).collection("players").stream()
        )
        assert not list(
            c.collection("seasons")
            .document(season + "-other")
            .collection("players")
            .stream()
        )
        # Explicit validated import is necessary in each destination.
        source = (
            c.collection("seasons")
            .document(season)
            .collection("players")
            .document("p1")
            .get()
            .to_dict()
        )
        domain = {
            k: v
            for k, v in source.items()
            if k not in ("version", "updatedBy", "updatedAt")
        }
        import_domain(other, season, {"players": {"p1": domain}})
        assert other.document(f"seasons/{season}/players/p1").get().exists
        assert c.document(f"seasons/{season}/players/p1").get().to_dict() == source
    finally:
        other.close()


def test_region_realm_and_case_scoped_aliases_do_not_modify_cached_bundle():
    raw = {
        "players": {"a": meta(**player("Alpha")), "b": meta(**player("Bravo"))},
        "characters": {
            "us": meta(name="Alt", realm="Test Realm", region="us", playerId="a"),
            "eu": meta(name="ALT", realm="TEST REALM", region="eu", playerId="b"),
        },
    }
    inputs = validate_snapshot(raw)
    bundle = {
        "region": {"compactName": "EU"},
        "masterData": {
            "actors": [{"name": "alt", "server": "test realm", "type": "Player"}]
        },
    }
    original = deepcopy(bundle)
    assert mapped_bundle(bundle, inputs)["masterData"]["actors"][0]["name"] == "Bravo"
    assert bundle == original
    bundle["region"]["compactName"] = "US"
    assert mapped_bundle(bundle, inputs)["masterData"]["actors"][0]["name"] == "Alpha"
    bundle["masterData"]["actors"][0]["server"] = "Other Realm"
    assert mapped_bundle(bundle, inputs)["masterData"]["actors"][0]["name"] not in (
        "Alpha",
        "Bravo",
    )
    del bundle["region"]
    with pytest.raises(ValueError, match="region"):
        mapped_bundle(bundle, inputs)
    raw["characters"]["duplicate"] = meta(
        name="alt", realm="test realm", region="us", playerId="b"
    )
    with pytest.raises(ValueError, match="multiple"):
        validate_snapshot(raw)


@pytest.mark.parametrize(
    "value,expected",
    [
        ("Y", {"mode": "available"}),
        ("0", {"mode": "unavailable"}),
        ("-15.0", {"mode": "unavailableFor", "minutes": 15}),
        (45.0, {"mode": "partial", "minutes": 45}),
        ("-", {"mode": "automatic"}),
    ],
)
def test_legacy_numeric_availability(value, expected):
    assert sheet_availability(value) == expected


@pytest.mark.parametrize("value", ["maybe", "NaN", "1.5"])
def test_bad_legacy_availability_fails_instead_of_becoming_automatic(value):
    with pytest.raises(ValueError):
        sheet_availability(value)


def test_legacy_dates_and_numeric_wall_times():
    assert sheet_day("10/15/2026") == "2026-10-15"
    assert sheet_day(46310) == "2026-10-15"
    assert sheet_time(0.75, wall_ms("2026-10-15T00:00")) == wall_ms("2026-10-15T18:00")
    with pytest.raises(ValueError):
        sheet_day("not a date")
    with pytest.raises(ValueError):
        sheet_time(0.123456, wall_ms("2026-10-15T00:00"))
    with pytest.raises(ValueError, match="whole-minute"):
        sheet_time("19:00:30", wall_ms("2026-10-15T00:00"))
    with pytest.raises(ValueError, match="does not exist"):
        sheet_time("02:30", wall_ms("2026-03-08T00:00"))
    with pytest.raises(ValueError, match="ambiguous"):
        sheet_time("01:30", wall_ms("2026-11-01T00:00"))


@pytest.mark.parametrize(
    "night,timing",
    [
        ("2026-02-30", dict(start="", breakStart="", breakEnd="", end="")),
        (
            "2026-03-08",
            dict(start="2026-03-08T02:30", breakStart="", breakEnd="", end=""),
        ),
        (
            "2026-11-01",
            dict(start="2026-11-01T01:30", breakStart="", breakEnd="", end=""),
        ),
        (
            "2026-10-15",
            dict(
                start="2026-10-16T19:00",
                breakStart="",
                breakEnd="",
                end="2026-10-17T01:00",
            ),
        ),
    ],
)
def test_calendar_dst_and_night_identity_rejected_by_worker(night, timing):
    with pytest.raises(ValueError):
        validate_snapshot({"nightOverrides": {night: meta(**timing)}})


def test_overnight_timing_and_settings_order():
    validate_snapshot(
        {
            "nightOverrides": {
                "2026-10-15": meta(
                    start="2026-10-15T23:00",
                    breakStart="",
                    breakEnd="",
                    end="2026-10-16T01:00",
                )
            }
        }
    )
    for values in (
        {"breakWindowStart": ""},
        {"breakWindowStart": "22:00"},
        {"postExtensionMinutes": float("nan")},
    ):
        with pytest.raises(ValueError):
            CalculationInput(**meta(**values))


def test_publication_size_and_batch_budgets():
    # 200 players per night, long Unicode names, 52 weeks: larger than this raid.
    p = dict(
        id="p" * 100,
        name="界" * 32,
        className="Death Knight",
        role="Damage",
        played=720,
        prePlayed=360,
        postPlayed=360,
        bench=720,
        preBench=360,
        postBench=360,
        nights=52,
        overridden=True,
    )
    night = {"players": [dict(p, id=f"p{i}") for i in range(200)]}
    assert document_bytes(night) < 100_000
    batches = publication_batches([(i, night) for i in range(520)])
    assert sum(map(len, batches)) == 520
    assert all(
        len(b) <= 400 and sum(document_bytes(d) for _, d in b) <= 8_000_000
        for b in batches
    )
    with pytest.raises(ValueError, match="size budget"):
        publication_batches([(1, night), (2, {"players": [p] * 10000})])


def test_sheets_import_rejects_missing_references_before_import(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import Mock

    from pebble.web_import import import_sheets
    from pebble.web_models import WorkerConfig

    keys = ("reports", "team_roster", "roster_map", "availability_overrides")
    time = WorkerConfig(
        firestore={"project_id": "demo-pebble", "season_id": "test"},
        mongo={"uri": "mongodb://127.0.0.1"},
        wcl={"client_id": "fake", "client_secret": "fake"},
    ).time
    settings = SimpleNamespace(
        sheets=SimpleNamespace(
            tabs=SimpleNamespace(**{k: k for k in keys}),
            starts=SimpleNamespace(**{k: "A1" for k in keys}),
        ),
        time=time,
    )
    rows = {
        "team_roster": [["Main", "Active?"], ["Example", "TRUE"]],
        "reports": [],
        "roster_map": [["Alt", "Main"], ["Alt-Realm", "Missing"]],
        "availability_overrides": [],
    }
    monkeypatch.setattr("pebble.web_import.SheetsClient", Mock())
    monkeypatch.setattr("pebble.web_import.load_settings", lambda *a, **k: settings)
    monkeypatch.setattr("pebble.web_import._sheet_values_batch", lambda *a, **k: rows)
    save = Mock()
    monkeypatch.setattr("pebble.web_import.import_domain", save)
    with pytest.raises(ValueError, match="missing main reference"):
        import_sheets(Mock(), "test", "config", "credentials")
    save.assert_not_called()
    rows["roster_map"] = []
    rows["availability_overrides"] = [
        ["Night", "Main", "Avail Pre?"],
        ["2026-10-15", "Missing", "45"],
    ]
    with pytest.raises(ValueError, match="missing main reference"):
        import_sheets(Mock(), "test", "config", "credentials")
    save.assert_not_called()
    rows["availability_overrides"] = []
    rows["reports"] = [
        ["Report URL", "Report Start (PT)", "Break Override Start (PT)"],
        ["https://www.warcraftlogs.com/reports/aB3dE6gH9jK2mN5p", "", "8:00 PM"],
    ]
    with pytest.raises(ValueError, match="valid report date"):
        import_sheets(Mock(), "test", "config", "credentials")
    save.assert_not_called()


def test_parity_reports_numeric_changes_and_explicit_zero_rows():
    from pebble.web_parity import compare_sheet_results

    rows = {
        "bench_night_totals": [
            [
                "Night ID",
                "Main",
                "Played Total (min)",
                "Bench Pre (min)",
                "Bench Post (min)",
                "Bench Total (min)",
            ],
            ["2026-10-15", "Example", 30, 10, 5, 15],
        ],
        "bench_week_totals": [
            [
                "Game Week",
                "Main",
                "Played Week (min)",
                "Bench Pre (min)",
                "Bench Post (min)",
                "Bench Week (min)",
            ],
            ["2026-10-13", "Example", 30, 10, 5, 15],
        ],
        "bench_rankings": [
            ["Main", "Time Played (min)", "Bench Season-to-date (min)"],
            ["Example", 30, 15],
        ],
    }
    p = {"name": "Example", "played": 30, "preBench": 10, "postBench": 5, "bench": 15}
    public = {
        "players": [p],
        "nights": [
            {
                "night": "2026-10-15",
                "players": [
                    p,
                    dict(p, name="Zero", played=0, preBench=0, postBench=0, bench=0),
                ],
            }
        ],
        "weeks": [
            {
                "players": [
                    {
                        "main": "Example",
                        "game_week": "2026-10-13",
                        "played_min": 30,
                        "bench_pre_min": 10,
                        "bench_post_min": 5,
                        "bench_min": 15,
                    }
                ]
            }
        ],
    }
    result = compare_sheet_results(rows, public)
    assert all(v["differentRows"] == 0 for v in result.values())
    assert result["nightly"]["equivalentMissingZeroRows"] == 1
    p["played"] = 31
    result = compare_sheet_results(rows, public)
    assert result["nightly"]["differentRows"] == 1
    assert result["season"]["differentRows"] == 1
    assert "Example" not in json.dumps(result)


def test_oversized_publication_never_creates_staging_or_changes_pointer(db):
    import mongomock

    from pebble.web_models import WorkerConfig
    from pebble.web_worker import InputError, Worker

    c, season = db
    cfg = WorkerConfig(
        firestore={"project_id": "demo-pebble-data1", "season_id": season},
        mongo={"uri": "mongodb://127.0.0.1"},
        wcl={"client_id": "fake", "client_secret": "fake"},
    )
    worker = Worker(cfg, c, mongomock.MongoClient())
    pointer = c.document("publicSeasons/" + season)
    pointer.set({"latestRunId": "previous"})
    worker.season.collection("results").document("previous").set({"complete": True})
    ref = worker.season.collection("runs").document("oversized")
    ref.set({"status": "running"})
    output = {
        "players": [],
        "nights": [
            {
                "night": "2026-10-15",
                "players": [{"name": "界" * 32, "id": "p" * 100, "played": 720}]
                * 10000,
            }
        ],
        "weeks": [],
        "reportCount": 1,
        "warnings": [],
    }
    with pytest.raises(InputError, match="size budget"):
        worker.publish(ref, output)
    assert not worker.season.collection("results").document("oversized").get().exists
    assert pointer.get().to_dict()["latestRunId"] == "previous"
    assert (
        worker.season.collection("results")
        .document("previous")
        .get()
        .to_dict()["complete"]
    )


def test_long_player_ids_keep_valid_availability_identity(db):
    from pebble.web_models import Availability

    c, season = db
    pid = "p" * 100
    values = {
        "players": {pid: player()},
        "availabilityOverrides": {
            "2026-10-15_"
            + pid: {
                "nightId": "2026-10-15",
                "playerId": pid,
                "pre": {"mode": "automatic"},
                "post": {"mode": "available"},
            }
        },
    }
    assert import_domain(c, season, values) == {
        "players": 1,
        "availabilityOverrides": 1,
    }
    with pytest.raises(ValueError):
        Availability(mode="automatic", minutes=None)
