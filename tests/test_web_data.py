"""DATA-1 import atomicity, identities, domain validation and storage budgets."""

import os
import uuid
from copy import deepcopy
from datetime import datetime, timezone

import pytest

from pebble.web_data import import_domain
from pebble.web_identity import mapped_bundle
from pebble.web_models import CalculationInput, validate_snapshot
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


@pytest.mark.parametrize(
    "families",
    [
        ("characters",),
        ("availabilityOverrides",),
        ("characters", "availabilityOverrides"),
    ],
)
def test_linked_import_advances_existing_main_once_and_noop_keeps_version(db, families):
    c, season = db
    import_domain(c, season, {"players": {"p1": player(), "p2": player("Other")}})
    ref = c.document(f"seasons/{season}/players/p1")
    original = ref.get().to_dict()
    records = {
        "characters": {
            "alt": {"name": "Alt", "realm": "", "region": "us", "playerId": "p1"}
        },
        "availabilityOverrides": {
            "2026-10-15_p1": {
                "nightId": "2026-10-15",
                "playerId": "p1",
                "pre": {"mode": "automatic"},
                "post": {"mode": "available"},
            }
        },
    }
    values = {family: records[family] for family in families}
    assert import_domain(c, season, values) == {family: 1 for family in families}
    saved = ref.get().to_dict()
    assert saved["version"] == 2
    assert saved["updatedBy"] == "local-admin"
    assert {k: v for k, v in saved.items() if k not in ("version", "updatedAt")} == {
        k: v for k, v in original.items() if k not in ("version", "updatedAt")
    }
    assert c.document(f"seasons/{season}/players/p2").get().to_dict()["version"] == 1
    assert import_domain(c, season, values) == {family: 0 for family in families}
    assert ref.get().to_dict() == saved


def test_linked_import_counts_main_version_writes_in_atomic_limit(db, monkeypatch):
    c, season = db
    import_domain(c, season, {"players": {"p1": player()}})
    monkeypatch.setattr("pebble.web_data.MAX_IMPORT_WRITES", 1)
    with pytest.raises(ValueError, match="400 writes"):
        import_domain(
            c,
            season,
            {
                "characters": {
                    "alt": {
                        "name": "Alt",
                        "realm": "",
                        "region": "us",
                        "playerId": "p1",
                    }
                }
            },
        )
    assert not c.document(f"seasons/{season}/characters/alt").get().exists
    assert c.document(f"seasons/{season}/players/p1").get().to_dict()["version"] == 1


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
        import_domain(
            c,
            s,
            {
                "players": {"p": player()},
                "settings": {"calculation": {"breakWindowStart": "25:00"}},
            },
        )
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
