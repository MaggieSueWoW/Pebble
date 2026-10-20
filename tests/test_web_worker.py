from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from unittest.mock import Mock

import mongomock
import pytest
from google.api_core.exceptions import ServiceUnavailable
from google.cloud import firestore

from pebble.attendance import build_attendance_projection
from pebble.web_models import Availability, WorkerConfig, validate_snapshot, wall_ms
from pebble.web_worker import (
    Worker,
    calculate_snapshot,
    capture_inputs,
    firestore_client,
)

CODES = ["aB3dE6gH9jK2mN5p", "bD4fG7jL2nQ5sV8x"]


def meta(**values):
    return {
        "version": 1,
        "updatedBy": "test-officer",
        "updatedAt": datetime.now(timezone.utc),
        **values,
    }


def snapshot():
    return {
        "reports": {
            code: meta(enabled=True, notes="PRIVATE REPORT NOTE") for code in CODES
        },
        "players": {
            "maggie": meta(
                name="Maggie",
                className="Druid",
                role="Healer",
                active=True,
                joinDate="",
                leaveDate="",
                notes="PRIVATE PLAYER NOTE",
            ),
            "solace": meta(
                name="Solace",
                className="Paladin",
                role="Healer",
                active=True,
                joinDate="",
                leaveDate="",
            ),
        },
        "characters": {},
        "nightOverrides": {},
        "settings": {},
        "availabilityOverrides": {
            "2026-10-15_solace": meta(
                nightId="2026-10-15",
                playerId="solace",
                pre={"mode": "partial", "minutes": 45},
                post={"mode": "automatic"},
                notes="PRIVATE OVERRIDE NOTE",
            )
        },
    }


@pytest.fixture
def config():
    return WorkerConfig(
        firestore={"project_id": "demo-pebble", "season_id": "worker-tests"},
        mongo={"uri": "mongodb://localhost:27017", "db": "test"},
        wcl={"client_id": "test", "client_secret": "test"},
    )


@pytest.fixture
def wcl(monkeypatch):
    original = mongomock.collection.BulkOperationBuilder.add_update

    def compatible_update(self, *args, sort=None, **kwargs):
        return original(self, *args, **kwargs)

    monkeypatch.setattr(
        mongomock.collection.BulkOperationBuilder, "add_update", compatible_update
    )

    def fetch(_self, code):
        day = "2026-10-15" if code == CODES[0] else "2026-10-14"
        start = wall_ms(f"{day}T19:00")
        return {
            "title": "Test raid",
            "startTime": start,
            "endTime": start + 4_200_000,
            "masterData": {
                "actors": [
                    {
                        "id": 1,
                        "name": "Maggie",
                        "server": "TestRealm",
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


def test_snapshot_projection_and_exclusion(config, wcl):
    db = mongomock.MongoClient().working
    raw = snapshot()
    first = calculate_snapshot(config, db, raw)
    assert len(first["nights"]) == 2
    assert next(p for p in first["players"] if p["id"] == "solace")["bench"] == 45
    projected = build_attendance_projection(db)
    assert all(p["attendance"] == projected[p["name"]] for p in first["players"])
    assert all("attendance" not in p for n in first["nights"] for p in n["players"])
    assert "PRIVATE" not in json.dumps(first, default=str)
    raw["reports"][CODES[1]]["enabled"] = False
    second = calculate_snapshot(config, db, raw)
    assert [n["night"] for n in second["nights"]] == ["2026-10-15"]
    assert db.reports.count_documents({}) == 1
    assert db.fights_all.distinct("night_id") == ["2026-10-15"]
    raw["reports"][CODES[0]]["enabled"] = False
    empty = calculate_snapshot(config, db, raw)
    assert empty["nights"] == []
    assert all(p["bench"] == p["played"] == 0 for p in empty["players"])


def test_worker_validates_input_even_when_rules_are_bypassed(config, wcl):
    raw = snapshot()
    raw["reports"][CODES[0]]["command"] = "anything"
    with pytest.raises(ValueError):
        calculate_snapshot(config, mongomock.MongoClient().db, raw)
    with pytest.raises(ValueError):
        Availability(mode="partial", minutes=True)
    with pytest.raises(ValueError):
        Availability(mode="available", minutes=5)
    raw = snapshot()
    raw["players"]["solace"]["name"] = "Maggie"
    with pytest.raises(ValueError, match="unique"):
        validate_snapshot(raw)


def test_wall_times_reject_dst_gaps_and_ambiguity():
    assert wall_ms("2026-10-15T19:00") is not None
    with pytest.raises(ValueError, match="does not exist"):
        wall_ms("2026-03-08T02:30")
    with pytest.raises(ValueError, match="ambiguous"):
        wall_ms("2026-11-01T01:30")


@pytest.fixture
def emulator(config):
    if not os.getenv("FIRESTORE_EMULATOR_HOST"):
        pytest.skip(
            "Set FIRESTORE_EMULATOR_HOST to run Firestore worker integration tests"
        )
    client = firestore_client("demo-pebble")
    # Use a dedicated season and never contact a real Firebase project.
    season = client.document("seasons/worker-tests")
    for name in [
        "runs",
        "reports",
        "players",
        "characters",
        "nightOverrides",
        "availabilityOverrides",
        "settings",
    ]:
        for doc in season.collection(name).stream():
            doc.reference.delete()
    client.document("officers/test-officer").set({"enabled": True})
    season.set({"displayName": "Worker test season"})
    for name, docs in snapshot().items():
        for key, value in docs.items():
            season.collection(name).document(key).set(value)
    yield client, season
    client.close()


def test_real_firestore_run_publish_failure_and_revocation(
    config, emulator, wcl, monkeypatch
):
    client, season = emulator
    worker = Worker(config, client, mongomock.MongoClient())

    def queue(run_id):
        ref = season.collection("runs").document(run_id)
        ref.set(
            {
                "status": "queued",
                "requestedBy": "test-officer",
                "requestedAt": firestore.SERVER_TIMESTAMP,
            }
        )
        return ref

    ref = queue("first")
    raw, versions = capture_inputs(client, "worker-tests")
    assert len(raw["reports"]) == 2 and versions[f"reports/{CODES[0]}"] == 1
    # Progress must reach Firestore immediately, independently of the periodic
    # heartbeat, so the browser can show each real processing stage.
    observed = []
    publish = worker.publish

    def compute_with_progress(config, db, raw, *, stage):
        observed.append(ref.get().to_dict()["stage"])

        def progress(message):
            stage(message)
            observed.append(ref.get().to_dict()["stage"])

        return calculate_snapshot(config, db, raw, stage=progress)

    def publish_with_progress(run_ref, output):
        observed.append(run_ref.get().to_dict()["stage"])
        return publish(run_ref, output)

    with monkeypatch.context() as patch:
        patch.setattr("pebble.web_worker.calculate_snapshot", compute_with_progress)
        patch.setattr(worker, "publish", publish_with_progress)
        heartbeat = Mock()
        patch.setattr(worker, "heartbeat", heartbeat)
        assert worker.process_next()
        heartbeat.assert_not_called()
    assert observed == [
        "Validating inputs",
        "Fetching reports",
        "Calculating results",
        "Updating results",
    ]
    assert ref.get().to_dict()["status"] == "succeeded"
    pointer = client.document("publicSeasons/worker-tests")
    assert pointer.get().to_dict()["latestRunId"] == "first"
    assert (
        season.collection("results").document("first").get().to_dict()["complete"]
        is True
    )
    assert (
        len(
            list(
                season.collection("results")
                .document("first")
                .collection("nights")
                .stream()
            )
        )
        == 2
    )
    published_players = list(
        season.collection("results").document("first").collection("players").stream()
    )
    projected = build_attendance_projection(worker.db)
    assert all(
        p.to_dict()["attendance"] == projected[p.to_dict()["name"]]
        for p in published_players
    )
    assert not worker.process_next(), "completed requests must not run twice"

    def interrupted(run_ref, _output):
        season.collection("results").document(run_ref.id).set({"complete": False})
        raise RuntimeError("simulated upload interruption")

    monkeypatch.setattr(worker, "publish", interrupted)
    failed = queue("failed-upload")
    worker.process_next()
    assert failed.get().to_dict()["status"] == "failed"
    assert pointer.get().to_dict()["latestRunId"] == "first"
    assert (
        season.collection("results")
        .document("failed-upload")
        .get()
        .to_dict()["complete"]
        is False
    )
    assert all(
        p.reference.get().to_dict()["attendance"] == p.to_dict()["attendance"]
        for p in published_players
    )
    # A request accepted before revocation must still fail at worker claim.
    revoked = queue("revoked")
    client.document("officers/test-officer").set({"enabled": False})
    worker.process_next()
    revoked_data = revoked.get().to_dict()
    assert revoked_data["status"] == "failed"
    assert revoked_data["error"] == "Requester no longer has officer access."
    assert not season.collection("results").document("revoked").get().exists
    assert pointer.get().to_dict()["latestRunId"] == "first"
    interrupted_ref = queue("interrupted")
    interrupted_ref.update({"status": "running"})
    worker.recover()
    assert interrupted_ref.get().to_dict()["status"] == "failed"


def test_listener_drains_requests_on_main_thread_and_unsubscribes(config, monkeypatch):

    worker = Worker(config, Mock(), mongomock.MongoClient())
    stop = threading.Event()
    watch = Mock(is_active=True)
    query = Mock()
    query.on_snapshot.side_effect = lambda callback: (callback([], [], None) or watch)
    monkeypatch.setattr(worker, "queued_runs", lambda: query)
    monkeypatch.setattr(worker, "recover", Mock())
    calls = []

    def process(stop=None):
        calls.append(threading.get_ident())
        if len(calls) == 3:
            stop.set()
            return False
        return True

    monkeypatch.setattr(worker, "process_next", process)
    worker.listen(stop)
    assert calls == [threading.get_ident()] * 3
    worker.recover.assert_called_once()
    query.on_snapshot.assert_called_once()
    query.stream.assert_not_called()
    watch.unsubscribe.assert_called_once()


def test_idle_listener_health_checks_do_not_query_firestore(config, monkeypatch):

    stop = threading.Event()
    waits = []

    class IdleWake:
        def wait(self, timeout):
            waits.append(timeout)
            if len(waits) == 3:
                stop.set()
            return False

    worker = Worker(config, Mock(), mongomock.MongoClient())
    watch = Mock(is_active=True)
    query = Mock()
    query.on_snapshot.return_value = watch
    monkeypatch.setattr(threading, "Event", IdleWake)
    monkeypatch.setattr(worker, "queued_runs", lambda: query)
    monkeypatch.setattr(worker, "recover", Mock())
    monkeypatch.setattr(worker, "process_next", Mock())
    worker.listen(stop)
    assert len(waits) == 3
    worker.process_next.assert_not_called()
    query.stream.assert_not_called()
    query.on_snapshot.assert_called_once()
    watch.unsubscribe.assert_called_once()


def test_listener_retries_startup_and_stopped_watch_with_backoff(config, monkeypatch):

    worker = Worker(config, Mock(), mongomock.MongoClient())
    stop = Mock()
    stop.is_set.return_value = False
    stop.wait.return_value = False
    dead = Mock(is_active=False)
    healthy = Mock(is_active=True)
    query = Mock()
    attempts = 0

    def attach(callback):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ServiceUnavailable("temporarily offline")
        if attempts == 2:
            return dead
        callback([], [], None)  # A new listener supplies the initial queue state.
        return healthy

    query.on_snapshot.side_effect = attach
    monkeypatch.setattr(worker, "queued_runs", lambda: query)
    monkeypatch.setattr(
        worker, "recover", Mock(side_effect=[ServiceUnavailable("offline"), None, None])
    )

    def process(stop=None):
        stop.is_set.return_value = True
        return False

    monkeypatch.setattr(worker, "process_next", process)
    worker.listen(stop)
    assert [c.args[0] for c in stop.wait.call_args_list] == [1, 2, 4]
    dead.unsubscribe.assert_called_once()
    healthy.unsubscribe.assert_called_once()
    assert worker.recover.call_count == 3


def test_listener_retry_delay_is_bounded_and_can_stop(config, monkeypatch):

    worker = Worker(config, Mock(), mongomock.MongoClient())
    stop = Mock()
    stop.is_set.return_value = False
    stop.wait.side_effect = [False] * 8 + [True]
    monkeypatch.setattr(
        worker, "recover", Mock(side_effect=ServiceUnavailable("offline"))
    )
    worker.listen(stop)
    assert [c.args[0] for c in stop.wait.call_args_list] == [
        1,
        2,
        4,
        8,
        16,
        32,
        60,
        60,
        60,
    ]


def test_listener_receives_real_firestore_requests_and_restarts(
    config, emulator, wcl, monkeypatch
):
    client, season = emulator
    worker = Worker(config, client, mongomock.MongoClient())
    stop = threading.Event()
    finished = threading.Event()
    first_finished = threading.Event()
    attached = threading.Event()
    watches = []
    failures = []
    process_next = worker.process_next
    queued_runs = worker.queued_runs
    calls = []

    def process(stop=None):
        calls.append(threading.get_ident())
        worked = process_next(stop=stop)
        first = season.collection("runs").document("listener-first").get().to_dict()
        if first and first["status"] == "succeeded":
            first_finished.set()
        second = season.collection("runs").document("listener-second").get().to_dict()
        if second and second["status"] == "succeeded":
            stop.set()
            finished.set()
        return worked

    def listen():
        try:
            worker.listen(stop)
        except Exception as exc:
            failures.append(exc)
            finished.set()

    monkeypatch.setattr(worker, "process_next", process)
    original_query = queued_runs()
    original_attach = type(original_query).on_snapshot

    def observe_attach(self, callback):
        watch = original_attach(self, callback)
        watches.append(watch)
        attached.set()
        return watch

    monkeypatch.setattr(type(original_query), "on_snapshot", observe_attach)

    # Existing work must be delivered by the initial snapshot.
    def queue(run_id):
        season.collection("runs").document(run_id).set(
            {
                "status": "queued",
                "requestedBy": "test-officer",
                "requestedAt": firestore.SERVER_TIMESTAMP,
            }
        )

    queue("listener-first")
    # SDK notifications run in the background; calculations run on the main
    # thread so the POSIX run deadline can interrupt blocked dependencies.
    main_id = threading.get_ident()

    def producer():
        try:
            assert attached.wait(10)
            assert first_finished.wait(10)
            attached.clear()
            watches[0].unsubscribe()
            assert attached.wait(15), "stopped listener was not recreated"
            queue("listener-second")
            assert finished.wait(10), "listener did not pick up work after restarting"
        except BaseException as exc:
            failures.append(exc)
            stop.set()

    thread = threading.Thread(target=producer, daemon=True)
    thread.start()
    try:
        listen()
        assert not failures
        assert len(watches) >= 2
        assert set(calls) == {main_id}, "all processing must remain serial"
        for name in ("listener-first", "listener-second"):
            assert (
                season.collection("runs").document(name).get().to_dict()["status"]
                == "succeeded"
            )
    finally:
        stop.set()
        thread.join(timeout=12)
        assert not thread.is_alive()


def test_worker_heartbeat_runs_at_most_once_per_minute(config, monkeypatch):
    worker = Worker(config, Mock(), mongomock.MongoClient())
    stop = threading.Event()
    waited = threading.Event()
    intervals = []

    original_wait = threading.Event.wait

    def wait(event, interval=None):
        if interval == 60:
            intervals.append(interval)
            stop.set()
            event.set()
            waited.set()
            return True
        return original_wait(event, interval)

    monkeypatch.setattr(threading.Event, "wait", wait)
    monkeypatch.setattr(worker, "heartbeat", Mock())
    monkeypatch.setattr(worker, "listen", lambda _: waited.wait(1))
    worker.run(stop=stop)
    worker.heartbeat.assert_called_once()
    assert intervals == [60]


@pytest.mark.parametrize(
    "scenario",
    [
        "multiple_reports",
        "excluded",
        "no_mythic",
        "positive_short_half",
        "negative_minutes",
        "eligibility",
        "zero_played",
        "rounding",
        "alias",
        "pacific_boundary",
    ],
)
def test_adapter_parity_representative_nights(config, monkeypatch, wcl, scenario):
    """Compare web adaptation with the established Sheets calculation inputs."""
    import logging
    from copy import deepcopy

    from pebble.ingest import persist_report_fights
    from pebble.pipeline import compute_results
    from pebble.utils.time import night_id_from_ms
    from pebble.wcl_client import WCLClient
    from pebble.web_models import validate_snapshot

    raw = snapshot()
    raw["availabilityOverrides"] = {}
    raw["settings"] = {}
    fetch = WCLClient.fetch_report_bundle
    bundles = {code: fetch(None, code) for code in CODES}
    if scenario == "multiple_reports":
        bundles[CODES[1]] = deepcopy(bundles[CODES[0]])
        # Overlapping reports must deduplicate fights, rather than double time.
    elif scenario == "excluded":
        raw["reports"][CODES[1]]["enabled"] = False
    elif scenario == "no_mythic":
        for bundle in bundles.values():
            for fight in bundle["fights"]:
                fight["difficulty"] = 4
    elif scenario in ("positive_short_half", "negative_minutes"):
        raw["availabilityOverrides"]["2026-10-15_solace"] = meta(
            nightId="2026-10-15",
            playerId="solace",
            pre=(
                {"mode": "partial", "minutes": 120}
                if scenario == "positive_short_half"
                else {"mode": "unavailableFor", "minutes": 5}
            ),
            post={"mode": "available"},
        )
    elif scenario == "eligibility":
        raw["players"]["solace"]["joinDate"] = "2026-10-15"
        raw["players"]["maggie"]["leaveDate"] = "2026-10-14"
        raw["players"]["inactive"] = meta(
            name="Inactive", className="Druid", active=False
        )
    elif scenario == "zero_played":
        raw["availabilityOverrides"]["2026-10-15_solace"] = meta(
            nightId="2026-10-15",
            playerId="solace",
            pre={"mode": "available"},
            post={"mode": "available"},
        )
    elif scenario == "rounding":
        for bundle in bundles.values():
            bundle["fights"][0]["endTime"] += 29_000
            bundle["fights"][1]["endTime"] += 31_000
            bundle["endTime"] += 31_000
    elif scenario == "alias":
        raw["characters"]["alt"] = meta(
            name="Alt", realm="TestRealm", region="us", playerId="maggie"
        )
        for bundle in bundles.values():
            bundle["region"] = {"compactName": "US"}
            bundle["masterData"]["actors"][0]["name"] = "Alt"
    elif scenario == "pacific_boundary":
        for bundle in bundles.values():
            # UTC's next date is still the raid's Pacific date, across DST change.
            bundle["startTime"] = wall_ms("2026-11-01T18:00")
            bundle["endTime"] = bundle["startTime"] + 4_200_000
    monkeypatch.setattr(
        WCLClient, "fetch_report_bundle", lambda _, code: deepcopy(bundles[code])
    )
    web_db = mongomock.MongoClient().web
    output = calculate_snapshot(config, web_db, raw)
    inputs = validate_snapshot(raw)
    legacy = mongomock.MongoClient().legacy
    legacy.team_roster.insert_many(
        [
            {
                "main": p.name,
                "active": p.active,
                "join_night": p.joinDate,
                "leave_night": p.leaveDate,
            }
            for p in inputs["players"].values()
        ]
    )
    for code, report in inputs["reports"].items():
        if not report.enabled:
            continue
        b = bundles[code]
        night = night_id_from_ms(b["startTime"])
        legacy.reports.insert_one(
            {
                "code": code,
                "night_id": night,
                "start_ms": b["startTime"],
                "end_ms": b["endTime"],
            }
        )
        persist_report_fights(legacy, code, b, b["startTime"], night)
    aliases = {
        f"{c.name}-{c.realm}": inputs["players"][c.playerId].name
        for c in inputs["characters"].values()
    }
    overrides = {}
    for a in inputs["availabilityOverrides"].values():
        overrides.setdefault(a.nightId, {})[inputs["players"][a.playerId].name] = {
            "pre": a.pre.effective(),
            "post": a.post.effective(),
        }
    compute_results(
        config,
        legacy,
        logging.getLogger("parity"),
        roster_map=aliases,
        overrides_by_night=overrides,
    )
    fields = (
        "night_id",
        "main",
        "played_pre_min",
        "played_post_min",
        "bench_pre_min",
        "bench_post_min",
    )

    def rows(db):
        return sorted(tuple(r[k] for k in fields) for r in db.bench_night_totals.find())

    assert rows(web_db) == rows(legacy)
    week_fields = (
        "game_week",
        "main",
        "played_min",
        "bench_min",
        "bench_pre_min",
        "bench_post_min",
    )

    def weeks(db):
        return sorted(
            tuple(r[k] for k in week_fields) for r in db.bench_week_totals.find()
        )

    assert weeks(web_db) == weeks(legacy)
    for p in output["players"]:
        assert p["played"] == p["prePlayed"] + p["postPlayed"]
        assert p["bench"] == p["preBench"] + p["postBench"]
        assert p["played"] == sum(
            n["players"][i]["played"]
            for n in output["nights"]
            for i, v in enumerate(n["players"])
            if v["id"] == p["id"]
        )
    if scenario == "no_mythic":
        assert not output["nights"] and output["warnings"]
    if scenario == "positive_short_half":
        assert next(p for p in output["players"] if p["id"] == "solace")["bench"] <= 120


def test_attendance_projection_omits_ineligible_season_players(config, wcl):
    raw = snapshot()
    for pid, values in {
        "future": {"name": "Future", "joinDate": "2026-10-16"},
        "former": {"name": "Former", "leaveDate": "2026-10-14"},
        "inactive": {"name": "Inactive", "active": False},
    }.items():
        raw["players"][pid] = meta(
            name=values["name"],
            className="Mage",
            role="Damage",
            active=values.get("active", True),
            joinDate=values.get("joinDate", ""),
            leaveDate=values.get("leaveDate", ""),
        )
    output = calculate_snapshot(config, mongomock.MongoClient().db, raw)
    rows = {player["id"]: player for player in output["players"]}
    assert "inactive" not in rows
    assert "attendance" not in rows["future"]
    assert "attendance" not in rows["former"]


def test_attendance_payload_is_preflighted_before_publication(config, emulator):
    from pebble.web_worker import InputError

    client, season = emulator
    worker = Worker(config, client, mongomock.MongoClient())
    pointer = client.document("publicSeasons/worker-tests")
    pointer.set({"latestRunId": "previous-attendance"})
    root = season.collection("results").document("previous-attendance")
    root.set({"complete": True})
    old_player = root.collection("players").document("example")
    old_player.set(
        {
            "attendance": {
                "played": 10,
                "bench": 0,
                "possible": 10,
                "rate": 1,
                "weeks": [],
            }
        }
    )
    run = season.collection("runs").document("oversized-attendance")
    run.set({"status": "running"})
    output = {
        "players": [
            {
                "id": "example",
                "attendance": {
                    "played": 10,
                    "bench": 0,
                    "possible": 10,
                    "rate": 1,
                    "weeks": [{"week": "2026-10-13", "status": "PBO"}] * 30000,
                },
            }
        ],
        "nights": [],
        "weeks": [],
        "reportCount": 0,
        "warnings": [],
    }
    with pytest.raises(InputError, match="size budget"):
        worker.publish(run, output)
    assert not season.collection("results").document(run.id).get().exists
    assert pointer.get().to_dict()["latestRunId"] == "previous-attendance"
    assert old_player.get().to_dict()["attendance"]["possible"] == 10


@pytest.mark.parametrize("scenario", ["zero", "membership", "ties", "no_mythic"])
def test_public_season_ranking_matches_established_materialization(
    config, wcl, monkeypatch, scenario
):
    """Use recorded ranking eligibility, totals, ordering and undefined ratios."""
    from copy import deepcopy

    from pebble.wcl_client import WCLClient

    raw = snapshot()
    raw["availabilityOverrides"] = {}
    raw["players"] = {
        "alpha": meta(name="Alpha", className="Mage", active=True),
        "beta": meta(name="Beta", className="Priest", active=True),
    }
    if scenario == "membership":
        raw["players"]["alpha"]["leaveDate"] = "2026-10-14"
        raw["players"].update(
            former=meta(name="Former", className="Mage", leaveDate="2026-10-14"),
            future=meta(name="Future", className="Mage", joinDate="2026-10-16"),
            inactive=meta(name="Inactive", className="Mage", active=False),
        )
    if scenario == "ties":
        raw["players"].update(
            zulu=meta(name="Zulu", className="Mage"),
            lower=meta(name="apple", className="Mage"),
        )
    original_fetch = WCLClient.fetch_report_bundle

    def fetch(client, code):
        bundle = deepcopy(original_fetch(client, code))
        bundle["masterData"]["actors"][0]["name"] = "Alpha"
        if scenario == "no_mythic":
            for fight in bundle["fights"]:
                fight["difficulty"] = 4
        return bundle

    monkeypatch.setattr(WCLClient, "fetch_report_bundle", fetch)
    db = mongomock.MongoClient().ranking_parity
    output = calculate_snapshot(config, db, raw)
    published = {p["name"]: p for p in output["players"]}
    expected = list(db.bench_rankings.find({}, {"_id": 0}).sort("rank", 1))
    actual = sorted(
        (p for p in output["players"] if p["ranking"] is not None),
        key=lambda p: p["ranking"]["rank"],
    )
    assert [p["name"] for p in actual] == [r["main"] for r in expected]
    for player, recorded in zip(actual, expected):
        assert player["ranking"] == {
            "rank": recorded["rank"],
            "played": recorded["played_min"],
            "bench": recorded["bench_min"],
            "ratio": recorded["bench_to_played_ratio"],
        }
        assert player["played"] == recorded["played_min"]
        assert player["bench"] == recorded["bench_min"]
    assert all("ranking" not in p for n in output["nights"] for p in n["players"])
    if scenario != "no_mythic":
        assert published["Beta"]["ranking"]["ratio"] is None
        assert published["Beta"]["ranking"]["played"] == 0
    if scenario == "membership":
        assert published["Alpha"]["ranking"] is None
        assert published["Alpha"]["played"] > 0
        assert published["Former"]["ranking"] is None
        assert "Inactive" not in published
        # Future join can share an observed game week; ranking and attendance
        # intentionally have different established membership boundaries.
        assert published["Future"]["ranking"] is not None
        assert "attendance" not in published["Future"]
        assert any(
            p["name"] == "Former" for n in output["nights"] for p in n["players"]
        )
    if scenario == "ties":
        tied = [p["name"] for p in actual if p["ranking"]["bench"] == 0]
        assert tied.index("Zulu") < tied.index("apple")
        assert [p["ranking"]["rank"] for p in actual] == list(range(1, len(actual) + 1))
    if scenario == "no_mythic":
        assert all(p["ranking"] is None for p in output["players"])
