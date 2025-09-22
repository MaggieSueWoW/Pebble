import mongomock
from datetime import datetime, timedelta
from types import SimpleNamespace

import pebble.cli as cli
from pebble.utils.time import PT


def test_compute_includes_not_on_roster(monkeypatch):
    db = mongomock.MongoClient().db

    night_id = "2024-07-10"
    base = datetime(2024, 7, 10, 19, 0, tzinfo=PT)
    report_start = int(base.timestamp() * 1000)
    report_end = int((base + timedelta(hours=4)).timestamp() * 1000)
    fight_start = int((base + timedelta(minutes=30)).timestamp() * 1000)
    fight_end = int((base + timedelta(minutes=40)).timestamp() * 1000)

    db["reports"].insert_one(
        {
            "night_id": night_id,
            "code": "R1",
            "start_ms": report_start,
            "end_ms": report_end,
        }
    )
    db["fights_all"].insert_one(
        {
            "night_id": night_id,
            "report_code": "R1",
            "fight_abs_start_ms": fight_start,
            "fight_abs_end_ms": fight_end,
            "participants": [
                {"name": "Alice-Illidan"},
                {"name": "Bobalt-Illidan"},
            ],
            "encounter_id": 1,
            "is_mythic": True,
            "id": 1,
        }
    )

    db["team_roster"].insert_one({"main": "Alice-Illidan", "active": True})

    captured = {}

    def fake_replace_values(spreadsheet_id, tab, values, creds_path, **kwargs):
        captured[tab] = values

    def fake_sheet_values(settings, tab, start, last_processed):
        if tab == settings.sheets.tabs.roster_map:
            return [["Alt", "Main"], ["Bobalt-Illidan", "Bob-Illidan"]]
        if tab == settings.sheets.tabs.availability_overrides:
            return []
        return []

    def fake_logging():
        return SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None)

    settings = SimpleNamespace(
        service_account_json="creds.json",
        sheets=SimpleNamespace(
            spreadsheet_id="sheet",
            tabs=SimpleNamespace(
                roster_map="Roster Map",
                availability_overrides="Availability Overrides",
                night_qa="Night QA",
                bench_night_totals="Bench Night Totals",
            ),
            starts=SimpleNamespace(
                roster_map="A2",
                availability_overrides="A2",
                night_qa="A1",
                bench_night_totals="A1",
            ),
            last_processed=SimpleNamespace(
                roster_map="B2",
                availability_overrides="B2",
                night_qa="B1",
                bench_night_totals="B1",
            ),
        ),
        time=SimpleNamespace(
            break_window=SimpleNamespace(
                start_pt="19:30",
                end_pt="21:00",
                min_gap_minutes=10,
                max_gap_minutes=30,
            ),
            mythic_post_extension_min=5,
        ),
    )

    monkeypatch.setattr("pebble.cli.load_settings", lambda _: settings)
    monkeypatch.setattr("pebble.cli.get_db", lambda s: db)
    monkeypatch.setattr("pebble.cli.ensure_indexes", lambda db: None)
    monkeypatch.setattr("pebble.cli.setup_logging", fake_logging)
    monkeypatch.setattr("pebble.cli.replace_values", fake_replace_values)
    monkeypatch.setattr("pebble.cli._sheet_values", fake_sheet_values)
    cli.compute.callback("config.yaml")

    assert settings.sheets.tabs.night_qa in captured
    header = captured[settings.sheets.tabs.night_qa][0]
    assert "Not on Roster" in header
    assert "Mythic Post Extension (min)" in header
    idx = header.index("Not on Roster")
    data_row = captured[settings.sheets.tabs.night_qa][1]
    assert data_row[idx] == "Bob-Illidan"
    ext_idx = header.index("Mythic Post Extension (min)")
    assert data_row[ext_idx] == "0.00"


def test_compute_extends_last_mythic_players(monkeypatch):
    db = mongomock.MongoClient().db

    night_id = "2024-07-10"
    base = datetime(2024, 7, 10, 19, 0, tzinfo=PT)
    report_start = int(base.timestamp() * 1000)
    report_end = int((base + timedelta(hours=4)).timestamp() * 1000)
    break_start = int((base + timedelta(minutes=60)).timestamp() * 1000)
    break_end = int((base + timedelta(minutes=75)).timestamp() * 1000)
    mythic_pre_start = int((base + timedelta(minutes=30)).timestamp() * 1000)
    mythic_pre_end = int((base + timedelta(minutes=45)).timestamp() * 1000)
    mythic_post_start = int((base + timedelta(minutes=80)).timestamp() * 1000)
    mythic_post_end = int((base + timedelta(minutes=90)).timestamp() * 1000)

    db["reports"].insert_one(
        {
            "night_id": night_id,
            "code": "R1",
            "start_ms": report_start,
            "end_ms": report_end,
            "break_override_start_ms": break_start,
            "break_override_end_ms": break_end,
        }
    )

    db["fights_all"].insert_many(
        [
            {
                "night_id": night_id,
                "report_code": "R1",
                "fight_abs_start_ms": mythic_pre_start,
                "fight_abs_end_ms": mythic_pre_end,
                "participants": [
                    {"name": "Alice-Illidan"},
                    {"name": "Bob-Illidan"},
                ],
                "encounter_id": 1,
                "is_mythic": True,
                "id": 1,
            },
            {
                "night_id": night_id,
                "report_code": "R1",
                "fight_abs_start_ms": mythic_post_start,
                "fight_abs_end_ms": mythic_post_end,
                "participants": [
                    {"name": "Alice-Illidan"},
                    {"name": "Charlie-Illidan"},
                ],
                "encounter_id": 2,
                "is_mythic": True,
                "id": 2,
            },
        ]
    )

    db["team_roster"].insert_many(
        [
            {"main": "Alice-Illidan", "active": True},
            {"main": "Bob-Illidan", "active": True},
            {"main": "Charlie-Illidan", "active": True},
        ]
    )

    captured = {}

    def fake_replace_values(spreadsheet_id, tab, values, creds_path, **kwargs):
        captured[tab] = values

    def fake_sheet_values(settings, tab, start, last_processed):
        if tab == settings.sheets.tabs.roster_map:
            return []
        if tab == settings.sheets.tabs.availability_overrides:
            return []
        return []

    def fake_logging():
        return SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None)

    settings = SimpleNamespace(
        service_account_json="creds.json",
        sheets=SimpleNamespace(
            spreadsheet_id="sheet",
            tabs=SimpleNamespace(
                roster_map="Roster Map",
                availability_overrides="Availability Overrides",
                night_qa="Night QA",
                bench_night_totals="Bench Night Totals",
            ),
            starts=SimpleNamespace(
                roster_map="A2",
                availability_overrides="A2",
                night_qa="A1",
                bench_night_totals="A1",
            ),
            last_processed=SimpleNamespace(
                roster_map="B2",
                availability_overrides="B2",
                night_qa="B1",
                bench_night_totals="B1",
            ),
        ),
        time=SimpleNamespace(
            break_window=SimpleNamespace(
                start_pt="19:30",
                end_pt="21:00",
                min_gap_minutes=10,
                max_gap_minutes=30,
            ),
            mythic_post_extension_min=5,
        ),
    )

    monkeypatch.setattr("pebble.cli.load_settings", lambda _: settings)
    monkeypatch.setattr("pebble.cli.get_db", lambda s: db)
    monkeypatch.setattr("pebble.cli.ensure_indexes", lambda db: None)
    monkeypatch.setattr("pebble.cli.setup_logging", fake_logging)
    monkeypatch.setattr("pebble.cli.replace_values", fake_replace_values)
    monkeypatch.setattr("pebble.cli._sheet_values", fake_sheet_values)

    cli.compute.callback("config.yaml")

    bench_docs = list(db["bench_night_totals"].find({}, {"_id": 0}))

    def doc_for(main_prefix):
        return next(doc for doc in bench_docs if doc["main"].startswith(main_prefix))

    assert doc_for("Alice")["played_post_min"] == 15
    assert doc_for("Charlie")["played_post_min"] == 15
    assert doc_for("Bob")["played_post_min"] == 0

def test_compute_removes_stale_bench_entries(monkeypatch):
    db = mongomock.MongoClient().db

    night_id = "2024-07-10"
    base = datetime(2024, 7, 10, 19, 0, tzinfo=PT)
    report_start = int(base.timestamp() * 1000)
    report_end = int((base + timedelta(hours=4)).timestamp() * 1000)
    fight_start = int((base + timedelta(minutes=30)).timestamp() * 1000)
    fight_end = int((base + timedelta(minutes=40)).timestamp() * 1000)

    db["reports"].insert_one(
        {
            "night_id": night_id,
            "code": "R1",
            "start_ms": report_start,
            "end_ms": report_end,
        }
    )
    db["fights_all"].insert_one(
        {
            "night_id": night_id,
            "report_code": "R1",
            "fight_abs_start_ms": fight_start,
            "fight_abs_end_ms": fight_end,
            "participants": [{"name": "Alice-Illidan"}],
            "encounter_id": 1,
            "is_mythic": True,
            "id": 1,
        }
    )

    db["team_roster"].insert_one({"main": "Alice-Illidan", "active": True})

    bench_sequences = [
        [
            {
                "main": "Alice-Illidan",
                "played_pre_min": 0,
                "played_post_min": 0,
                "played_total_min": 0,
                "bench_pre_min": 10,
                "bench_post_min": 0,
                "bench_total_min": 10,
                "avail_pre": True,
                "avail_post": False,
                "status_source": "roster",
            },
            {
                "main": "Bob-Illidan",
                "played_pre_min": 0,
                "played_post_min": 0,
                "played_total_min": 0,
                "bench_pre_min": 5,
                "bench_post_min": 5,
                "bench_total_min": 10,
                "avail_pre": True,
                "avail_post": True,
                "status_source": "roster",
            },
        ],
        [
            {
                "main": "Alice-Illidan",
                "played_pre_min": 0,
                "played_post_min": 0,
                "played_total_min": 0,
                "bench_pre_min": 10,
                "bench_post_min": 0,
                "bench_total_min": 10,
                "avail_pre": True,
                "avail_post": False,
                "status_source": "roster",
            }
        ],
    ]

    def fake_bench_minutes(*args, **kwargs):
        assert bench_sequences, "No bench sequence available"
        return bench_sequences.pop(0)

    captured = {}

    def fake_replace_values(spreadsheet_id, tab, values, creds_path, **kwargs):
        captured.setdefault(tab, []).append(values)

    def fake_sheet_values(settings, tab, start, last_processed):
        if tab == settings.sheets.tabs.roster_map:
            return []
        if tab == settings.sheets.tabs.availability_overrides:
            return []
        return []

    def fake_logging():
        return SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None)

    settings = SimpleNamespace(
        service_account_json="creds.json",
        sheets=SimpleNamespace(
            spreadsheet_id="sheet",
            tabs=SimpleNamespace(
                roster_map="Roster Map",
                availability_overrides="Availability Overrides",
                night_qa="Night QA",
                bench_night_totals="Bench Night Totals",
            ),
            starts=SimpleNamespace(
                roster_map="A2",
                availability_overrides="A2",
                night_qa="A1",
                bench_night_totals="A1",
            ),
            last_processed=SimpleNamespace(
                roster_map="B2",
                availability_overrides="B2",
                night_qa="B1",
                bench_night_totals="B1",
            ),
        ),
        time=SimpleNamespace(
            break_window=SimpleNamespace(
                start_pt="19:30",
                end_pt="21:00",
                min_gap_minutes=10,
                max_gap_minutes=30,
            ),
            mythic_post_extension_min=5,
        ),
    )

    monkeypatch.setattr("pebble.cli.load_settings", lambda _: settings)
    monkeypatch.setattr("pebble.cli.get_db", lambda s: db)
    monkeypatch.setattr("pebble.cli.ensure_indexes", lambda db: None)
    monkeypatch.setattr("pebble.cli.setup_logging", fake_logging)
    monkeypatch.setattr("pebble.cli.replace_values", fake_replace_values)
    monkeypatch.setattr("pebble.cli._sheet_values", fake_sheet_values)
    monkeypatch.setattr("pebble.cli.bench_minutes_for_night", fake_bench_minutes)

    cli.compute.callback("config.yaml")

    docs = list(db["bench_night_totals"].find({}, {"_id": 0}))
    assert {d["main"] for d in docs} == {"Alice-Illidan", "Bob-Illidan"}

    # Second run should remove Bob's entry
    cli.compute.callback("config.yaml")

    docs = list(db["bench_night_totals"].find({}, {"_id": 0}))
    assert {d["main"] for d in docs} == {"Alice-Illidan"}


def test_compute_refreshes_weekly_rankings(monkeypatch):
    db = mongomock.MongoClient().db

    night_id = "2024-07-10"
    base = datetime(2024, 7, 10, 19, 0, tzinfo=PT)
    report_start = int(base.timestamp() * 1000)
    report_end = int((base + timedelta(hours=4)).timestamp() * 1000)
    fight_start = int((base + timedelta(minutes=30)).timestamp() * 1000)
    fight_end = int((base + timedelta(minutes=40)).timestamp() * 1000)

    db["reports"].insert_one(
        {
            "night_id": night_id,
            "code": "R1",
            "start_ms": report_start,
            "end_ms": report_end,
        }
    )
    db["fights_all"].insert_one(
        {
            "night_id": night_id,
            "report_code": "R1",
            "fight_abs_start_ms": fight_start,
            "fight_abs_end_ms": fight_end,
            "participants": [{"name": "Alice-Illidan"}],
            "encounter_id": 1,
            "is_mythic": True,
            "id": 1,
        }
    )

    db["team_roster"].insert_many(
        [
            {"main": "Alice-Illidan", "active": True},
            {"main": "Bob-Illidan", "active": True},
        ]
    )

    bench_sequences = [
        [
            {
                "main": "Alice-Illidan",
                "played_pre_min": 0,
                "played_post_min": 0,
                "played_total_min": 0,
                "bench_pre_min": 10,
                "bench_post_min": 0,
                "bench_total_min": 10,
                "avail_pre": True,
                "avail_post": False,
                "status_source": "roster",
            },
            {
                "main": "Bob-Illidan",
                "played_pre_min": 0,
                "played_post_min": 0,
                "played_total_min": 0,
                "bench_pre_min": 5,
                "bench_post_min": 5,
                "bench_total_min": 10,
                "avail_pre": True,
                "avail_post": True,
                "status_source": "roster",
            },
        ],
        [
            {
                "main": "Alice-Illidan",
                "played_pre_min": 0,
                "played_post_min": 0,
                "played_total_min": 0,
                "bench_pre_min": 8,
                "bench_post_min": 2,
                "bench_total_min": 10,
                "avail_pre": True,
                "avail_post": True,
                "status_source": "roster",
            }
        ],
    ]

    def fake_bench_minutes(*args, **kwargs):
        assert bench_sequences, "No bench sequence available"
        return bench_sequences.pop(0)

    captured = {}

    def fake_replace_values(spreadsheet_id, tab, values, creds_path, **kwargs):
        captured.setdefault(tab, []).append(values)

    def fake_sheet_values(settings, tab, start, last_processed):
        if tab == settings.sheets.tabs.roster_map:
            return []
        if tab == settings.sheets.tabs.availability_overrides:
            return []
        return []

    def fake_logging():
        return SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None)

    settings = SimpleNamespace(
        service_account_json="creds.json",
        sheets=SimpleNamespace(
            spreadsheet_id="sheet",
            tabs=SimpleNamespace(
                roster_map="Roster Map",
                availability_overrides="Availability Overrides",
                night_qa="Night QA",
                bench_night_totals="Bench Night Totals",
            ),
            starts=SimpleNamespace(
                roster_map="A2",
                availability_overrides="A2",
                night_qa="A1",
                bench_night_totals="A1",
            ),
            last_processed=SimpleNamespace(
                roster_map="B2",
                availability_overrides="B2",
                night_qa="B1",
                bench_night_totals="B1",
            ),
        ),
        time=SimpleNamespace(
            break_window=SimpleNamespace(
                start_pt="19:30",
                end_pt="21:00",
                min_gap_minutes=10,
                max_gap_minutes=30,
            ),
            mythic_post_extension_min=5,
        ),
    )

    monkeypatch.setattr("pebble.cli.load_settings", lambda _: settings)
    monkeypatch.setattr("pebble.cli.get_db", lambda s: db)
    monkeypatch.setattr("pebble.cli.ensure_indexes", lambda db: None)
    monkeypatch.setattr("pebble.cli.setup_logging", fake_logging)
    monkeypatch.setattr("pebble.cli.replace_values", fake_replace_values)
    monkeypatch.setattr("pebble.cli._sheet_values", fake_sheet_values)
    monkeypatch.setattr("pebble.cli.bench_minutes_for_night", fake_bench_minutes)

    cli.compute.callback("config.yaml")

    ranks = list(
        db["bench_rankings"].find(
            {},
            {
                "_id": 0,
                "rank": 1,
                "main": 1,
                "bench_min": 1,
                "played_min": 1,
                "bench_to_played_ratio": 1,
            },
        )
    )
    assert {r["main"] for r in ranks} == {"Alice-Illidan", "Bob-Illidan"}

    # Mark Bob inactive and rerun with updated bench sequences (only Alice)
    db["team_roster"].update_one({"main": "Bob-Illidan"}, {"$set": {"active": False}})

    cli.compute.callback("config.yaml")

    ranks = list(
        db["bench_rankings"].find(
            {},
            {
                "_id": 0,
                "rank": 1,
                "main": 1,
                "bench_min": 1,
                "played_min": 1,
                "bench_to_played_ratio": 1,
            },
        )
    )
    assert ranks == [
        {
            "rank": 1,
            "main": "Alice-Illidan",
            "bench_min": 10,
            "played_min": 0,
            "bench_to_played_ratio": None,
        }
    ]

    week_rows = list(
        db["bench_week_totals"].find(
            {}, {"_id": 0, "game_week": 1, "main": 1, "bench_min": 1}
        )
    )
    assert week_rows == [
        {
            "game_week": "2024-07-09",
            "main": "Alice-Illidan",
            "bench_min": 10,
        }
    ]
