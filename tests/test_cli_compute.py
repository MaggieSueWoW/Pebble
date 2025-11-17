import mongomock
from datetime import datetime, timedelta
from types import SimpleNamespace

import pebble.cli as cli
from pebble.utils.time import PT


def _base_settings():
    tabs = SimpleNamespace(
        team_roster="Team Roster",
        reports="Reports",
        roster_map="Roster Map",
        availability_overrides="Availability Overrides",
        night_qa="Night QA",
        bench_night_totals="Bench Night Totals",
        bench_week_totals="Bench Week Totals",
        bench_rankings="Bench Rankings",
        attendance="Attendance",
    )
    starts = SimpleNamespace(
        team_roster="A5",
        reports="A5",
        roster_map="A2",
        availability_overrides="A2",
        night_qa="A1",
        bench_night_totals="A1",
        bench_week_totals="A1",
        bench_rankings="A1",
        attendance="A1",
    )
    last_processed = SimpleNamespace(
        team_roster="B3",
        reports="B3",
        roster_map="B2",
        availability_overrides="B2",
        night_qa="B1",
        bench_night_totals="B1",
        bench_week_totals="B1",
        bench_rankings="B1",
        attendance="B1",
    )
    sheets = SimpleNamespace(
        spreadsheet_id="sheet",
        tabs=tabs,
        starts=starts,
        last_processed=last_processed,
    )
    time = SimpleNamespace(
        break_window=SimpleNamespace(
            start_pt="19:30",
            end_pt="21:00",
            min_gap_minutes=10,
            max_gap_minutes=30,
        ),
        mythic_post_extension_min=5,
    )
    return SimpleNamespace(service_account_json="creds.json", sheets=sheets, time=time)


def _fake_log():
    return SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None)


def _sheet_map(settings, roster=None, overrides=None, attendance=None):
    return {
        settings.sheets.tabs.roster_map: roster or [],
        settings.sheets.tabs.availability_overrides: overrides or [],
        settings.sheets.tabs.attendance:
            attendance
            or [
                [
                    "Player",
                    "Attendance",
                    "Played",
                    "Bench",
                    "Possible",
                ]
            ],
    }


def _setup_pipeline(monkeypatch, db, settings, sheet_values_map, *, sheets_client_cls=None):
    monkeypatch.setattr("pebble.cli.get_db", lambda s: db)

    def fake_ingest_reports(_settings, *, rows=None, client=None):
        return {"reports": 0, "fights": 0}

    def fake_ingest_roster(_settings, *, rows=None, client=None):
        return db["team_roster"].count_documents({})

    monkeypatch.setattr("pebble.cli.ingest_reports", fake_ingest_reports)
    monkeypatch.setattr("pebble.cli.ingest_roster", fake_ingest_roster)

    def fake_batch(_settings, requests, client=None):
        values = {}
        for key, tab, *_ in requests:
            values[key] = sheet_values_map.get(tab, [])
        return values

    monkeypatch.setattr("pebble.cli._sheet_values_batch", fake_batch)

    class DummySheetsClient:
        def __init__(self, *_args, **_kwargs):
            self.svc = None

        def execute(self, req):
            return req

    monkeypatch.setattr(
        "pebble.cli.SheetsClient", sheets_client_cls or DummySheetsClient
    )


def test_run_pipeline_includes_not_on_roster(monkeypatch):
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

    def fake_build_requests(spreadsheet_id, tab, values, *, client=None, **kwargs):
        captured[tab] = values
        return []

    settings = _base_settings()
    roster_rows = [["Alt", "Main"], ["Bobalt-Illidan", "Bob-Illidan"]]

    monkeypatch.setattr(
        "pebble.cli.build_replace_values_requests", fake_build_requests
    )
    _setup_pipeline(monkeypatch, db, settings, _sheet_map(settings, roster_rows, []))

    cli.run_pipeline(settings, _fake_log())

    assert settings.sheets.tabs.night_qa in captured
    header = captured[settings.sheets.tabs.night_qa][0]
    assert "Not on Roster" in header
    assert "Mythic Post Extension (min)" in header
    idx = header.index("Not on Roster")
    data_row = captured[settings.sheets.tabs.night_qa][1]
    assert data_row[idx] == "Bob-Illidan"
    ext_idx = header.index("Mythic Post Extension (min)")
    assert data_row[ext_idx] == "0.00"


def test_run_pipeline_extends_last_mythic_players(monkeypatch):
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

    def fake_build_requests(spreadsheet_id, tab, values, *, client=None, **kwargs):
        captured[tab] = values
        return []

    settings = _base_settings()

    monkeypatch.setattr(
        "pebble.cli.build_replace_values_requests", fake_build_requests
    )
    _setup_pipeline(monkeypatch, db, settings, _sheet_map(settings))

    cli.run_pipeline(settings, _fake_log())

    bench_docs = list(db["bench_night_totals"].find({}, {"_id": 0}))

    def doc_for(main_prefix):
        return next(doc for doc in bench_docs if doc["main"].startswith(main_prefix))

    assert doc_for("Alice")["played_post_min"] == 15
    assert doc_for("Charlie")["played_post_min"] == 15
    assert doc_for("Bob")["played_post_min"] == 0


def test_run_pipeline_removes_stale_bench_entries(monkeypatch):
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

    def fake_build_requests(spreadsheet_id, tab, values, *, client=None, **kwargs):
        captured.setdefault(tab, []).append(values)
        return []

    settings = _base_settings()

    monkeypatch.setattr(
        "pebble.cli.build_replace_values_requests", fake_build_requests
    )
    monkeypatch.setattr("pebble.cli.bench_minutes_for_night", fake_bench_minutes)
    _setup_pipeline(monkeypatch, db, settings, _sheet_map(settings))

    cli.run_pipeline(settings, _fake_log())

    docs = list(db["bench_night_totals"].find({}, {"_id": 0}))
    assert {d["main"] for d in docs} == {"Alice-Illidan", "Bob-Illidan"}

    # Second run should remove Bob's entry
    cli.run_pipeline(settings, _fake_log())

    docs = list(db["bench_night_totals"].find({}, {"_id": 0}))
    assert {d["main"] for d in docs} == {"Alice-Illidan"}


def test_run_pipeline_refreshes_weekly_rankings(monkeypatch):
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

    def fake_build_requests(spreadsheet_id, tab, values, *, client=None, **kwargs):
        captured.setdefault(tab, []).append(values)
        return []

    settings = _base_settings()

    monkeypatch.setattr(
        "pebble.cli.build_replace_values_requests", fake_build_requests
    )
    monkeypatch.setattr("pebble.cli.bench_minutes_for_night", fake_bench_minutes)
    _setup_pipeline(monkeypatch, db, settings, _sheet_map(settings))

    cli.run_pipeline(settings, _fake_log())

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

    cli.run_pipeline(settings, _fake_log())

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

    week_rows = list(db["bench_week_totals"].find({}, {"_id": 0, "game_week": 1, "main": 1, "bench_min": 1}))
    assert week_rows == [
        {
            "game_week": "2024-07-09",
            "main": "Alice-Illidan",
            "bench_min": 10,
        }
    ]


def test_run_pipeline_batches_ingest_sheet_updates(monkeypatch):
    db = mongomock.MongoClient().db
    settings = _base_settings()

    captured_values = []
    captured_requests = []
    captured_ingest_updates = []

    class RecordingSheetsClient:
        def __init__(self, *_args, **_kwargs):
            class DummyRequest:
                def __init__(self, body, kind):
                    self.body = body
                    self.kind = kind

                def execute(self):
                    if self.kind == "values":
                        captured_values.append(self.body)
                    elif self.kind == "requests":
                        captured_requests.append(self.body)
                    return {}

            class DummyValues:
                def batchUpdate(self_inner, spreadsheetId, body):
                    return DummyRequest(body, "values")

            class DummySpreadsheets:
                def values(self_inner):
                    return DummyValues()

                def batchUpdate(self_inner, spreadsheetId, body):
                    return DummyRequest(body, "requests")

            self._spreadsheets = DummySpreadsheets()
            self.svc = SimpleNamespace(spreadsheets=lambda: self._spreadsheets)

        def execute(self, req):
            return req.execute()

    monkeypatch.setattr(
        "pebble.cli.build_replace_values_requests", lambda *a, **k: []
    )

    def _fake_value_requests(spreadsheet_id, updates, *, client):
        captured_ingest_updates.append(updates)
        return [{"updateCells": {"range": {"sheetId": 123}}}]

    monkeypatch.setattr(
        "pebble.cli.build_value_update_requests", _fake_value_requests
    )

    _setup_pipeline(
        monkeypatch,
        db,
        settings,
        _sheet_map(settings, [], []),
        sheets_client_cls=RecordingSheetsClient,
    )

    def ingest_with_updates(_settings, *, rows=None, client=None):
        return {
            "reports": 0,
            "fights": 0,
            "sheet_updates": [
                {
                    "range": f"{settings.sheets.tabs.reports}!B6",
                    "values": [["Bad report link"]],
                }
            ],
        }

    monkeypatch.setattr("pebble.cli.ingest_reports", ingest_with_updates)

    cli.run_pipeline(settings, _fake_log())

    assert not captured_values  # no separate values().batchUpdate call
    assert captured_ingest_updates == [
        [
            {
                "range": f"{settings.sheets.tabs.reports}!B6",
                "values": [["Bad report link"]],
            }
        ]
    ]
    assert captured_requests == [{"requests": [{"updateCells": {"range": {"sheetId": 123}}}]}]
