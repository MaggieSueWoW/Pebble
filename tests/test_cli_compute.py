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
            )
        ),
    )

    monkeypatch.setattr("pebble.cli.load_settings", lambda _: settings)
    monkeypatch.setattr("pebble.cli.get_db", lambda s: db)
    monkeypatch.setattr("pebble.cli.ensure_indexes", lambda db: None)
    monkeypatch.setattr("pebble.cli.setup_logging", fake_logging)
    monkeypatch.setattr("pebble.cli.replace_values", fake_replace_values)
    monkeypatch.setattr("pebble.cli._sheet_values", fake_sheet_values)
    monkeypatch.setattr(db["participation_m"], "bulk_write", lambda ops, ordered=False: None)
    monkeypatch.setattr(db["blocks"], "bulk_write", lambda ops, ordered=False: None)
    monkeypatch.setattr(db["bench_night_totals"], "bulk_write", lambda ops, ordered=False: None)

    cli.compute.callback("config.yaml")

    assert settings.sheets.tabs.night_qa in captured
    header = captured[settings.sheets.tabs.night_qa][0]
    assert "Not on Roster" in header
    idx = header.index("Not on Roster")
    data_row = captured[settings.sheets.tabs.night_qa][1]
    assert data_row[idx] == "Bob-Illidan"
