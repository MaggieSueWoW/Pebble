import mongomock
from datetime import datetime
import logging
from pebble.config_loader import (
    Settings,
    SheetsConfig,
    SheetsTriggers,
    MongoConfig,
    WCLConfig,
)
from pebble.ingest import ingest_reports, _report_inputs_hash
from pebble.utils.time import ms_to_pt_sheets, PT


def test_ingest_reports_updates_sheet(monkeypatch):
    rows = [
        [
            "Report URL",
            "Status",
            "Last Checked (PT)",
            "Break Override Start (PT)",
            "Break Override End (PT)",
            "Mythic Override Start (PT)",
            "Mythic Override End (PT)",
            "Notes",
            "Report Name",
            "Report Start (PT)",
            "Report End (PT)",
            "Created By",
        ],
        [
            "https://www.warcraftlogs.com/reports/ABC123",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
        ],
    ]
    class DummySheetsClient:
        svc = None

        def execute(self, _req):
            raise AssertionError("ingest should not touch Sheets")

    sample_bundle = {
        "title": "Report One",
        "startTime": 1000,
        "endTime": 2000,
        "owner": {"name": "Creator"},
        "fights": [],
        "masterData": {"actors": []},
    }

    class DummyWCLClient:
        def __init__(self, *args, **kwargs):
            pass

        def fetch_report_bundle(self, code):
            return sample_bundle

    monkeypatch.setattr("pebble.ingest.WCLClient", DummyWCLClient)
    monkeypatch.setattr("pebble.ingest.get_db", lambda s: mongomock.MongoClient().db)

    fixed_now = datetime(2025, 4, 2, 18, 50, 49, tzinfo=PT)

    class DummyDateTime:
        @staticmethod
        def now(tz=None):
            return fixed_now if tz is None else fixed_now.astimezone(tz)

    monkeypatch.setattr("pebble.ingest.datetime", DummyDateTime)

    settings = Settings(
        sheets=SheetsConfig(
            spreadsheet_id="1",
            triggers=SheetsTriggers(ingest_compute_week="Reports!B2"),
        ),
        mongo=MongoConfig(uri="mongodb://example"),
        wcl=WCLConfig(client_id="id", client_secret="secret"),
    )

    client = DummySheetsClient()

    res = ingest_reports(settings, rows=rows, client=client)
    assert res["reports"] == 1
    update_map = {
        u["range"].split("!")[1]: u["values"][0][0] for u in res["sheet_updates"]
    }
    assert update_map["I6"] == "Report One"
    assert update_map["J6"] == ms_to_pt_sheets(1000)
    assert update_map["K6"] == ms_to_pt_sheets(2000)
    assert update_map["C6"] == ms_to_pt_sheets(int(fixed_now.timestamp() * 1000))
    assert update_map["L6"] == "Creator"


def test_ingest_reports_rejects_non_wcl_links(monkeypatch, caplog):
    rows = [
        [
            "Report URL",
            "Status",
            "Last Checked PT",
            "Break Override Start (PT)",
            "Break Override End (PT)",
            "Mythic Override Start (PT)",
            "Mythic Override End (PT)",
            "Notes",
            "Report Name",
            "Report Start (PT)",
            "Report End (PT)",
            "Created By",
        ],
        [
            "https://example.com/notwcl",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
        ],
    ]
    class DummySheetsClient:
        svc = None

        def execute(self, _req):
            raise AssertionError("ingest should not touch Sheets")
    monkeypatch.setattr("pebble.ingest.get_db", lambda s: mongomock.MongoClient().db)

    settings = Settings(
        sheets=SheetsConfig(
            spreadsheet_id="1",
            triggers=SheetsTriggers(ingest_compute_week="Reports!B2"),
        ),
        mongo=MongoConfig(uri="mongodb://example"),
        wcl=WCLConfig(client_id="id", client_secret="secret"),
    )

    client = DummySheetsClient()

    with caplog.at_level(logging.WARNING):
        res = ingest_reports(settings, rows=rows, client=client)

    assert res["reports"] == 0
    update_map = {
        u["range"].split("!")[1]: u["values"][0][0] for u in res["sheet_updates"]
    }
    assert update_map["B6"] == "Bad report link"
    assert "Bad report link at row" in caplog.text


def test_ingest_reports_marks_bad_links_on_fetch_error(monkeypatch, caplog):
    rows = [
        [
            "Report URL",
            "Status",
            "Last Checked PT",
            "Break Override Start (PT)",
            "Break Override End (PT)",
            "Mythic Override Start (PT)",
            "Mythic Override End (PT)",
            "Notes",
            "Report Name",
            "Report Start (PT)",
            "Report End (PT)",
            "Created By",
        ],
        [
            "https://www.warcraftlogs.com/reports/ABC123",  # missing last char
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
        ],
    ]
    class DummySheetsClient:
        svc = None

        def execute(self, _req):
            raise AssertionError("ingest should not touch Sheets")
    monkeypatch.setattr("pebble.ingest.get_db", lambda s: mongomock.MongoClient().db)

    class DummyWCLClient:
        def __init__(self, *args, **kwargs):
            pass

        def fetch_report_bundle(self, code):
            raise RuntimeError([{"message": "Unknown report"}])

    monkeypatch.setattr("pebble.ingest.WCLClient", DummyWCLClient)

    settings = Settings(
        sheets=SheetsConfig(
            spreadsheet_id="1",
            triggers=SheetsTriggers(ingest_compute_week="Reports!B2"),
        ),
        mongo=MongoConfig(uri="mongodb://example"),
        wcl=WCLConfig(client_id="id", client_secret="secret"),
    )

    client = DummySheetsClient()

    with caplog.at_level(logging.WARNING):
        res = ingest_reports(settings, rows=rows, client=client)

    assert res["reports"] == 0
    update_map = {
        u["range"].split("!")[1]: u["values"][0][0] for u in res["sheet_updates"]
    }
    assert update_map["B6"] == "Bad report link"
    assert "Failed to fetch WCL report bundle" in caplog.text


def _base_report_rows():
    return [
        [
            "Report URL",
            "Status",
            "Last Checked (PT)",
            "Break Override Start (PT)",
            "Break Override End (PT)",
            "Mythic Override Start (PT)",
            "Mythic Override End (PT)",
            "Notes",
            "Report Name",
            "Report Start (PT)",
            "Report End (PT)",
            "Created By",
        ],
        [
            "https://www.warcraftlogs.com/reports/ABC123",
            "",
            "",
            "8:00 PM",
            "8:15 PM",
            "8:30 PM",
            "11:00 PM",
            "Some note",
            "",
            "",
            "",
            "",
        ],
    ]


def _base_settings():
    return Settings(
        sheets=SheetsConfig(
            spreadsheet_id="1",
            triggers=SheetsTriggers(ingest_compute_week="Reports!B2"),
        ),
        mongo=MongoConfig(uri="mongodb://example"),
        wcl=WCLConfig(client_id="id", client_secret="secret"),
    )


def test_ingest_reports_skips_when_inputs_unchanged(monkeypatch):
    rows = _base_report_rows()
    db = mongomock.MongoClient().db
    existing_hash = _report_inputs_hash(
        "Some note", "8:00 PM", "8:15 PM", "8:30 PM", "11:00 PM"
    )
    db["reports"].insert_one(
        {
            "code": "ABC123",
            "inputs_hash": existing_hash,
            "ingested_at": datetime.now(PT),
        }
    )

    class DummySheetsClient:
        svc = None

        def execute(self, _req):
            raise AssertionError("ingest should not touch Sheets")

    class DummyWCLClient:
        def __init__(self, *args, **kwargs):
            pass

        def fetch_report_bundle(self, code):
            raise AssertionError("should not fetch WCL when skipping")

    monkeypatch.setattr("pebble.ingest.WCLClient", DummyWCLClient)
    monkeypatch.setattr("pebble.ingest.get_db", lambda s: db)

    settings = _base_settings()
    client = DummySheetsClient()

    res = ingest_reports(settings, rows=rows, client=client)
    assert res["reports"] == 0
    assert res["skipped_reports"] == 1
    assert res["sheet_updates"] == []


def test_ingest_reports_force_full_reingest(monkeypatch):
    rows = _base_report_rows()
    db = mongomock.MongoClient().db
    existing_hash = _report_inputs_hash(
        "Some note", "8:00 PM", "8:15 PM", "8:30 PM", "11:00 PM"
    )
    db["reports"].insert_one(
        {
            "code": "ABC123",
            "inputs_hash": existing_hash,
            "ingested_at": datetime(2024, 1, 1, tzinfo=PT),
        }
    )

    class DummySheetsClient:
        svc = None

        def execute(self, _req):
            raise AssertionError("ingest should not touch Sheets")

    sample_bundle = {
        "title": "Report One",
        "startTime": 1000,
        "endTime": 2000,
        "owner": {"name": "Creator"},
        "fights": [],
        "masterData": {"actors": []},
    }

    class DummyWCLClient:
        calls = 0

        def __init__(self, *args, **kwargs):
            pass

        def fetch_report_bundle(self, code):
            DummyWCLClient.calls += 1
            return sample_bundle

    monkeypatch.setattr("pebble.ingest.WCLClient", DummyWCLClient)
    monkeypatch.setattr("pebble.ingest.get_db", lambda s: db)

    settings = _base_settings()
    client = DummySheetsClient()

    res = ingest_reports(
        settings,
        rows=rows,
        client=client,
        force_full_reingest=True,
    )

    assert res["reports"] == 1
    assert res["skipped_reports"] == 0
    assert DummyWCLClient.calls == 1
