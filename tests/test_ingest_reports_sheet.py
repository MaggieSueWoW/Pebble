import mongomock
from datetime import datetime
from pebble.config_loader import Settings, SheetsConfig, MongoConfig, WCLConfig
from pebble.ingest import ingest_reports
from pebble.utils.time import ms_to_pt_sheets, PT


def test_ingest_reports_updates_sheet(monkeypatch):
    rows = [
        [
            "Report URL",
            "Status",
            "Last Checked PT",
            "Notes",
            "Break Override Start (PT)",
            "Break Override End (PT)",
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
        ],
    ]
    updates = []

    class DummyRequest:
        def __init__(self, data):
            self.data = data

        def execute(self):
            return self.data

    class DummyValues:
        def get(self, spreadsheetId, range):
            return DummyRequest({"values": rows})

        def batchUpdate(self, spreadsheetId, body):
            updates.extend(body["data"])
            return DummyRequest({})

    class DummySpreadsheets:
        def values(self):
            return DummyValues()

    class DummySvc:
        def spreadsheets(self):
            return DummySpreadsheets()

    class DummySheetsClient:
        def __init__(self, *args, **kwargs):
            self.svc = DummySvc()

        def execute(self, req):
            return req.execute()

    monkeypatch.setattr("pebble.ingest.SheetsClient", DummySheetsClient)

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
    monkeypatch.setattr("pebble.ingest.ensure_indexes", lambda db: None)

    fixed_now = datetime(2025, 4, 2, 18, 50, 49, tzinfo=PT)

    class DummyDateTime:
        @staticmethod
        def now(tz=None):
            return fixed_now if tz is None else fixed_now.astimezone(tz)

    monkeypatch.setattr("pebble.ingest.datetime", DummyDateTime)

    settings = Settings(
        sheets=SheetsConfig(spreadsheet_id="1"),
        mongo=MongoConfig(uri="mongodb://example"),
        wcl=WCLConfig(client_id="id", client_secret="secret"),
    )

    res = ingest_reports(settings)
    assert res["reports"] == 1
    update_map = {u["range"].split("!")[1]: u["values"][0][0] for u in updates}
    assert update_map["G2"] == "Report One"
    assert update_map["H2"] == ms_to_pt_sheets(1000)
    assert update_map["I2"] == ms_to_pt_sheets(2000)
    assert update_map["C2"] == ms_to_pt_sheets(int(fixed_now.timestamp() * 1000))
    assert update_map["J2"] == "Creator"
