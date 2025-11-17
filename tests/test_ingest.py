import mongomock
from types import SimpleNamespace
from pebble.ingest import ingest_roster


def _setup_monkeypatch(monkeypatch, db, updates):
    class FakeValues:
        def __init__(self, bucket):
            self._bucket = bucket

        def batchUpdate(self, spreadsheetId, body):
            class Req:
                def __init__(self, bucket, payload):
                    self._bucket = bucket
                    self._payload = payload
                    self._rest_path = "values.batchUpdate"

                def execute(self):
                    self._bucket.extend(self._payload.get("data", []))
                    return {}

            return Req(self._bucket, body)

    class FakeSpreadsheets:
        def __init__(self, bucket):
            self._bucket = bucket

        def values(self):
            return FakeValues(self._bucket)

    class FakeService:
        def __init__(self, bucket):
            self._bucket = bucket

        def spreadsheets(self):
            return FakeSpreadsheets(self._bucket)

    class FakeSheetsClient:
        def __init__(self, creds_path):
            self.svc = FakeService(updates)

        def execute(self, req):
            return req.execute()

    monkeypatch.setattr("pebble.ingest.get_db", lambda s: db)
    monkeypatch.setattr("pebble.ingest.ensure_indexes", lambda db: None)
    monkeypatch.setattr("pebble.ingest.SheetsClient", FakeSheetsClient)

    s = SimpleNamespace(
        service_account_json="",
        sheets=SimpleNamespace(
            spreadsheet_id="sheet-1",
            tabs=SimpleNamespace(team_roster="Team Roster"),
            starts=SimpleNamespace(team_roster="A5"),
            last_processed=SimpleNamespace(team_roster="B3"),
        ),
    )
    return s, db


def test_ingest_roster_parses_sheet(monkeypatch):
    updates = []
    rows = [
        ["Main", "Class Color", "Join Date", "Leave Date", "Active?"],
        ["Alice-Illidan", "", "6/25/24", "", "Y"],
        ["Bob-Illidan", "#123456", "June 25, 2024", "", "n"],
        ["Charlie-Illidan", "#00FF00", "", "", ""],
    ]
    db = mongomock.MongoClient().db

    db["actors"].insert_many(
        [
            {"name": "Alice-Illidan", "subType": "Paladin"},
            {"name": "Bob-Illidan", "subType": "DeathKnight"},
        ]
    )

    s, db = _setup_monkeypatch(monkeypatch, db, updates)

    count = ingest_roster(s, rows=rows)
    docs = list(db["team_roster"].find({}, {"_id": 0}))
    assert count == 3
    assert sorted(docs, key=lambda r: r["main"]) == [
        {
            "main": "Alice-Illidan",
            "join_night": "2024-06-25",
            "leave_night": "",
            "active": True,
            "class_color": "#F58CBA",
        },
        {
            "main": "Bob-Illidan",
            "join_night": "2024-06-25",
            "leave_night": "",
            "active": False,
            "class_color": "#C41F3B",
        },
        {
            "main": "Charlie-Illidan",
            "join_night": "",
            "leave_night": "",
            "active": True,
            "class_color": "",
        },
    ]
    assert updates == [
        {"range": "Team Roster!B6", "values": [["#F58CBA"]]},
        {"range": "Team Roster!B7", "values": [["#C41F3B"]]},
    ]


def test_ingest_roster_handles_names_without_realm(monkeypatch):
    updates = []
    rows = [
        ["Main", "Class Color", "Join Date", "Leave Date", "Active?"],
        ["Alice", "", "6/25/24", "", "Y"],
        ["Bob", "#123456", "June 25, 2024", "", "n"],
    ]
    db = mongomock.MongoClient().db

    db["actors"].insert_many(
        [
            {"name": "Alice-Illidan", "subType": "Paladin"},
            {"name": "Bob-Illidan", "subType": "DeathKnight"},
        ]
    )

    s, db = _setup_monkeypatch(monkeypatch, db, updates)

    count = ingest_roster(s, rows=rows)
    docs = list(db["team_roster"].find({}, {"_id": 0}))
    assert count == 2
    assert sorted(docs, key=lambda r: r["main"]) == [
        {
            "main": "Alice",
            "join_night": "2024-06-25",
            "leave_night": "",
            "active": True,
            "class_color": "#F58CBA",
        },
        {
            "main": "Bob",
            "join_night": "2024-06-25",
            "leave_night": "",
            "active": False,
            "class_color": "#C41F3B",
        },
    ]
    assert updates == [
        {"range": "Team Roster!B6", "values": [["#F58CBA"]]},
        {"range": "Team Roster!B7", "values": [["#C41F3B"]]},
    ]
