import pebble.export_sheets as es


def test_replace_values_user_entered(monkeypatch):
    updates = {}

    class FakeReq:
        def __init__(self, bucket):
            self.bucket = bucket
        def execute(self):
            self.bucket["executed"] = True
            return None

    class FakeValues:
        def clear(self, **kwargs):
            updates["clear"] = kwargs
            return FakeReq(updates)
        def update(self, **kwargs):
            updates.update({"update": kwargs})
            return FakeReq(updates)

    class FakeSpreadsheets:
        def values(self):
            return FakeValues()

    class FakeSvc:
        def spreadsheets(self):
            return FakeSpreadsheets()

    class FakeClient:
        def __init__(self, path):
            self.svc = FakeSvc()
        def execute(self, req):
            req.execute()

    monkeypatch.setattr(es, "SheetsClient", FakeClient)

    es.replace_values(
        "sid",
        "Sheet1",
        [["2024-07-02 20:00:00"]],
        "creds.json",
        start_cell="B2",
    )

    assert updates["update"]["valueInputOption"] == "USER_ENTERED"
    assert updates["update"]["range"] == "Sheet1!B2"
    assert updates["clear"]["range"] == "Sheet1!B2:Z"
