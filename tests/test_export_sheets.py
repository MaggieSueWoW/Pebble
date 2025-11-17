import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pebble import export_sheets


class _FakeRequest:
    def __init__(self, callback):
        self._callback = callback
        self.uri = "fake"

    def execute(self):
        return self._callback()


class _FakeValues:
    def __init__(self, header, recorded):
        self._header = header
        self._recorded = recorded

    def clear(self, *, spreadsheetId, range):  # noqa: N803 (Google API signature)
        def _run():
            self._recorded.setdefault("clears", []).append(range)
            return {}

        return _FakeRequest(_run)

    def update(self, *, spreadsheetId, range, valueInputOption, body):  # noqa: N803
        def _run():
            self._recorded.setdefault("updates", []).append((range, body))
            return {}

        return _FakeRequest(_run)

    def get(self, *, spreadsheetId, range):  # noqa: N803
        def _run():
            return {"values": [self._header]}

        return _FakeRequest(_run)


class _FakeSpreadsheets:
    def __init__(self, header, recorded):
        self._header = header
        self._recorded = recorded

    def values(self):
        return _FakeValues(self._header, self._recorded)

    def batchUpdate(self, *, spreadsheetId, body):  # noqa: N803
        def _run():
            self._recorded.setdefault("batch", []).append(body["requests"])
            return {}

        return _FakeRequest(_run)

    def get(self, *, spreadsheetId, fields=None):  # noqa: N803
        def _run():
            return {"sheets": [{"properties": {"title": "Attendance", "sheetId": 314}}]}

        return _FakeRequest(_run)


class _FakeClient:
    def __init__(self, header, recorded):
        self.svc = type("_FakeSvc", (), {"spreadsheets": lambda _self: _FakeSpreadsheets(header, recorded)})()
        self._recorded = recorded

    def execute(self, request):
        return request.execute()


def test_replace_values_inserts_columns_before_trailing_cells(monkeypatch):
    header_prefix = [
        "Player",
        "Attendance",
        "Played",
        "Bench",
        "Possible",
    ]
    existing_header = header_prefix + ["2024-07-09", "Legend"]
    values = [header_prefix + ["2024-07-09", "2024-07-16", "Legend"]]
    recorded = {}

    client = _FakeClient(existing_header, recorded)

    export_sheets.replace_values(
        "sheet",
        "Attendance",
        values,
        client=client,
        ensure_tail_space=True,
        existing_header_row=existing_header,
    )

    requests = recorded.get("batch")
    assert requests is not None
    insert_requests = [
        req["insertDimension"]
        for batch in requests
        for req in batch
        if "insertDimension" in req
    ]
    assert insert_requests, "expected insert dimension requests"
    insert = insert_requests[0]
    assert insert["range"]["startIndex"] == 6
    assert insert["range"]["endIndex"] == 7
    assert insert["inheritFromBefore"] is True


def test_replace_values_does_not_insert_when_space_exists(monkeypatch):
    header_prefix = [
        "Player",
        "Attendance",
        "Played",
        "Bench",
        "Possible",
    ]
    existing_header = header_prefix + ["2024-07-09", "2024-07-16", "Legend"]
    values = [header_prefix + ["2024-07-09", "2024-07-16", "Legend"]]
    recorded = {}

    client = _FakeClient(existing_header, recorded)

    export_sheets.replace_values(
        "sheet",
        "Attendance",
        values,
        client=client,
        ensure_tail_space=True,
        existing_header_row=existing_header,
    )

    requests = recorded.get("batch", [])
    insert_requests = [
        req
        for batch in requests
        for req in batch
        if "insertDimension" in req
    ]
    assert insert_requests == []


def test_replace_values_skips_timestamp_when_cell_missing(monkeypatch):
    recorded = {}

    client = _FakeClient(["Player"], recorded)
    called = False

    def _fake_update_last_processed(*args, **kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(export_sheets, "update_last_processed", _fake_update_last_processed)

    export_sheets.replace_values(
        "sheet",
        "Attendance",
        [["Player"]],
        client=client,
    )

    assert called is False


def test_replace_values_updates_timestamp_when_cell_provided(monkeypatch):
    recorded = {}

    client = _FakeClient(["Player"], recorded)
    captured = {}

    def _fake_update_last_processed(spreadsheet_id, tab, cell, *, client):
        captured["args"] = (spreadsheet_id, tab, cell, client)

    monkeypatch.setattr(export_sheets, "update_last_processed", _fake_update_last_processed)

    export_sheets.replace_values(
        "sheet",
        "Attendance",
        [["Player"]],
        client=client,
        last_processed_cell="B9",
    )

    assert captured["args"][2] == "B9"
    assert captured["args"][3] is not None
