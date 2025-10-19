from types import SimpleNamespace

import pytest

import pebble.cli as cli


class FakeRequest:
    def __init__(self, response):
        self._response = response
        self.uri = "fake://request"

    def execute(self):
        return self._response


class FakeValuesApi:
    def __init__(self, get_response=None):
        self.get_response = get_response or []
        self.last_get_args = None
        self.last_update_kwargs = None

    def get(self, spreadsheetId, range):
        self.last_get_args = {"spreadsheetId": spreadsheetId, "range": range}
        return FakeRequest({"values": self.get_response})

    def update(self, spreadsheetId, range, valueInputOption, body):
        self.last_update_kwargs = {
            "spreadsheetId": spreadsheetId,
            "range": range,
            "valueInputOption": valueInputOption,
            "body": body,
        }
        return FakeRequest({})


class FakeSpreadsheetsApi:
    def __init__(self, values_api):
        self._values_api = values_api

    def values(self):
        return self._values_api


class FakeSheetsService:
    def __init__(self, values_api):
        self._spreadsheets = FakeSpreadsheetsApi(values_api)

    def spreadsheets(self):
        return self._spreadsheets


class FakeSheetsClient:
    def __init__(self, get_response=None):
        self.values_api = FakeValuesApi(get_response)
        self.svc = FakeSheetsService(self.values_api)

    def execute(self, req):
        return req.execute()


class FakeClock:
    def __init__(self, start=100.0):
        self._now = float(start)
        self.sleeps = []

    def monotonic(self):
        return self._now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self._now += seconds


def _settings_with_trigger():
    trigger = "Reports!B2"
    sheets = SimpleNamespace(
        spreadsheet_id="sheet-id",
        triggers=SimpleNamespace(ingest_compute_week=trigger),
    )
    return SimpleNamespace(service_account_json="creds.json", sheets=sheets)


def test_read_ingest_trigger_checkbox_true():
    settings = _settings_with_trigger()
    client = FakeSheetsClient([["TRUE"]])

    assert cli._read_ingest_trigger_checkbox(settings, client=client) is True
    assert client.values_api.last_get_args == {
        "spreadsheetId": "sheet-id",
        "range": "Reports!B2",
    }


def test_read_ingest_trigger_checkbox_false_for_empty():
    settings = _settings_with_trigger()
    client = FakeSheetsClient([])

    assert cli._read_ingest_trigger_checkbox(settings, client=client) is False


def test_set_ingest_trigger_checkbox_updates_cell():
    settings = _settings_with_trigger()
    client = FakeSheetsClient([])

    cli._set_ingest_trigger_checkbox(settings, False, client=client)

    assert client.values_api.last_update_kwargs == {
        "spreadsheetId": "sheet-id",
        "range": "Reports!B2",
        "valueInputOption": "USER_ENTERED",
        "body": {"values": [["FALSE"]], "majorDimension": "ROWS"},
    }


def test_wait_for_ingest_trigger_returns_when_checkbox_set(monkeypatch):
    settings = _settings_with_trigger()
    log = SimpleNamespace(info=lambda *a, **k: None)
    fake_client = object()
    clock = FakeClock()

    monkeypatch.setattr(cli, "_read_ingest_trigger_checkbox", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(cli, "time", clock)

    should_run, client = cli._wait_for_ingest_trigger(
        settings,
        log,
        timeout=5,
        iteration=1,
        client=fake_client,
    )

    assert should_run is True
    assert client is fake_client
    assert clock.sleeps == []
    assert clock.monotonic() == pytest.approx(100.0)


def test_wait_for_ingest_trigger_times_out(monkeypatch):
    settings = _settings_with_trigger()
    log = SimpleNamespace(info=lambda *a, **k: None)
    fake_client = object()
    clock = FakeClock()

    monkeypatch.setattr(cli, "_read_ingest_trigger_checkbox", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(cli, "time", clock)

    should_run, client = cli._wait_for_ingest_trigger(
        settings,
        log,
        timeout=3,
        iteration=2,
        client=fake_client,
    )

    assert should_run is False
    assert client is None
    assert clock.sleeps == [3.0]
    assert clock.monotonic() == pytest.approx(103.0)


def test_wait_for_ingest_trigger_zero_timeout(monkeypatch):
    settings = _settings_with_trigger()
    log = SimpleNamespace(info=lambda *a, **k: None)
    fake_client = object()
    clock = FakeClock()

    monkeypatch.setattr(cli, "_read_ingest_trigger_checkbox", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(cli, "time", clock)

    should_run, client = cli._wait_for_ingest_trigger(
        settings,
        log,
        timeout=0,
        iteration=3,
        client=fake_client,
    )

    assert should_run is False
    assert client is None
    assert clock.sleeps == [0.0]
    assert clock.monotonic() == pytest.approx(100.0)


def test_require_ingest_trigger_range_missing():
    sheets = SimpleNamespace(spreadsheet_id="sheet-id", triggers=SimpleNamespace(ingest_compute_week=""))
    settings = SimpleNamespace(service_account_json="creds.json", sheets=sheets)

    with pytest.raises(cli.click.ClickException):
        cli._require_ingest_trigger_range(settings)
