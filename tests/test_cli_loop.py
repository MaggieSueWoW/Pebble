from types import SimpleNamespace
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pebble.cli as cli
from pebble.config_loader import clear_settings_cache, get_cached_settings, load_settings_entry
from tests.test_config_loader import (
    StubSheetsClient,
    _default_settings_values,
    _write_config,
)


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


def test_read_ingest_trigger_checkbox_prefetched_skips_request():
    settings = _settings_with_trigger()
    client = FakeSheetsClient([["FALSE"]])

    assert (
        cli._read_ingest_trigger_checkbox(
            settings, client=client, prefetched_values=[["TRUE"]]
        )
        is True
    )
    assert client.values_api.last_get_args is None


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

    should_run = cli._wait_for_ingest_trigger(
        settings,
        log,
        timeout=5,
        iteration=1,
        client=fake_client,
    )

    assert should_run is True
    assert clock.sleeps == []
    assert clock.monotonic() == pytest.approx(100.0)


def test_wait_for_ingest_trigger_times_out(monkeypatch):
    settings = _settings_with_trigger()
    log = SimpleNamespace(info=lambda *a, **k: None)
    fake_client = object()
    clock = FakeClock()

    monkeypatch.setattr(cli, "_read_ingest_trigger_checkbox", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(cli, "time", clock)

    should_run = cli._wait_for_ingest_trigger(
        settings,
        log,
        timeout=3,
        iteration=2,
        client=fake_client,
    )

    assert should_run is False
    assert clock.sleeps == [3.0]
    assert clock.monotonic() == pytest.approx(103.0)


def test_wait_for_ingest_trigger_zero_timeout(monkeypatch):
    settings = _settings_with_trigger()
    log = SimpleNamespace(info=lambda *a, **k: None)
    fake_client = object()
    clock = FakeClock()

    monkeypatch.setattr(cli, "_read_ingest_trigger_checkbox", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(cli, "time", clock)

    should_run = cli._wait_for_ingest_trigger(
        settings,
        log,
        timeout=0,
        iteration=3,
        client=fake_client,
    )

    assert should_run is False
    assert clock.sleeps == [0.0]
    assert clock.monotonic() == pytest.approx(100.0)


def test_load_settings_and_pipeline_values_batches_trigger(tmp_path, monkeypatch):
    clear_settings_cache()
    config_path = _write_config(tmp_path)

    settings_values = _default_settings_values()
    ordered_ranges = list(settings_values.keys())
    settings_response = {
        "valueRanges": [
            {"range": rng, "values": [[settings_values[rng]]]} for rng in ordered_ranges
        ]
    }

    # Seed the cache with settings read from the Settings tab
    seed_client = StubSheetsClient(settings_response)
    load_settings_entry(config_path=config_path, sheets_client=seed_client)
    cached = get_cached_settings(config_path)

    data_values = {
        "reports": [["reports"]],
        "team_roster": [["roster"]],
        "roster_map": [["map"]],
        "availability_overrides": [["overrides"]],
        "attendance_header": [["attendance"]],
        "ingest_trigger": [["TRUE"]],
    }

    pipeline_requests = cli._pipeline_sheet_requests(cached.settings)
    pipeline_ranges = [f"{tab}!{start}:Z" for _, tab, start in pipeline_requests]

    combined_response = {
        "valueRanges": settings_response["valueRanges"]
        + [
            {"range": rng, "values": data_values[key]}
            for (key, _, _), rng in zip(pipeline_requests, pipeline_ranges)
        ]
    }

    class RecordingSheetsClient:
        def __init__(self, response):
            self._response = response
            self.recorded_ranges: list[list[str]] = []
            self.svc = self

        def spreadsheets(self):
            return self

        def values(self):
            return self

        def batchGet(self, spreadsheetId: str, ranges):
            self.recorded_ranges.append(ranges)
            return self

        def execute(self, req=None):
            return self._response

    recording_client = RecordingSheetsClient(combined_response)
    monkeypatch.setattr(cli, "SheetsClient", lambda *_args, **_kwargs: recording_client)

    try:
        settings, sheet_client, sheet_values = cli._load_settings_and_pipeline_values(config_path)

        assert sheet_client is recording_client
        assert recording_client.recorded_ranges == [cached.ranges + pipeline_ranges]
        assert sheet_values["ingest_trigger"] == data_values["ingest_trigger"]
    finally:
        clear_settings_cache()


def test_require_ingest_trigger_range_missing():
    sheets = SimpleNamespace(spreadsheet_id="sheet-id", triggers=SimpleNamespace(ingest_compute_week=""))
    settings = SimpleNamespace(service_account_json="creds.json", sheets=sheets)

    with pytest.raises(cli.click.ClickException):
        cli._require_ingest_trigger_range(settings)
