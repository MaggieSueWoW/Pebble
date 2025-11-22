from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pebble.config_loader import (
    clear_settings_cache,
    get_cached_settings,
    load_settings,
    load_settings_entry,
)


class _StubRequest:
    def __init__(self, response: dict):
        self._response = response
        self._rest_path = "batchGet"

    def execute(self):
        return self._response


class _StubValues:
    def __init__(self, response: dict, recorder):
        self._response = response
        self._recorder = recorder

    def batchGet(self, spreadsheetId: str, ranges: list[str]):
        self._recorder(ranges)
        return _StubRequest(self._response)


class _StubSpreadsheets:
    def __init__(self, response: dict, recorder):
        self._values = _StubValues(response, recorder)

    def values(self):
        return self._values


class _StubSvc:
    def __init__(self, response: dict, recorder):
        self._spreadsheets = _StubSpreadsheets(response, recorder)

    def spreadsheets(self):
        return self._spreadsheets


class StubSheetsClient:
    def __init__(self, response: dict):
        self.recorded_ranges: list[list[str]] = []
        self._svc = _StubSvc(response, self._record_ranges)

    def _record_ranges(self, ranges: list[str]):
        self.recorded_ranges.append(ranges)

    @property
    def svc(self):
        return self._svc

    def execute(self, req):
        return req.execute()


def _write_config(tmp_path) -> str:
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        textwrap.dedent(
            """
            sheets:
              spreadsheet_id: "spreadsheet"
              tabs:
                reports: "Settings!B2"
                roster_map: "Settings!B3"
                team_roster: "Settings!B4"
                availability_overrides: "Settings!B5"
                night_qa: "Settings!B6"
                bench_night_totals: "Settings!B7"
                bench_week_totals: "Settings!B8"
                bench_rankings: "Settings!B9"
                attendance: "Settings!B10"
              starts:
                reports: "Settings!C2"
                roster_map: "Settings!C3"
                team_roster: "Settings!C4"
                availability_overrides: "Settings!C5"
                night_qa: "Settings!C6"
                bench_night_totals: "Settings!C7"
                bench_week_totals: "Settings!C8"
                bench_rankings: "Settings!C9"
                attendance: "Settings!C10"
                attendance_probability: "Settings!C11"
              last_processed: "Settings!B12"
              triggers:
                ingest_compute_week: "Settings!B13"

            time:
              tz: "Settings!B15"
              break_window:
                start_pt: "Settings!B16"
                end_pt: "Settings!B17"
                min_gap_minutes: "Settings!B18"
                max_gap_minutes: "Settings!B19"
              mythic_post_extension_min: "Settings!B20"

            wcl:
              client_id: "client-id"
              client_secret: "client-secret"

            redis:
              url: "redis://localhost:6379/0"
              key_prefix: "pebble:wcl:"

            mongo:
              uri: "mongodb://localhost:27017"

            service_account_json: "./service-account.json"
            """
        )
    )
    return str(cfg)


def _default_settings_values():
    return {
        "Settings!B2": "Reports",
        "Settings!B3": "Roster Map",
        "Settings!B4": "Team Roster",
        "Settings!B5": "Availability Overrides",
        "Settings!B6": "Night QA",
        "Settings!B7": "Bench Night Totals",
        "Settings!B8": "Bench Week Totals",
        "Settings!B9": "Bench Rankings",
        "Settings!B10": "Attendance",
        "Settings!C2": "B7",
        "Settings!C3": "B6",
        "Settings!C4": "B5",
        "Settings!C5": "B9",
        "Settings!C6": "B5",
        "Settings!C7": "B5",
        "Settings!C8": "B5",
        "Settings!C9": "B5",
        "Settings!C10": "B5",
        "Settings!C11": "N5",
        "Settings!B12": "Bench Rankings!C3",
        "Settings!B13": "Reports!B2",
        "Settings!B15": "America/Los_Angeles",
        "Settings!B16": "20:40",
        "Settings!B17": "21:40",
        "Settings!B18": "8",
        "Settings!B19": "45",
        "Settings!B20": "5",
    }


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    clear_settings_cache()
    yield
    clear_settings_cache()


def test_load_settings_dereferences_sheet_values(tmp_path):
    config_path = _write_config(tmp_path)

    settings_values = _default_settings_values()

    ordered_ranges = list(settings_values.keys())
    response = {
        "valueRanges": [
            {"range": rng, "values": [[settings_values[rng]]]} for rng in ordered_ranges
        ]
    }

    stub_client = StubSheetsClient(response)

    settings = load_settings(config_path=config_path, sheets_client=stub_client)

    assert stub_client.recorded_ranges[0] == ordered_ranges
    assert settings.sheets.tabs.night_qa == "Night QA"
    assert settings.sheets.starts.availability_overrides == "B9"
    assert settings.sheets.last_processed == "Bench Rankings!C3"
    assert settings.time.break_window.min_gap_minutes == 8
    assert settings.time.mythic_post_extension_min == 5.0


def test_load_settings_missing_value_raises(tmp_path):
    config_path = _write_config(tmp_path)

    response = {
        "valueRanges": [
            {"range": "Settings!B2", "values": [[]]},
        ]
        + [{"range": rng, "values": [["ok"]]} for rng in ["Settings!B3"] * 26]
    }

    stub_client = StubSheetsClient(response)

    with pytest.raises(ValueError, match="Missing value for settings cell Settings!B2"):
        load_settings(config_path=config_path, sheets_client=stub_client)


def test_load_settings_populates_cache(tmp_path):
    config_path = _write_config(tmp_path)

    settings_values = _default_settings_values()
    ordered_ranges = list(settings_values.keys())
    response = {
        "valueRanges": [
            {"range": rng, "values": [[settings_values[rng]]]} for rng in ordered_ranges
        ]
    }

    stub_client = StubSheetsClient(response)
    settings = load_settings(config_path=config_path, sheets_client=stub_client)

    cached = get_cached_settings(config_path)
    assert cached is not None
    assert cached.settings == settings
    assert cached.ranges[0] == "Settings!B2"
    assert cached.values[0] == settings_values[ordered_ranges[0]]


def test_load_settings_entry_accepts_prefetched_values(tmp_path):
    config_path = _write_config(tmp_path)

    settings_values = _default_settings_values()
    ordered_ranges = list(settings_values.keys())
    response = {
        "valueRanges": [
            {"range": rng, "values": [[settings_values[rng]]]} for rng in ordered_ranges
        ]
    }

    stub_client = StubSheetsClient(response)
    entry = load_settings_entry(
        config_path=config_path,
        sheets_client=stub_client,
        settings_value_ranges=response["valueRanges"],
    )

    assert stub_client.recorded_ranges == []
    assert entry.values[0] == settings_values[ordered_ranges[0]]
