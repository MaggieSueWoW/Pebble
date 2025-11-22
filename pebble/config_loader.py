from __future__ import annotations
from dataclasses import dataclass
from copy import deepcopy
from pathlib import Path
import logging
import os

from dotenv import load_dotenv
from pydantic import BaseModel, Field
import yaml

from .sheets_client import SheetsClient
from .utils.sheets import parse_tab_cell

logger = logging.getLogger(__name__)


@dataclass
class SettingsCacheEntry:
    settings: "Settings"
    ranges: list[str]
    values: list[str]


_SETTINGS_CACHE: dict[str, SettingsCacheEntry] = {}


class SheetsTabs(BaseModel):
    reports: str = Field(default="Reports")
    roster_map: str = Field(default="Roster Map")
    team_roster: str = Field(default="Team Roster")
    availability_overrides: str = Field(default="Availability Overrides")
    night_qa: str = Field(default="Night QA")
    bench_night_totals: str = Field(default="Bench Night Totals")
    bench_week_totals: str = Field(default="Bench Week Totals")
    bench_rankings: str = Field(default="Bench Rankings")
    attendance: str = Field(default="Attendance")


class SheetsStarts(BaseModel):
    reports: str = Field(default="A5")
    roster_map: str = Field(default="A5")
    team_roster: str = Field(default="A5")
    availability_overrides: str = Field(default="A5")
    night_qa: str = Field(default="A5")
    bench_night_totals: str = Field(default="A5")
    bench_week_totals: str = Field(default="A5")
    bench_rankings: str = Field(default="A5")
    attendance: str = Field(default="A5")
    attendance_probability: str = Field(default="A5")


class SheetsTriggers(BaseModel):
    ingest_compute_week: str


class SheetsConfig(BaseModel):
    spreadsheet_id: str
    tabs: SheetsTabs = Field(default_factory=SheetsTabs)
    starts: SheetsStarts = Field(default_factory=SheetsStarts)
    last_processed: str = Field(default="Bench Rankings!C3")
    triggers: SheetsTriggers


class MongoConfig(BaseModel):
    uri: str
    db: str = Field(default="pebble")


class WCLConfig(BaseModel):
    client_id: str
    client_secret: str
    base_url: str = Field(default="https://www.warcraftlogs.com/api/v2/client")
    token_url: str = Field(default="https://www.warcraftlogs.com/oauth/token")


class RedisConfig(BaseModel):
    url: str = Field(default="redis://localhost:6379/0")
    key_prefix: str = Field(default="pebble:wcl:")


class BreakWindowConfig(BaseModel):
    start_pt: str = Field(default="20:50")
    end_pt: str = Field(default="21:30")
    min_gap_minutes: int = 10
    max_gap_minutes: int = 30


class TimeConfig(BaseModel):
    tz: str = Field(default="America/Los_Angeles")
    break_window: BreakWindowConfig = Field(default_factory=BreakWindowConfig)
    mythic_post_extension_min: float = Field(default=5.0)


class Settings(BaseModel):
    sheets: SheetsConfig
    mongo: MongoConfig
    wcl: WCLConfig
    redis: RedisConfig = Field(default_factory=RedisConfig)
    time: TimeConfig = Field(default_factory=TimeConfig)
    service_account_json: str = Field(default="service-account.json")


def _load_yaml(path: str | os.PathLike) -> dict:
    p = Path(path)
    if not p.exists():
        logger.error(f"Config file {p} does not exist")
        return {}
    with p.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _settings_cache_key(config_path: str) -> str:
    return str(Path(config_path).resolve())


def _ensure_value_range(range_name: str, value_range: dict) -> str:
    values = value_range.get("values", [])
    if not values or not values[0]:
        raise ValueError(f"Missing value for settings cell {range_name}")
    return values[0][0]


def _set_path(data: dict, path: tuple[str, ...], value):
    cursor = data
    for key in path[:-1]:
        cursor = cursor.setdefault(key, {})
    cursor[path[-1]] = value


def _collect_setting_references(data: dict) -> list[tuple[tuple[str, ...], str]]:
    try:
        sheets = data["sheets"]
        tabs = sheets["tabs"]
        starts = sheets["starts"]
        triggers = sheets["triggers"]
        time_cfg = data["time"]
        break_window = time_cfg["break_window"]
    except KeyError as exc:
        raise ValueError("Missing required settings configuration sections") from exc

    return [
        (("sheets", "tabs", "reports"), tabs["reports"]),
        (("sheets", "tabs", "roster_map"), tabs["roster_map"]),
        (("sheets", "tabs", "team_roster"), tabs["team_roster"]),
        (
            ("sheets", "tabs", "availability_overrides"),
            tabs["availability_overrides"],
        ),
        (("sheets", "tabs", "night_qa"), tabs["night_qa"]),
        (
            ("sheets", "tabs", "bench_night_totals"),
            tabs["bench_night_totals"],
        ),
        (
            ("sheets", "tabs", "bench_week_totals"),
            tabs["bench_week_totals"],
        ),
        (("sheets", "tabs", "bench_rankings"), tabs["bench_rankings"]),
        (("sheets", "tabs", "attendance"), tabs["attendance"]),
        (("sheets", "starts", "reports"), starts["reports"]),
        (("sheets", "starts", "roster_map"), starts["roster_map"]),
        (("sheets", "starts", "team_roster"), starts["team_roster"]),
        (
            ("sheets", "starts", "availability_overrides"),
            starts["availability_overrides"],
        ),
        (("sheets", "starts", "night_qa"), starts["night_qa"]),
        (
            ("sheets", "starts", "bench_night_totals"),
            starts["bench_night_totals"],
        ),
        (
            ("sheets", "starts", "bench_week_totals"),
            starts["bench_week_totals"],
        ),
        (
            ("sheets", "starts", "bench_rankings"),
            starts["bench_rankings"],
        ),
        (("sheets", "starts", "attendance"), starts["attendance"]),
        (
            ("sheets", "starts", "attendance_probability"),
            starts["attendance_probability"],
        ),
        (("sheets", "last_processed"), sheets["last_processed"]),
        (
            ("sheets", "triggers", "ingest_compute_week"),
            triggers["ingest_compute_week"],
        ),
        (("time", "tz"), time_cfg["tz"]),
        (("time", "break_window", "start_pt"), break_window["start_pt"]),
        (("time", "break_window", "end_pt"), break_window["end_pt"]),
        (
            ("time", "break_window", "min_gap_minutes"),
            break_window["min_gap_minutes"],
        ),
        (
            ("time", "break_window", "max_gap_minutes"),
            break_window["max_gap_minutes"],
        ),
        (
            ("time", "mythic_post_extension_min"),
            time_cfg["mythic_post_extension_min"],
        ),
    ]


def _references_to_ranges(
    references: list[tuple[tuple[str, ...], str]]
) -> list[str]:
    ranges: list[str] = []
    for _, ref in references:
        tab, cell = parse_tab_cell(ref)
        if tab is None:
            raise ValueError(f"Settings reference '{ref}' must include a sheet tab name")
        ranges.append(f"{tab}!{cell}")
    return ranges


def _dereference_sheet_settings(
    merged: dict,
    *,
    sheets_client: SheetsClient | None = None,
    settings_value_ranges: list[dict] | None = None,
) -> tuple[dict, list[str], list[str]]:
    try:
        service_account_json = merged["service_account_json"]
        spreadsheet_id = merged["sheets"]["spreadsheet_id"]
    except KeyError as exc:
        raise ValueError("Sheets configuration is missing spreadsheet details") from exc

    references = _collect_setting_references(merged)
    ranges = _references_to_ranges(references)

    if settings_value_ranges is None:
        client = sheets_client or SheetsClient(service_account_json)
        response = client.execute(
            client.svc.spreadsheets()
            .values()
            .batchGet(spreadsheetId=spreadsheet_id, ranges=ranges)
        )
        value_ranges = response.get("valueRanges", [])
    else:
        value_ranges = settings_value_ranges

    if len(value_ranges) != len(ranges):
        raise ValueError("Unexpected response when reading settings from Google Sheets")

    resolved = deepcopy(merged)
    values: list[str] = []
    for (path, ref), value_range in zip(references, value_ranges):
        value = _ensure_value_range(ref, value_range)
        _set_path(resolved, path, value)
        values.append(value)

    return resolved, ranges, values


def load_settings_entry(
    config_path: str = "config.yaml",
    *,
    sheets_client: SheetsClient | None = None,
    settings_value_ranges: list[dict] | None = None,
    update_cache: bool = True,
) -> SettingsCacheEntry:
    load_dotenv(override=False)

    data = _load_yaml(config_path)

    # Allow env overrides for secrets
    env_overrides = {
        "sheets": {
            "spreadsheet_id": os.getenv(
                "SHEETS_SPREADSHEET_ID", data.get("sheets", {}).get("spreadsheet_id")
            ),
        },
        "mongo": {
            "uri": os.getenv("MONGODB_URI", data.get("mongo", {}).get("uri")),
            "db": data.get("mongo", {}).get("db", "pebble"),
        },
        "wcl": {
            "client_id": os.getenv(
                "WCL_CLIENT_ID", data.get("wcl", {}).get("client_id")
            ),
            "client_secret": os.getenv(
                "WCL_CLIENT_SECRET", data.get("wcl", {}).get("client_secret")
            ),
        },
        "redis": {
            "url": os.getenv(
                "REDIS_URL",
                data.get("redis", {}).get("url", "redis://localhost:6379/0"),
            ),
            "key_prefix": os.getenv(
                "REDIS_KEY_PREFIX",
                data.get("redis", {}).get("key_prefix", "pebble:wcl:"),
            ),
        },
        "service_account_json": os.getenv(
            "GOOGLE_APPLICATION_CREDENTIALS",
            data.get("service_account_json", "service-account.json"),
        ),
    }

    # Merge shallowly
    merged = {
        **data,
        "sheets": {**data.get("sheets", {}), **env_overrides["sheets"]},
        "mongo": {**data.get("mongo", {}), **env_overrides["mongo"]},
        "wcl": {**data.get("wcl", {}), **env_overrides["wcl"]},
        "redis": {**data.get("redis", {}), **env_overrides["redis"]},
        "service_account_json": env_overrides["service_account_json"],
    }

    resolved, ranges, values = _dereference_sheet_settings(
        merged,
        sheets_client=sheets_client,
        settings_value_ranges=settings_value_ranges,
    )

    entry = SettingsCacheEntry(settings=Settings(**resolved), ranges=ranges, values=values)
    if update_cache:
        _SETTINGS_CACHE[_settings_cache_key(config_path)] = entry
    return entry


def load_settings(
    config_path: str = "config.yaml",
    *,
    sheets_client: SheetsClient | None = None,
    settings_value_ranges: list[dict] | None = None,
) -> Settings:
    entry = load_settings_entry(
        config_path,
        sheets_client=sheets_client,
        settings_value_ranges=settings_value_ranges,
    )
    return entry.settings


def get_cached_settings(config_path: str = "config.yaml") -> SettingsCacheEntry | None:
    return _SETTINGS_CACHE.get(_settings_cache_key(config_path))


def clear_settings_cache():
    _SETTINGS_CACHE.clear()
