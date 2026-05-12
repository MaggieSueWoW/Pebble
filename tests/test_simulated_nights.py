from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from random import Random
from types import SimpleNamespace
import sys

import mongomock
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pebble.cli as cli
from pebble.utils.time import PT


def _base_settings():
    tabs = SimpleNamespace(
        team_roster="Team Roster",
        reports="Reports",
        roster_map="Roster Map",
        availability_overrides="Availability Overrides",
        night_qa="Night QA",
        bench_night_totals="Bench Night Totals",
        bench_week_totals="Bench Week Totals",
        bench_rankings="Bench Rankings",
        attendance="Attendance",
    )
    starts = SimpleNamespace(
        team_roster="A5",
        reports="A5",
        roster_map="A2",
        availability_overrides="A2",
        night_qa="A1",
        bench_night_totals="A1",
        bench_week_totals="A1",
        bench_rankings="A1",
        attendance="A1",
    )
    triggers = SimpleNamespace(ingest_compute_week="Triggers!A1")
    last_processed = "Bench Rankings!B1"
    sheets = SimpleNamespace(
        spreadsheet_id="sheet",
        tabs=tabs,
        starts=starts,
        triggers=triggers,
        last_processed=last_processed,
    )
    time = SimpleNamespace(
        break_window=SimpleNamespace(
            start_pt="19:30",
            end_pt="21:30",
            min_gap_minutes=10,
            max_gap_minutes=30,
        ),
        mythic_post_extension_min=0,
        mythic_default_start_pt="",
    )
    return SimpleNamespace(service_account_json="creds.json", sheets=sheets, time=time)


def _fake_log():
    return SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None)


def _sheet_map(settings, roster=None, overrides=None, attendance=None):
    return {
        settings.sheets.tabs.roster_map: roster or [],
        settings.sheets.tabs.availability_overrides: overrides or [],
        settings.sheets.tabs.attendance: attendance
        or [["Player", "Attendance", "Played", "Bench", "Possible"]],
    }


def _setup_pipeline(monkeypatch, db, settings, sheet_values_map):
    monkeypatch.setattr("pebble.cli.get_db", lambda s: db)

    def fake_ingest_reports(_settings, *, rows=None, client=None, force_full_reingest=False):
        return {"reports": 0, "fights": 0}

    def fake_ingest_roster(_settings, *, rows=None, client=None):
        return db["team_roster"].count_documents({})

    def fake_batch(_settings, requests, client=None):
        values = {}
        for key, tab, *_ in requests:
            values[key] = sheet_values_map.get(tab, [])
        return values

    class DummySheetsClient:
        def __init__(self, *_args, **_kwargs):
            self.svc = None

        def execute(self, req):
            return req

    monkeypatch.setattr("pebble.cli.ingest_reports", fake_ingest_reports)
    monkeypatch.setattr("pebble.cli.ingest_roster", fake_ingest_roster)
    monkeypatch.setattr("pebble.cli._sheet_values_batch", fake_batch)
    monkeypatch.setattr("pebble.cli.SheetsClient", DummySheetsClient)


def _players(count: int) -> list[str]:
    return [f"Player{idx:02d}" for idx in range(1, count + 1)]


def _participants(players: list[str], indexes: list[int]) -> list[dict]:
    return [{"name": players[idx]} for idx in indexes]


def _main(name: str) -> str:
    return name.split("-", 1)[0]


def _build_night(
    *,
    night_id: str,
    start_dt: datetime,
    roster_count: int,
    segments: list[dict],
    report_overrides: dict[str, dict] | None = None,
    roster_map_rows: list[list[str]] | None = None,
    availability_overrides_rows: list[list[str]] | None = None,
):
    players = _players(roster_count)
    roster_docs = [{"main": name, "join_night": night_id, "active": True} for name in players]

    fights_all = []
    report_windows: dict[str, dict[str, int]] = {}
    current_dt = start_dt
    for idx, segment in enumerate(segments, start=1):
        current_dt += timedelta(minutes=segment.get("gap_before_min", 0))
        start_ms = int(current_dt.timestamp() * 1000)
        current_dt += timedelta(minutes=segment["duration_min"], seconds=segment.get("duration_sec", 0))
        end_ms = int(current_dt.timestamp() * 1000)
        report_code = segment.get("report_code", "R1")
        window = report_windows.setdefault(report_code, {"start_ms": start_ms, "end_ms": end_ms})
        window["start_ms"] = min(window["start_ms"], start_ms)
        window["end_ms"] = max(window["end_ms"], end_ms)
        fights_all.append(
            {
                "night_id": night_id,
                "report_code": report_code,
                "fight_abs_start_ms": start_ms,
                "fight_abs_end_ms": end_ms,
                "participants": _participants(players, segment.get("participants", [])),
                "encounter_id": segment["encounter_id"],
                "is_mythic": segment.get("difficulty") == "mythic",
                "difficulty": {"normal": 3, "heroic": 4, "mythic": 5}.get(segment.get("difficulty"), 0),
                "kill": segment.get("kill", False),
                "id": idx,
            }
        )

    reports = []
    report_overrides = report_overrides or {}
    for code, window in sorted(report_windows.items()):
        report_doc = {
            "night_id": night_id,
            "code": code,
            "start_ms": window["start_ms"] - 5 * 60000,
            "end_ms": window["end_ms"] + 5 * 60000,
        }
        report_doc.update(report_overrides.get(code, {}))
        reports.append(report_doc)

    return {
        "night_id": night_id,
        "players": players,
        "roster_docs": roster_docs,
        "fights_all": fights_all,
        "reports": reports,
        "roster_map_rows": roster_map_rows or [],
        "availability_overrides_rows": availability_overrides_rows or [],
    }


def _run_scenario(monkeypatch, scenario: dict, *, settings=None):
    db = mongomock.MongoClient().db
    settings = settings or _base_settings()
    db["team_roster"].insert_many(scenario["roster_docs"])
    db["reports"].insert_many(scenario["reports"])
    if scenario["fights_all"]:
        db["fights_all"].insert_many(scenario["fights_all"])

    captured = {}

    def fake_build_requests(spreadsheet_id, tab, values, *, client=None, **kwargs):
        captured[tab] = values
        return []

    monkeypatch.setattr("pebble.cli.build_replace_values_requests", fake_build_requests)
    _setup_pipeline(
        monkeypatch,
        db,
        settings,
        _sheet_map(
            settings,
            roster=scenario["roster_map_rows"],
            overrides=scenario["availability_overrides_rows"],
        ),
    )

    cli.run_pipeline(settings, _fake_log())
    return db, captured, settings


def _bench_by_main(db, night_id: str) -> dict[str, dict]:
    docs = list(db["bench_night_totals"].find({"night_id": night_id}, {"_id": 0}))
    return {doc["main"]: doc for doc in docs}


def _simulate_expected_blocks(
    fights_all: list[dict],
    break_start_ms: int | None,
    roster_map: dict[str, str] | None = None,
) -> dict[str, list[tuple[int, int, str]]]:
    mythic = [fight for fight in sorted(fights_all, key=lambda fight: fight["fight_abs_start_ms"]) if fight.get("is_mythic")]
    roster_map = roster_map or {}
    nm_bosses = [
        (fight["fight_abs_start_ms"], fight["fight_abs_end_ms"])
        for fight in fights_all
        if not fight.get("is_mythic") and int(fight.get("encounter_id", 0)) > 0
    ]
    output: dict[str, list[tuple[int, int, str]]] = {}
    for fight in mythic:
        half = "pre" if break_start_ms is None or ((fight["fight_abs_start_ms"] + fight["fight_abs_end_ms"]) // 2) < break_start_ms else "post"
        for participant in fight.get("participants", []):
            name = roster_map.get(participant["name"], participant["name"])
            blocks = output.setdefault(name, [])
            if not blocks:
                blocks.append((fight["fight_abs_start_ms"], fight["fight_abs_end_ms"], half))
                continue
            start_ms, end_ms, current_half = blocks[-1]
            split = any(end_ms <= nm_start and nm_end <= fight["fight_abs_start_ms"] for nm_start, nm_end in nm_bosses)
            if current_half == half and not split:
                blocks[-1] = (start_ms, fight["fight_abs_end_ms"], half)
            else:
                blocks.append((fight["fight_abs_start_ms"], fight["fight_abs_end_ms"], half))
    return output


def _parse_override_value(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    lowered = text.lower()
    if lowered in {"y", "yes", "true", "t"}:
        return True
    if lowered in {"n", "no", "false", "f"}:
        return False
    try:
        minutes = int(text)
    except ValueError:
        return None
    if minutes == 0:
        return False
    return minutes


def _scenario_roster_map(scenario: dict) -> dict[str, str]:
    rows = scenario.get("roster_map_rows") or []
    if not rows:
        return {}
    header = rows[0]
    try:
        alt_idx = header.index("Alt")
        main_idx = header.index("Main")
    except ValueError:
        return {}
    mapping = {}
    for row in rows[1:]:
        alt = row[alt_idx].strip() if alt_idx < len(row) else ""
        main = row[main_idx].strip() if main_idx < len(row) else ""
        if alt and main:
            mapping[alt] = main
    return mapping


def _scenario_overrides(scenario: dict, night_id: str) -> dict[str, dict[str, object]]:
    rows = scenario.get("availability_overrides_rows") or []
    if not rows:
        return {}
    header = rows[0]
    try:
        night_idx = header.index("Night")
        main_idx = header.index("Main")
        pre_idx = header.index("Avail Pre?")
        post_idx = header.index("Avail Post?")
    except ValueError:
        return {}
    mapping = _scenario_roster_map(scenario)
    overrides = {}
    for row in rows[1:]:
        row_night = row[night_idx].strip() if night_idx < len(row) else ""
        if row_night != night_id:
            continue
        main = row[main_idx].strip() if main_idx < len(row) else ""
        if not main:
            continue
        resolved_main = mapping.get(main, main)
        overrides[resolved_main] = {
            "pre": _parse_override_value(row[pre_idx] if pre_idx < len(row) else ""),
            "post": _parse_override_value(row[post_idx] if post_idx < len(row) else ""),
        }
    return overrides


def _oracle_break_range(scenario: dict) -> tuple[int, int] | None:
    for report in scenario.get("reports", []):
        start_ms = report.get("break_override_start_ms")
        end_ms = report.get("break_override_end_ms")
        if start_ms is not None and end_ms is not None:
            return (start_ms, end_ms)
    return None


def _oracle_mythic_envelope(scenario: dict) -> tuple[int, int] | None:
    fights_m = [fight for fight in scenario["fights_all"] if fight.get("is_mythic")]
    if not fights_m:
        return None
    auto_start = min(fight["fight_abs_start_ms"] for fight in fights_m)
    auto_end = max(fight["fight_abs_end_ms"] for fight in fights_m)
    override_start = None
    override_end = None
    for report in scenario.get("reports", []):
        if override_start is None and report.get("mythic_override_start_ms") is not None:
            override_start = report["mythic_override_start_ms"]
        if override_end is None and report.get("mythic_override_end_ms") is not None:
            override_end = report["mythic_override_end_ms"]
    return (override_start if override_start is not None else auto_start, override_end if override_end is not None else auto_end)


def _oracle_fight_half(fight: dict, break_range: tuple[int, int] | None) -> str:
    if not break_range:
        return "pre"
    midpoint = (fight["fight_abs_start_ms"] + fight["fight_abs_end_ms"]) // 2
    return "pre" if midpoint < break_range[0] else "post"


def _oracle_split_minutes(
    fights_all: list[dict],
    fights_m: list[dict],
    break_range: tuple[int, int] | None,
    envelope: tuple[int, int],
) -> tuple[int, int]:
    summary_ms = {"pre": 0, "post": 0}
    for half in ("pre", "post"):
        half_fights = [fight for fight in fights_m if _oracle_fight_half(fight, break_range) == half]
        if not half_fights:
            continue
        start_ms = min(fight["fight_abs_start_ms"] for fight in half_fights)
        end_ms = max(fight["fight_abs_end_ms"] for fight in half_fights)
        if _oracle_fight_half({"fight_abs_start_ms": envelope[0], "fight_abs_end_ms": envelope[0]}, break_range) == half:
            start_ms = min(start_ms, envelope[0])
        if _oracle_fight_half({"fight_abs_start_ms": envelope[1], "fight_abs_end_ms": envelope[1]}, break_range) == half:
            end_ms = max(end_ms, envelope[1])
        duration_ms = max(0, end_ms - start_ms)
        ordered_half_fights = sorted(half_fights, key=lambda fight: fight["fight_abs_start_ms"])
        for previous, current in zip(ordered_half_fights, ordered_half_fights[1:]):
            prev_end = previous["fight_abs_end_ms"]
            next_start = current["fight_abs_start_ms"]
            if next_start <= prev_end:
                continue
            has_non_mythic_boss = any(
                not fight.get("is_mythic")
                and int(fight.get("encounter_id", 0)) > 0
                and _oracle_fight_half(fight, break_range) == half
                and fight["fight_abs_start_ms"] < next_start
                and fight["fight_abs_end_ms"] > prev_end
                for fight in fights_all
            )
            if has_non_mythic_boss:
                duration_ms -= next_start - prev_end
        summary_ms[half] = max(0, duration_ms)
    return summary_ms["pre"], summary_ms["post"]


def _oracle_last_non_mythic_mains(
    fights_all: list[dict],
    mythic_start_ms: int,
    roster_map: dict[str, str],
) -> set[str]:
    eligible = [
        fight
        for fight in fights_all
        if not fight.get("is_mythic")
        and int(fight.get("encounter_id", 0)) > 0
        and fight["fight_abs_start_ms"] < mythic_start_ms
    ]
    if not eligible:
        return set()
    fight = max(eligible, key=lambda item: item["fight_abs_start_ms"])
    return {roster_map.get(participant["name"], participant["name"]) for participant in fight.get("participants", [])}


def _oracle_expected_night(scenario: dict) -> dict | None:
    envelope = _oracle_mythic_envelope(scenario)
    if not envelope:
        return None
    break_range = _oracle_break_range(scenario)
    roster_map = _scenario_roster_map(scenario)
    overrides = _scenario_overrides(scenario, scenario["night_id"])
    fights_m = [fight for fight in scenario["fights_all"] if fight.get("is_mythic")]
    pre_ms, post_ms = _oracle_split_minutes(scenario["fights_all"], fights_m, break_range, envelope)
    blocks = _simulate_expected_blocks(scenario["fights_all"], break_range[0] if break_range else None, roster_map)
    last_non_mythic = _oracle_last_non_mythic_mains(scenario["fights_all"], envelope[0], roster_map)

    all_mains = {main for main, main_blocks in blocks.items() if main_blocks}
    all_mains.update(last_non_mythic)
    all_mains.update(overrides.keys())

    expected_bench = {}
    for main in sorted(all_mains):
        main_blocks = blocks.get(main, [])
        played_by_half_ms = {"pre": 0, "post": 0}
        for start_ms, end_ms, half in main_blocks:
            played_by_half_ms[half] += max(0, end_ms - start_ms)
        pre_played_ms = played_by_half_ms["pre"]
        post_played_ms = played_by_half_ms["post"]
        pre_played = pre_played_ms // 60000
        post_played = post_played_ms // 60000

        pre_avail = pre_played_ms > 0 or post_played_ms > 0
        post_avail = pre_played_ms > 0 or post_played_ms > 0
        pre_available_ms = pre_ms if pre_avail else pre_played_ms
        post_available_ms = post_ms if post_avail else post_played_ms

        if main in last_non_mythic:
            pre_avail = True
            post_avail = True
            pre_available_ms = pre_ms
            post_available_ms = post_ms

        override = overrides.get(main)
        if override:
            for half, full_ms, played_ms in (("pre", pre_ms, pre_played_ms), ("post", post_ms, post_played_ms)):
                value = override.get(half)
                if isinstance(value, bool):
                    if half == "pre":
                        pre_avail = value
                        pre_available_ms = full_ms if value else played_ms
                    else:
                        post_avail = value
                        post_available_ms = full_ms if value else played_ms
                elif isinstance(value, int):
                    if value > 0:
                        available_ms = min(full_ms, value * 60000)
                    else:
                        available_ms = max(0, full_ms - abs(value) * 60000)
                    available_ms = max(available_ms, played_ms)
                    if half == "pre":
                        pre_avail = available_ms > 0
                        pre_available_ms = available_ms
                    else:
                        post_avail = available_ms > 0
                        post_available_ms = available_ms

        pre_bench_min = (max(0, pre_available_ms - pre_played_ms) // 60000) if pre_avail else 0
        post_bench_min = (max(0, post_available_ms - post_played_ms) // 60000) if post_avail else 0

        expected_bench[main] = {
            "main": main,
            "played_pre_min": pre_played,
            "played_post_min": post_played,
            "played_total_min": pre_played + post_played,
            "bench_pre_min": pre_bench_min,
            "bench_post_min": post_bench_min,
            "bench_total_min": pre_bench_min + post_bench_min,
            "avail_pre": pre_avail,
            "avail_post": post_avail,
        }

    return {
        "mythic_pre_min": round(pre_ms / 60000.0, 2),
        "mythic_post_min": round(post_ms / 60000.0, 2),
        "bench": expected_bench,
    }


def _alternating_patterns(max_segments: int = 5) -> list[str]:
    patterns = []
    for length in range(1, max_segments + 1):
        for start in ("H", "M"):
            current = start
            chars = []
            for _ in range(length):
                chars.append(current)
                current = "M" if current == "H" else "H"
            patterns.append("".join(chars))
    return patterns


def _matrix_segments(pre_pattern: str, post_pattern: str) -> list[dict]:
    mythic_common = list(range(0, 10))
    mythic_flex = list(range(20, 25))
    mythic_odd = list(range(10, 15))
    mythic_even = list(range(15, 20))
    heroic_only = list(range(25, 30))
    heroic_participants = mythic_common + mythic_flex + mythic_odd + mythic_even + heroic_only

    segments = []
    encounter_id = 700
    for pattern, report_code, opening_gap in ((pre_pattern, "R1", 5), (post_pattern, "R2", 20)):
        mythic_occurrence = 0
        for index, difficulty in enumerate(pattern):
            gap_before = opening_gap if index == 0 else 4
            if difficulty == "M":
                mythic_occurrence += 1
                swap_group = mythic_odd if mythic_occurrence % 2 == 1 else mythic_even
                participants = mythic_common + mythic_flex + swap_group
                segments.append(
                    {
                        "gap_before_min": gap_before,
                        "duration_min": 8,
                        "difficulty": "mythic",
                        "encounter_id": encounter_id,
                        "participants": participants,
                        "kill": mythic_occurrence % 2 == 0,
                        "report_code": report_code,
                    }
                )
            else:
                segments.append(
                    {
                        "gap_before_min": gap_before,
                        "duration_min": 6,
                        "difficulty": "heroic",
                        "encounter_id": encounter_id,
                        "participants": heroic_participants,
                        "kill": True,
                        "report_code": report_code,
                    }
                )
            encounter_id += 1
    return segments


def test_simulated_pure_mythic_night(monkeypatch):
    start_dt = datetime(2024, 8, 6, 19, 5, tzinfo=PT)
    scenario = _build_night(
        night_id="2024-08-06",
        start_dt=start_dt,
        roster_count=30,
        segments=[
            {"gap_before_min": 10, "duration_min": 10, "difficulty": "mythic", "encounter_id": 1, "participants": list(range(20)), "kill": False},
            {"gap_before_min": 5, "duration_min": 11, "difficulty": "mythic", "encounter_id": 1, "participants": list(range(20)), "kill": True},
            {"gap_before_min": 10, "duration_min": 10, "difficulty": "mythic", "encounter_id": 2, "participants": list(range(5, 25)), "kill": False},
            {"gap_before_min": 6, "duration_min": 10, "difficulty": "mythic", "encounter_id": 2, "participants": list(range(5, 25)), "kill": True},
        ],
        report_overrides={
            "R1": {
                "break_override_start_ms": int((start_dt + timedelta(minutes=40)).timestamp() * 1000),
                "break_override_end_ms": int((start_dt + timedelta(minutes=55)).timestamp() * 1000),
            }
        },
    )

    db, captured, settings = _run_scenario(monkeypatch, scenario)
    qa_doc = db["night_qa"].find_one({"night_id": scenario["night_id"]}, {"_id": 0})
    assert qa_doc["mythic_pre_min"] == 26.0
    assert qa_doc["mythic_post_min"] == 26.0

    bench = _bench_by_main(db, scenario["night_id"])
    assert bench[_main(scenario["players"][0])]["bench_post_min"] == 26
    assert bench[_main(scenario["players"][24])]["bench_pre_min"] == 26
    assert bench[_main(scenario["players"][10])]["bench_total_min"] == 0
    assert _main(scenario["players"][25]) not in bench

    attendance_rows = captured[settings.sheets.tabs.attendance]
    attendance_by_main = {row[0]: row for row in attendance_rows[1:]}
    week_idx = attendance_rows[0].index("2024-08-06")
    assert attendance_by_main[_main(scenario["players"][0])][week_idx] == "PB"
    assert attendance_by_main[_main(scenario["players"][25])][week_idx] == "O"


def test_simulated_mixed_difficulties_with_overrides(monkeypatch):
    start_dt = datetime(2024, 8, 8, 19, 20, tzinfo=PT)
    break_start = int((start_dt + timedelta(minutes=95)).timestamp() * 1000)
    break_end = int((start_dt + timedelta(minutes=110)).timestamp() * 1000)
    mythic_override_start = int((start_dt + timedelta(minutes=28)).timestamp() * 1000)
    mythic_override_end = int((start_dt + timedelta(minutes=152)).timestamp() * 1000)
    scenario = _build_night(
        night_id="2024-08-08",
        start_dt=start_dt,
        roster_count=30,
        segments=[
            {"gap_before_min": 5, "duration_min": 10, "difficulty": "heroic", "encounter_id": 101, "participants": list(range(25)), "kill": True},
            {"gap_before_min": 5, "duration_min": 8, "difficulty": "mythic", "encounter_id": 1, "participants": list(range(20)), "kill": False},
            {"gap_before_min": 4, "duration_min": 9, "difficulty": "mythic", "encounter_id": 1, "participants": list(range(20)), "kill": True},
            {"gap_before_min": 4, "duration_min": 8, "difficulty": "heroic", "encounter_id": 102, "participants": list(range(22)), "kill": True},
            {"gap_before_min": 20, "duration_min": 3, "difficulty": "trash", "encounter_id": 0, "participants": list(range(20))},
            {"gap_before_min": 5, "duration_min": 8, "difficulty": "heroic", "encounter_id": 103, "participants": list(range(22)), "kill": True},
            {"gap_before_min": 5, "duration_min": 9, "difficulty": "mythic", "encounter_id": 2, "participants": list(range(5, 25)), "kill": False},
            {"gap_before_min": 4, "duration_min": 9, "difficulty": "mythic", "encounter_id": 2, "participants": list(range(5, 25)), "kill": True},
            {"gap_before_min": 4, "duration_min": 8, "difficulty": "heroic", "encounter_id": 104, "participants": list(range(20)), "kill": True},
        ],
        report_overrides={
            "R1": {
                "break_override_start_ms": break_start,
                "break_override_end_ms": break_end,
                "mythic_override_start_ms": mythic_override_start,
                "mythic_override_end_ms": mythic_override_end,
            }
        },
    )

    db, _captured, _settings = _run_scenario(monkeypatch, scenario)
    qa_doc = db["night_qa"].find_one({"night_id": scenario["night_id"]}, {"_id": 0})
    assert qa_doc["override_used"] is True
    assert qa_doc["mythic_start_ms"] == mythic_override_start
    assert qa_doc["mythic_end_ms"] == mythic_override_end
    assert qa_doc["mythic_pre_min"] == 21.0
    assert qa_doc["mythic_post_min"] == 58.0

    bench = _bench_by_main(db, scenario["night_id"])
    assert bench[_main(scenario["players"][2])]["bench_post_min"] == 58
    assert bench[_main(scenario["players"][22])]["bench_pre_min"] == 21


def test_simulated_no_mythic_night_produces_no_accounting(monkeypatch):
    start_dt = datetime(2024, 8, 13, 19, 0, tzinfo=PT)
    scenario = _build_night(
        night_id="2024-08-13",
        start_dt=start_dt,
        roster_count=30,
        segments=[
            {"gap_before_min": 5, "duration_min": 10, "difficulty": "normal", "encounter_id": 201, "participants": list(range(25)), "kill": True},
            {"gap_before_min": 8, "duration_min": 9, "difficulty": "heroic", "encounter_id": 202, "participants": list(range(25)), "kill": False},
            {"gap_before_min": 12, "duration_min": 8, "difficulty": "heroic", "encounter_id": 202, "participants": list(range(25)), "kill": True},
        ],
    )

    db, captured, settings = _run_scenario(monkeypatch, scenario)
    assert db["night_qa"].count_documents({"night_id": scenario["night_id"]}) == 0
    assert db["bench_night_totals"].count_documents({"night_id": scenario["night_id"]}) == 0
    attendance_rows = captured[settings.sheets.tabs.attendance]
    assert attendance_rows[0] == ["Player", "Attendance", "Played", "Bench", "Possible"]
    assert all(row[2:] == [0, 0, 0] for row in attendance_rows[1:])


def test_simulated_last_non_mythic_boss_marks_benched_players_available(monkeypatch):
    start_dt = datetime(2024, 8, 15, 19, 10, tzinfo=PT)
    scenario = _build_night(
        night_id="2024-08-15",
        start_dt=start_dt,
        roster_count=30,
        segments=[
            {"gap_before_min": 5, "duration_min": 10, "difficulty": "heroic", "encounter_id": 301, "participants": list(range(25)), "kill": True},
            {"gap_before_min": 5, "duration_min": 10, "difficulty": "mythic", "encounter_id": 1, "participants": list(range(20)), "kill": False},
            {"gap_before_min": 5, "duration_min": 10, "difficulty": "mythic", "encounter_id": 1, "participants": list(range(20)), "kill": True},
        ],
    )

    db, _captured, _settings = _run_scenario(monkeypatch, scenario)
    qa_doc = db["night_qa"].find_one({"night_id": scenario["night_id"]}, {"_id": 0})
    assert qa_doc["mythic_pre_min"] == 25.0

    bench = _bench_by_main(db, scenario["night_id"])
    assert bench[_main(scenario["players"][22])]["bench_pre_min"] == 25
    assert bench[_main(scenario["players"][22])]["status_source"] == "last_fight"
    assert bench[_main(scenario["players"][22])]["avail_pre"] is True


def test_simulated_repeated_swaps_create_disjoint_blocks(monkeypatch):
    start_dt = datetime(2024, 8, 20, 19, 15, tzinfo=PT)
    scenario = _build_night(
        night_id="2024-08-20",
        start_dt=start_dt,
        roster_count=30,
        segments=[
            {"gap_before_min": 10, "duration_min": 8, "difficulty": "mythic", "encounter_id": 1, "participants": list(range(20)), "kill": False},
            {"gap_before_min": 4, "duration_min": 9, "difficulty": "heroic", "encounter_id": 401, "participants": list(range(22)), "kill": True},
            {"gap_before_min": 4, "duration_min": 8, "difficulty": "mythic", "encounter_id": 2, "participants": list(range(10, 30)), "kill": False},
            {"gap_before_min": 4, "duration_min": 8, "difficulty": "heroic", "encounter_id": 402, "participants": list(range(22)), "kill": True},
            {"gap_before_min": 4, "duration_min": 8, "difficulty": "mythic", "encounter_id": 3, "participants": list(range(20)), "kill": True},
        ],
        report_overrides={
            "R1": {
                "break_override_start_ms": int((start_dt + timedelta(minutes=120)).timestamp() * 1000),
                "break_override_end_ms": int((start_dt + timedelta(minutes=135)).timestamp() * 1000),
            }
        },
    )

    db, _captured, _settings = _run_scenario(monkeypatch, scenario)
    blocks = list(
        db["blocks"].find(
            {"night_id": scenario["night_id"], "main": _main(scenario["players"][0])},
            {"_id": 0, "start_ms": 1, "end_ms": 1, "half": 1, "block_seq": 1},
        ).sort("block_seq", 1)
    )
    assert len(blocks) == 2
    assert all(block["half"] == "pre" for block in blocks)

    bench = _bench_by_main(db, scenario["night_id"])
    assert bench[_main(scenario["players"][0])]["played_pre_min"] == 16
    assert bench[_main(scenario["players"][0])]["bench_pre_min"] == 8


def test_simulated_multi_report_night_matches_single_report_totals(monkeypatch):
    start_dt = datetime(2024, 8, 22, 19, 0, tzinfo=PT)
    segments = [
        {"gap_before_min": 10, "duration_min": 9, "difficulty": "heroic", "encounter_id": 501, "participants": list(range(25)), "kill": True, "report_code": "R1"},
        {"gap_before_min": 4, "duration_min": 8, "difficulty": "mythic", "encounter_id": 1, "participants": list(range(20)), "kill": False, "report_code": "R1"},
        {"gap_before_min": 5, "duration_min": 9, "difficulty": "mythic", "encounter_id": 1, "participants": list(range(20)), "kill": True, "report_code": "R2"},
        {"gap_before_min": 20, "duration_min": 8, "difficulty": "mythic", "encounter_id": 2, "participants": list(range(5, 25)), "kill": False, "report_code": "R2"},
        {"gap_before_min": 4, "duration_min": 8, "difficulty": "mythic", "encounter_id": 2, "participants": list(range(5, 25)), "kill": True, "report_code": "R2"},
    ]
    break_start = int((start_dt + timedelta(minutes=60)).timestamp() * 1000)
    break_end = int((start_dt + timedelta(minutes=75)).timestamp() * 1000)

    multi_report = _build_night(
        night_id="2024-08-22",
        start_dt=start_dt,
        roster_count=30,
        segments=segments,
        report_overrides={
            "R1": {"break_override_start_ms": break_start, "break_override_end_ms": break_end},
            "R2": {},
        },
    )
    single_report = _build_night(
        night_id="2024-08-22",
        start_dt=start_dt,
        roster_count=30,
        segments=[{**segment, "report_code": "R1"} for segment in segments],
        report_overrides={"R1": {"break_override_start_ms": break_start, "break_override_end_ms": break_end}},
    )

    db_multi, _captured_multi, _settings_multi = _run_scenario(monkeypatch, multi_report)
    db_single, _captured_single, _settings_single = _run_scenario(monkeypatch, single_report)

    qa_multi = db_multi["night_qa"].find_one({"night_id": "2024-08-22"}, {"_id": 0})
    qa_single = db_single["night_qa"].find_one({"night_id": "2024-08-22"}, {"_id": 0})
    assert qa_multi["mythic_pre_min"] == qa_single["mythic_pre_min"]
    assert qa_multi["mythic_post_min"] == qa_single["mythic_post_min"]
    assert _bench_by_main(db_multi, "2024-08-22") == _bench_by_main(db_single, "2024-08-22")


def test_simulated_alt_mapping_and_availability_override(monkeypatch):
    start_dt = datetime(2024, 8, 27, 19, 0, tzinfo=PT)
    scenario = _build_night(
        night_id="2024-08-27",
        start_dt=start_dt,
        roster_count=30,
        segments=[
            {"gap_before_min": 5, "duration_min": 8, "difficulty": "mythic", "encounter_id": 1, "participants": [0] + list(range(5, 24)), "kill": False},
            {"gap_before_min": 5, "duration_min": 9, "difficulty": "mythic", "encounter_id": 1, "participants": [0] + list(range(5, 24)), "kill": True},
        ],
        roster_map_rows=[["Alt", "Main"], ["AltOne", "Player01"]],
        availability_overrides_rows=[
            ["Night", "Main", "Avail Pre?", "Avail Post?", "Reason"],
            ["2024-08-27", "Player30", "Y", "", "Available but sat"],
        ],
    )
    scenario["roster_map_rows"] = [["Alt", "Main"], ["AltOne", "Player01"]]
    scenario["fights_all"][0]["participants"][0] = {"name": "AltOne"}
    scenario["fights_all"][1]["participants"][0] = {"name": "AltOne"}

    db, _captured, _settings = _run_scenario(monkeypatch, scenario)
    bench = _bench_by_main(db, scenario["night_id"])
    assert "Player01" in bench
    assert bench["Player01"]["played_pre_min"] == 22
    assert bench["Player30"]["bench_pre_min"] == 22
    assert bench["Player30"]["status_source"] == "override"


def test_generated_night_invariants(monkeypatch):
    seeds = [7, 11, 19, 23, 31, 37]
    for idx, seed in enumerate(seeds):
        rng = Random(seed)
        start_dt = datetime(2024, 9, 3 + idx, 19, rng.randint(0, 30), tzinfo=PT)
        roster_count = 30
        include_mythic = idx != 0
        players = _players(roster_count)
        roster_docs = [{"main": name, "join_night": start_dt.strftime("%Y-%m-%d"), "active": True} for name in players]

        segments = []
        total_minutes = rng.randint(185, 235)
        elapsed_minutes = 0
        report_break = rng.randint(8, 16)
        before_break_target = rng.randint(80, 110)
        break_gap = rng.randint(10, 25)
        mythic_seen = False

        while elapsed_minutes < total_minutes:
            gap_before = rng.randint(3, 7)
            duration_min = rng.randint(6, 11)
            candidate_start = elapsed_minutes + gap_before
            if before_break_target <= candidate_start < before_break_target + break_gap:
                gap_before += before_break_target + break_gap - candidate_start
                candidate_start = elapsed_minutes + gap_before

            pool = ["normal", "heroic"] if not include_mythic else ["normal", "heroic", "mythic", "mythic"]
            difficulty = rng.choice(pool)
            if difficulty == "mythic":
                mythic_seen = True

            if candidate_start > total_minutes:
                break

            participants = sorted(rng.sample(range(roster_count), rng.randint(20, 25)))
            segments.append(
                {
                    "gap_before_min": gap_before,
                    "duration_min": duration_min,
                    "duration_sec": rng.choice([0, 15, 30, 45]),
                    "difficulty": difficulty,
                    "encounter_id": 0 if rng.random() < 0.12 else 100 + len(segments),
                    "participants": participants,
                    "kill": rng.random() < 0.45,
                    "report_code": "R1" if len(segments) < report_break else "R2",
                }
            )
            elapsed_minutes = candidate_start + duration_min

        if include_mythic and not mythic_seen and segments:
            for segment in segments[-3:]:
                segment["difficulty"] = "mythic"
                segment["encounter_id"] = max(1, segment["encounter_id"])

        scenario = {
            "night_id": start_dt.strftime("%Y-%m-%d"),
            "players": players,
            "roster_docs": roster_docs,
            "fights_all": [],
            "reports": [],
            "roster_map_rows": [],
            "availability_overrides_rows": [],
        }
        built = _build_night(
            night_id=scenario["night_id"],
            start_dt=start_dt,
            roster_count=roster_count,
            segments=segments,
        )
        scenario.update(built)

        mythic_fights = [fight for fight in scenario["fights_all"] if fight.get("is_mythic")]
        if mythic_fights:
            reports = []
            for report in scenario["reports"]:
                report = dict(report)
                report["break_override_start_ms"] = int((start_dt + timedelta(minutes=before_break_target)).timestamp() * 1000)
                report["break_override_end_ms"] = report["break_override_start_ms"] + break_gap * 60000
                reports.append(report)
            scenario["reports"] = reports

        db, _captured, _settings = _run_scenario(monkeypatch, scenario)

        if not mythic_fights:
            assert db["night_qa"].count_documents({"night_id": scenario["night_id"]}) == 0
            assert db["bench_night_totals"].count_documents({"night_id": scenario["night_id"]}) == 0
            continue

        oracle = _oracle_expected_night(scenario)
        assert oracle is not None
        qa_doc = db["night_qa"].find_one({"night_id": scenario["night_id"]}, {"_id": 0})
        assert qa_doc["mythic_pre_min"] == float(oracle["mythic_pre_min"])
        assert qa_doc["mythic_post_min"] == float(oracle["mythic_post_min"])

        actual_bench = _bench_by_main(db, scenario["night_id"])
        assert set(actual_bench) == set(oracle["bench"])
        for main, expected in oracle["bench"].items():
            actual = actual_bench[main]
            for key in (
                "played_pre_min",
                "played_post_min",
                "played_total_min",
                "bench_pre_min",
                "bench_post_min",
                "bench_total_min",
                "avail_pre",
                "avail_post",
            ):
                assert actual[key] == expected[key], f"{seed=} {main=} {key=}"

        cloned_segments = [{**segment, "report_code": "R1"} for segment in segments]
        clone = _build_night(
            night_id=scenario["night_id"],
            start_dt=start_dt,
            roster_count=roster_count,
            segments=cloned_segments,
            report_overrides={"R1": {"break_override_start_ms": qa_doc["break_start_ms"], "break_override_end_ms": qa_doc["break_end_ms"]}},
        )
        db_clone, _captured_clone, _settings_clone = _run_scenario(monkeypatch, clone)
        assert _bench_by_main(db, scenario["night_id"]) == _bench_by_main(db_clone, clone["night_id"])


def test_trash_only_bridge_preserves_blocks_in_generated_shape(monkeypatch):
    start_dt = datetime(2024, 9, 17, 19, 0, tzinfo=PT)
    players = _players(30)
    base_segments = [
        {"gap_before_min": 10, "duration_min": 8, "difficulty": "mythic", "encounter_id": 1, "participants": list(range(20)), "kill": False},
        {"gap_before_min": 12, "duration_min": 8, "difficulty": "mythic", "encounter_id": 2, "participants": list(range(20)), "kill": True},
    ]
    trash_segments = [
        base_segments[0],
        {"gap_before_min": 3, "duration_min": 4, "difficulty": "trash", "encounter_id": 0, "participants": list(range(20))},
        {"gap_before_min": 5, "duration_min": 8, "difficulty": "mythic", "encounter_id": 2, "participants": list(range(20)), "kill": True},
    ]
    boss_segments = [
        base_segments[0],
        {"gap_before_min": 3, "duration_min": 4, "difficulty": "heroic", "encounter_id": 601, "participants": list(range(20)), "kill": True},
        {"gap_before_min": 5, "duration_min": 8, "difficulty": "mythic", "encounter_id": 2, "participants": list(range(20)), "kill": True},
    ]

    trash_night = _build_night(night_id="2024-09-17", start_dt=start_dt, roster_count=30, segments=trash_segments)
    boss_night = _build_night(night_id="2024-09-17", start_dt=start_dt, roster_count=30, segments=boss_segments)

    db_trash, _captured_trash, _settings_trash = _run_scenario(monkeypatch, trash_night)
    db_boss, _captured_boss, _settings_boss = _run_scenario(monkeypatch, boss_night)

    trash_blocks = list(db_trash["blocks"].find({"night_id": "2024-09-17", "main": _main(players[0])}, {"_id": 0}))
    boss_blocks = list(db_boss["blocks"].find({"night_id": "2024-09-17", "main": _main(players[0])}, {"_id": 0}))
    assert len(trash_blocks) == 1
    assert len(boss_blocks) == 2


_PATTERN_MATRIX = [
    (pre_pattern, post_pattern)
    for pre_pattern in _alternating_patterns()
    for post_pattern in _alternating_patterns()
]


@pytest.mark.parametrize(
    ("pre_pattern", "post_pattern"),
    _PATTERN_MATRIX,
    ids=[f"{pre_pattern}|{post_pattern}" for pre_pattern, post_pattern in _PATTERN_MATRIX],
)
def test_exhaustive_heroic_mythic_pattern_matrix(monkeypatch, pre_pattern: str, post_pattern: str):
    start_dt = datetime(2024, 10, 1, 19, 0, tzinfo=PT)
    night_id = "2024-10-01"
    segments = _matrix_segments(pre_pattern, post_pattern)
    scenario = _build_night(
        night_id=night_id,
        start_dt=start_dt,
        roster_count=30,
        segments=segments,
    )
    break_start_ms = max(fight["fight_abs_end_ms"] for fight in scenario["fights_all"] if fight["report_code"] == "R1")
    break_end_ms = min(fight["fight_abs_start_ms"] for fight in scenario["fights_all"] if fight["report_code"] == "R2")
    for report in scenario["reports"]:
        if report["code"] == "R1":
            report["break_override_start_ms"] = break_start_ms
            report["break_override_end_ms"] = break_end_ms

    db, _captured, _settings = _run_scenario(monkeypatch, scenario)
    oracle = _oracle_expected_night(scenario)

    pre_mythic_count = pre_pattern.count("M")
    post_mythic_count = post_pattern.count("M")
    expected_pre_min = pre_mythic_count * 8
    expected_post_min = post_mythic_count * 8
    total_expected = expected_pre_min + expected_post_min

    if total_expected == 0:
        assert db["night_qa"].count_documents({"night_id": night_id}) == 0
        assert db["bench_night_totals"].count_documents({"night_id": night_id}) == 0
        return

    assert oracle is not None
    qa_doc = db["night_qa"].find_one({"night_id": night_id}, {"_id": 0})
    assert qa_doc["mythic_pre_min"] == float(expected_pre_min), f"{pre_pattern=} {post_pattern=}"
    assert qa_doc["mythic_post_min"] == float(expected_post_min), f"{pre_pattern=} {post_pattern=}"
    assert qa_doc["mythic_pre_min"] == float(oracle["mythic_pre_min"]), f"{pre_pattern=} {post_pattern=}"
    assert qa_doc["mythic_post_min"] == float(oracle["mythic_post_min"]), f"{pre_pattern=} {post_pattern=}"

    bench = _bench_by_main(db, night_id)
    assert set(bench) == set(oracle["bench"]), f"{pre_pattern=} {post_pattern=}"
    for main, expected in oracle["bench"].items():
        actual = bench[main]
        for key in (
            "played_pre_min",
            "played_post_min",
            "played_total_min",
            "bench_pre_min",
            "bench_post_min",
            "bench_total_min",
            "avail_pre",
            "avail_post",
        ):
            assert actual[key] == expected[key], f"{pre_pattern=} {post_pattern=} {main=} {key=}"
    odd_main = "Player11"
    even_main = "Player16"
    bench_only_main = "Player26"

    odd_pre_played = ((pre_mythic_count + 1) // 2) * 8
    odd_post_played = ((post_mythic_count + 1) // 2) * 8
    even_pre_played = (pre_mythic_count // 2) * 8
    even_post_played = (post_mythic_count // 2) * 8

    odd_doc = bench[odd_main]
    assert odd_doc["played_pre_min"] == odd_pre_played, f"{pre_pattern=} {post_pattern=}"
    assert odd_doc["played_post_min"] == odd_post_played, f"{pre_pattern=} {post_pattern=}"

    if odd_pre_played + odd_post_played > 0:
        assert odd_doc["bench_total_min"] == total_expected - (odd_pre_played + odd_post_played)

    first_mythic_has_heroic_before = (
        ("M" in pre_pattern and pre_pattern.startswith("H"))
        or ("M" not in pre_pattern and "M" in post_pattern)
    )
    even_should_exist = (even_pre_played + even_post_played) > 0 or first_mythic_has_heroic_before
    if even_should_exist:
        even_doc = bench[even_main]
        assert even_doc["played_pre_min"] == even_pre_played, f"{pre_pattern=} {post_pattern=}"
        assert even_doc["played_post_min"] == even_post_played, f"{pre_pattern=} {post_pattern=}"
        assert even_doc["bench_total_min"] == total_expected - (even_pre_played + even_post_played)
    else:
        assert even_main not in bench, f"{pre_pattern=} {post_pattern=}"

    if first_mythic_has_heroic_before:
        bench_only_doc = bench[bench_only_main]
        assert bench_only_doc["bench_total_min"] == total_expected, f"{pre_pattern=} {post_pattern=}"
        assert bench_only_doc["status_source"] == "last_fight", f"{pre_pattern=} {post_pattern=}"
    else:
        assert bench_only_main not in bench, f"{pre_pattern=} {post_pattern=}"

    odd_blocks = list(
        db["blocks"].find(
            {"night_id": night_id, "main": odd_main},
            {"_id": 0, "half": 1, "block_seq": 1},
        ).sort([("half", 1), ("block_seq", 1)])
    )
    expected_block_count = ((pre_mythic_count + 1) // 2) + ((post_mythic_count + 1) // 2)
    assert len(odd_blocks) == expected_block_count, f"{pre_pattern=} {post_pattern=}"
