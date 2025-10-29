import sys
from pathlib import Path

import mongomock

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pebble.attendance import build_attendance_rows


def _minutes(minutes: int) -> int:
    return minutes * 60000


def test_build_attendance_rows_includes_week_markers():
    db = mongomock.MongoClient().db

    night1 = {
        "night_id": "2024-07-09",
        "mythic_pre_min": 20,
        "mythic_post_min": 40,
        "mythic_start_ms": 0,
        "mythic_end_ms": _minutes(70),
        "break_start_ms": _minutes(20),
        "break_end_ms": _minutes(30),
        "mythic_post_extension_min": 0,
    }
    night2 = {
        "night_id": "2024-07-11",
        "mythic_pre_min": 20,
        "mythic_post_min": 40,
        "mythic_start_ms": _minutes(2880),
        "mythic_end_ms": _minutes(2880 + 70),
        "break_start_ms": _minutes(2880 + 20),
        "break_end_ms": _minutes(2880 + 30),
        "mythic_post_extension_min": 0,
    }
    db["night_qa"].insert_many([night1, night2])

    db["team_roster"].insert_many(
        [
            {"main": "A-Illidan", "join_night": "2024-07-01", "active": True},
            {"main": "B-Illidan", "join_night": "2024-07-01", "active": True},
            {"main": "C-Illidan", "join_night": "2024-07-01", "active": True},
        ]
    )

    db["bench_night_totals"].insert_many(
        [
            {
                "night_id": "2024-07-09",
                "main": "A-Illidan",
                "played_pre_min": 0,
                "played_post_min": 30,
                "played_total_min": 30,
                "bench_pre_min": 20,
                "bench_post_min": 10,
                "bench_total_min": 30,
                "avail_pre": True,
                "avail_post": True,
            },
            {
                "night_id": "2024-07-11",
                "main": "A-Illidan",
                "played_pre_min": 0,
                "played_post_min": 0,
                "played_total_min": 0,
                "bench_pre_min": 0,
                "bench_post_min": 0,
                "bench_total_min": 0,
                "avail_pre": False,
                "avail_post": False,
            },
            {
                "night_id": "2024-07-09",
                "main": "B-Illidan",
                "played_pre_min": 20,
                "played_post_min": 40,
                "played_total_min": 60,
                "bench_pre_min": 0,
                "bench_post_min": 0,
                "bench_total_min": 0,
                "avail_pre": True,
                "avail_post": True,
            },
            {
                "night_id": "2024-07-11",
                "main": "B-Illidan",
                "played_pre_min": 20,
                "played_post_min": 0,
                "played_total_min": 20,
                "bench_pre_min": 0,
                "bench_post_min": 40,
                "bench_total_min": 40,
                "avail_pre": True,
                "avail_post": True,
            },
        ]
    )

    db["blocks"].insert_many(
        [
            {"night_id": "2024-07-09", "main": "A-Illidan", "half": "post", "start_ms": _minutes(30), "end_ms": _minutes(60)},
            {"night_id": "2024-07-09", "main": "B-Illidan", "half": "pre", "start_ms": 0, "end_ms": _minutes(20)},
            {"night_id": "2024-07-09", "main": "B-Illidan", "half": "post", "start_ms": _minutes(30), "end_ms": _minutes(70)},
            {"night_id": "2024-07-11", "main": "B-Illidan", "half": "pre", "start_ms": _minutes(2880 + 0), "end_ms": _minutes(2880 + 20)},
        ]
    )

    rows = build_attendance_rows(db)

    assert rows[0][:5] == [
        "Player",
        "Attendance %",
        "Mythic Played (min)",
        "Mythic Bench (min)",
        "Mythic Possible (min)",
    ]
    assert rows[0][5:] == ["2024-07-09"]

    data = {row[0]: row for row in rows[1:]}

    assert data["A-Illidan"][1] == "50.0%"
    assert data["A-Illidan"][2] == 30
    assert data["A-Illidan"][3] == 30
    assert data["A-Illidan"][4] == 120
    assert data["A-Illidan"][5] == "PBO"

    assert data["B-Illidan"][1] == "100.0%"
    assert data["B-Illidan"][2] == 80
    assert data["B-Illidan"][3] == 40
    assert data["B-Illidan"][4] == 120
    assert data["B-Illidan"][5] == "PB"

    assert data["C-Illidan"][1] == "0.0%"
    assert data["C-Illidan"][2] == 0
    assert data["C-Illidan"][3] == 0
    assert data["C-Illidan"][4] == 120
    assert data["C-Illidan"][5] == "O"


def test_full_participation_rounds_to_hundred_percent():
    db = mongomock.MongoClient().db

    db["night_qa"].insert_one(
        {
            "night_id": "2024-08-06",
            "mythic_pre_min": 149.6,
            "mythic_post_min": 0.0,
            "mythic_start_ms": 0,
            "mythic_end_ms": _minutes(149.6),
            "mythic_post_extension_min": 0,
        }
    )

    db["team_roster"].insert_one({"main": "PerfectPlayer", "join_night": "2024-08-06", "active": True})

    db["bench_night_totals"].insert_one(
        {
            "night_id": "2024-08-06",
            "main": "PerfectPlayer",
            "played_pre_min": 149,
            "played_post_min": 0,
            "played_total_min": 149,
            "bench_pre_min": 0,
            "bench_post_min": 0,
            "bench_total_min": 0,
            "avail_pre": True,
            "avail_post": False,
        }
    )

    rows = build_attendance_rows(db)

    assert rows[1][0] == "PerfectPlayer"
    assert rows[1][1] == "100.0%"
    assert rows[1][2] == 149
    assert rows[1][3] == 0
    assert rows[1][4] == 149
    assert rows[1][5] == "P"
