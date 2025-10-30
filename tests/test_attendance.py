import sys
from pathlib import Path

import mongomock

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pebble.attendance import (
    build_attendance_probability_rows,
    build_attendance_rows,
)


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
        "Attendance",
        "Played",
        "Bench",
        "Possible",
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


def test_build_attendance_rows_skips_players_outside_roster_window():
    db = mongomock.MongoClient().db

    db["night_qa"].insert_one(
        {
            "night_id": "2024-07-09",
            "mythic_pre_min": 30,
            "mythic_post_min": 30,
        }
    )

    db["team_roster"].insert_many(
        [
            {"main": "Active-Illidan", "join_night": "2024-07-01", "active": True},
            {
                "main": "Past-Illidan",
                "join_night": "2024-06-01",
                "leave_night": "2024-07-01",
                "active": True,
            },
        ]
    )

    db["bench_night_totals"].insert_many(
        [
            {
                "night_id": "2024-07-09",
                "main": "Active-Illidan",
                "played_total_min": 30,
                "bench_total_min": 30,
                "avail_pre": True,
                "avail_post": True,
            },
            {
                "night_id": "2024-07-09",
                "main": "Past-Illidan",
                "played_total_min": 0,
                "bench_total_min": 0,
            },
        ]
    )

    rows = build_attendance_rows(db)

    assert [row[0] for row in rows[1:]] == ["Active-Illidan"]


def test_build_attendance_rows_skips_inactive_roster_entries():
    db = mongomock.MongoClient().db

    db["night_qa"].insert_one(
        {
            "night_id": "2024-07-09",
            "mythic_pre_min": 30,
            "mythic_post_min": 30,
        }
    )

    db["team_roster"].insert_many(
        [
            {
                "main": "Rostered-Illidan",
                "join_night": "2024-07-01",
                "active": True,
            },
            {
                "main": "OffRoster-Illidan",
                "join_night": "2024-07-01",
                "active": False,
            },
        ]
    )

    db["bench_night_totals"].insert_many(
        [
            {
                "night_id": "2024-07-09",
                "main": "Rostered-Illidan",
                "played_total_min": 30,
                "bench_total_min": 30,
                "avail_pre": True,
                "avail_post": True,
            },
            {
                "night_id": "2024-07-09",
                "main": "OffRoster-Illidan",
                "played_total_min": 30,
                "bench_total_min": 30,
                "avail_pre": True,
                "avail_post": True,
            },
        ]
    )

    rows = build_attendance_rows(db)

    assert [row[0] for row in rows[1:]] == ["Rostered-Illidan"]


def test_build_attendance_rows_skips_players_joining_after_last_night():
    db = mongomock.MongoClient().db

    db["night_qa"].insert_many(
        [
            {"night_id": "2024-07-09", "mythic_pre_min": 30, "mythic_post_min": 30},
            {"night_id": "2024-07-11", "mythic_pre_min": 30, "mythic_post_min": 30},
        ]
    )

    db["team_roster"].insert_many(
        [
            {"main": "Current-Illidan", "join_night": "2024-07-01", "active": True},
            {"main": "Future-Illidan", "join_night": "2024-08-01", "active": True},
        ]
    )

    db["bench_night_totals"].insert_many(
        [
            {
                "night_id": "2024-07-09",
                "main": "Current-Illidan",
                "played_total_min": 30,
                "bench_total_min": 30,
                "avail_pre": True,
                "avail_post": True,
            },
            {
                "night_id": "2024-07-11",
                "main": "Current-Illidan",
                "played_total_min": 30,
                "bench_total_min": 30,
                "avail_pre": True,
                "avail_post": True,
            },
        ]
    )

    rows = build_attendance_rows(db)

    assert [row[0] for row in rows[1:]] == ["Current-Illidan"]


def _expected_probabilities(rates):
    team_size = len(rates)
    dp = [0.0] * (team_size + 1)
    dp[0] = 1.0

    for rate in rates:
        for attendees in range(team_size, 0, -1):
            dp[attendees] = dp[attendees] * (1 - rate) + dp[attendees - 1] * rate
        dp[0] *= 1 - rate

    tail = 0.0
    for minimum in range(team_size, -1, -1):
        tail += dp[minimum]
        yield minimum, (tail, dp[minimum])


def test_attendance_probability_table_uses_top_attendance_rates():
    db = mongomock.MongoClient().db

    db["night_qa"].insert_one(
        {
            "night_id": "2024-08-01",
            "mythic_pre_min": 50,
            "mythic_post_min": 50,
        }
    )

    rates = [
        0.95,
        0.5,
        0.7,
        0.85,
        0.6,
        0.4,
        0.8,
        0.3,
        0.9,
        0.65,
        0.55,
        0.75,
        0.45,
        0.35,
        0.25,
        0.15,
        0.05,
        1.0,
        0.88,
        0.77,
        0.66,
    ]

    total_possible = 100

    db["team_roster"].insert_many(
        [
            {"main": f"Player-{i:02d}", "join_night": "2024-07-01", "active": True}
            for i in range(len(rates))
        ]
    )

    db["bench_night_totals"].insert_many(
        [
            {
                "night_id": "2024-08-01",
                "main": f"Player-{i:02d}",
                "played_total_min": rate * total_possible,
                "bench_total_min": 0,
                "avail_pre": True,
                "avail_post": True,
            }
            for i, rate in enumerate(rates)
        ]
    )

    probability_rows = build_attendance_probability_rows(db)

    assert probability_rows[0] == [
        "Players",
        "Predicted",
        "Actual",
        "Delta",
    ]
    assert len(probability_rows) == 13

    data_rows = [row for row in probability_rows[1:] if row[0] != ""]

    assert [row[0] for row in data_rows] == list(range(20, 20 + len(data_rows)))

    expected = dict(_expected_probabilities(rates))
    expected_predicted = dict(_expected_probabilities([0.9] * len(rates)))

    for (
        minimum_players,
        predicted_str,
        at_least_str,
        delta_str,
    ) in data_rows:
        expected_at_least = expected[minimum_players][0]
        expected_predicted_at_least = expected_predicted[minimum_players][0]
        expected_delta =  expected_at_least - expected_predicted_at_least

        assert at_least_str == f"{expected_at_least * 100:.1f}%"
        assert predicted_str == f"{expected_predicted_at_least * 100:.1f}%"
        assert delta_str == f"{expected_delta * 100:.1f}%"

    blank_rows = [row for row in probability_rows[1:] if row[0] == ""]
    assert len(probability_rows) - 1 == len(data_rows) + len(blank_rows)
    assert all(row == [""] * 4 for row in blank_rows)


def test_attendance_probability_table_contains_blank_rows_without_roster():
    db = mongomock.MongoClient().db

    probability_rows = build_attendance_probability_rows(db)

    assert probability_rows[0] == [
        "Players",
        "Predicted",
        "Actual",
        "Delta",
    ]
    assert len(probability_rows) == 13
    assert all(row == [""] * 4 for row in probability_rows[1:])
