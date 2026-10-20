"""Public attendance projection uses the existing attendance calculation."""

import json

import mongomock
import pytest

from pebble.attendance import build_attendance_projection, build_attendance_rows


def test_projection_matches_sheets_minutes_rate_rounding_and_tuesday_weeks():
    db = mongomock.MongoClient().db
    db.night_qa.insert_many(
        [
            {"night_id": "2026-10-12", "mythic_pre_min": 10.9, "mythic_post_min": 20.8},
            {"night_id": "2026-10-13", "mythic_pre_min": 10.9, "mythic_post_min": 20.8},
        ]
    )
    db.team_roster.insert_one({"main": "Example", "active": True, "notes": "PRIVATE"})
    db.bench_night_totals.insert_many(
        [
            {
                "night_id": "2026-10-12",
                "main": "Example",
                "played_total_min": 10.04,
                "bench_total_min": 19.96,
                "out_pre_min": 0.01,
                "avail_pre": True,
                "avail_post": True,
                "notes": "PRIVATE",
            },
            {
                "night_id": "2026-10-13",
                "main": "Example",
                "played_total_min": 5.02,
                "bench_total_min": 0,
                "avail_pre": True,
                "avail_post": False,
            },
        ]
    )
    value = build_attendance_projection(db)["Example"]
    rows = build_attendance_rows(db)
    assert rows[0][5:] == ["2026-10-06", "2026-10-13"]
    assert value == {
        "played": 15.1,
        "bench": 20.0,
        "possible": 60,
        "rate": pytest.approx(35.02 / 60),
        "weeks": [
            {"week": "2026-10-06", "status": "PBO"},
            {"week": "2026-10-13", "status": "PO"},
        ],
    }
    assert rows[1][2:5] == [value["played"], value["bench"], value["possible"]]
    assert rows[1][1] == f"{value['rate'] * 100:.1f}%"
    assert rows[1][5:] == [week["status"] for week in value["weeks"]]
    assert "PRIVATE" not in json.dumps(value)
    assert set(value) == {"played", "bench", "possible", "rate", "weeks"}


def test_projection_preserves_existing_membership_and_active_eligibility():
    db = mongomock.MongoClient().db
    db.night_qa.insert_many(
        [
            {"night_id": "2026-10-12", "mythic_pre_min": 10, "mythic_post_min": 20},
            {"night_id": "2026-10-13", "mythic_pre_min": 10, "mythic_post_min": 20},
        ]
    )
    db.team_roster.insert_many(
        [
            {"main": "Current", "active": True},
            {"main": "Joined", "active": True, "join_night": "2026-10-13"},
            {"main": "Leaving", "active": True, "leave_night": "2026-10-13"},
            {"main": "Former", "active": True, "leave_night": "2026-10-12"},
            {"main": "Future", "active": True, "join_night": "2026-10-14"},
            {"main": "Inactive", "active": False},
        ]
    )
    values = build_attendance_projection(db)
    assert (
        set(values)
        == {row[0] for row in build_attendance_rows(db)[1:]}
        == {"Current", "Joined", "Leaving"}
    )
    assert values["Current"]["possible"] == values["Leaving"]["possible"] == 60
    assert values["Joined"]["possible"] == 30
    assert values["Joined"]["weeks"] == [
        {"week": "2026-10-06", "status": ""},
        {"week": "2026-10-13", "status": "O"},
    ]


@pytest.mark.parametrize("observed", [False, True])
def test_projection_uses_null_rate_when_no_possible_minutes(observed):
    db = mongomock.MongoClient().db
    db.team_roster.insert_one({"main": "Example", "active": True})
    if observed:
        db.night_qa.insert_one(
            {"night_id": "2026-10-13", "mythic_pre_min": 0.9, "mythic_post_min": 0.9}
        )
    value = build_attendance_projection(db)["Example"]
    assert value["played"] == value["bench"] == value["possible"] == 0
    assert value["rate"] is None
    assert value["weeks"] == (
        [{"week": "2026-10-13", "status": ""}] if observed else []
    )
