import mongomock

from pebble.week_agg import materialize_week_totals, week_id_from_night_id


def test_week_id_maps_to_tuesday():
    assert week_id_from_night_id("2024-07-04") == "2024-07-02"
    assert week_id_from_night_id("2024-07-02") == "2024-07-02"


def test_materialize_week_totals_fills_roster():
    db = mongomock.MongoClient().db
    db["bench_night_totals"].insert_many(
        [
            {
                "night_id": "2024-07-02",
                "main": "Alice-Illidan",
                "played_pre_min": 5,
                "played_post_min": 5,
                "bench_pre_min": 5,
                "bench_post_min": 5,
            },
            {
                "night_id": "2024-07-04",
                "main": "Bob-Illidan",
                "played_pre_min": 10,
                "played_post_min": 10,
                "bench_pre_min": 0,
                "bench_post_min": 0,
            },
        ]
    )
    db["team_roster"].insert_many(
        [
            {"main": "Alice-Illidan", "join_night": "2024-06-25"},
            {"main": "Bob-Illidan", "join_night": "2024-06-25"},
            {"main": "Charlie-Illidan", "join_night": "2024-06-25"},
            {"main": "Eve-Illidan", "join_night": "2024-07-09"},  # joins later
            {
                "main": "Frank-Illidan",
                "join_night": "2024-06-18",
                "leave_night": "2024-06-25",
            },  # left before
        ]
    )

    count = materialize_week_totals(db)
    rows = list(
        db["bench_week_totals"].find(
            {}, {"_id": 0, "game_week": 1, "main": 1, "played_min": 1, "bench_min": 1}
        )
    )
    assert count == 3
    assert sorted(rows, key=lambda r: r["main"]) == [
        {
            "game_week": "2024-07-02",
            "main": "Alice-Illidan",
            "played_min": 10,
            "bench_min": 10,
        },
        {
            "game_week": "2024-07-02",
            "main": "Bob-Illidan",
            "played_min": 20,
            "bench_min": 0,
        },
        {
            "game_week": "2024-07-02",
            "main": "Charlie-Illidan",
            "played_min": 0,
            "bench_min": 0,
        },
    ]
