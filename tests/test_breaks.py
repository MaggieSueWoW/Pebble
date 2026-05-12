from pebble.breaks import detect_break


def test_detect_break_candidates():
    fights = [
        {"fight_abs_start_ms": 0, "fight_abs_end_ms": 600000, "encounter_id": 1},
        {"fight_abs_start_ms": 1200000, "fight_abs_end_ms": 1800000, "encounter_id": 2},
        {
            "fight_abs_start_ms": 1800000,
            "fight_abs_end_ms": 2000000,
            "encounter_id": 0,
        },  # trash
        {"fight_abs_start_ms": 3000000, "fight_abs_end_ms": 3600000, "encounter_id": 3},
    ]
    br, meta = detect_break(
        fights,
        window_start_min=0,
        window_end_min=60,
        min_break_min=5,
        max_break_min=30,
    )
    assert br == (1800000, 3000000)
    assert meta["largest_gap_min"] == 20
    assert len(meta["candidates"]) == 2


def test_detect_break_accepts_exact_min_and_max_lengths():
    fights = [
        {"fight_abs_start_ms": 0, "fight_abs_end_ms": 10 * 60000, "encounter_id": 1},
        {
            "fight_abs_start_ms": 20 * 60000,
            "fight_abs_end_ms": 30 * 60000,
            "encounter_id": 2,
        },
        {
            "fight_abs_start_ms": 60 * 60000,
            "fight_abs_end_ms": 70 * 60000,
            "encounter_id": 3,
        },
    ]

    br, _meta = detect_break(
        fights,
        window_start_min=0,
        window_end_min=120,
        min_break_min=10,
        max_break_min=30,
    )
    assert br == (30 * 60000, 60 * 60000)


def test_detect_break_rejects_out_of_range_gap_lengths():
    fights = [
        {"fight_abs_start_ms": 0, "fight_abs_end_ms": 10 * 60000, "encounter_id": 1},
        {
            "fight_abs_start_ms": 19 * 60000,
            "fight_abs_end_ms": 25 * 60000,
            "encounter_id": 2,
        },
    ]

    br, meta = detect_break(
        fights,
        window_start_min=0,
        window_end_min=60,
        min_break_min=10,
        max_break_min=30,
    )
    assert br is None
    assert meta["largest_gap_min"] == 9


def test_detect_break_uses_window_midpoint_boundaries():
    fights = [
        {"fight_abs_start_ms": 0, "fight_abs_end_ms": 10 * 60000, "encounter_id": 1},
        {
            "fight_abs_start_ms": 30 * 60000,
            "fight_abs_end_ms": 40 * 60000,
            "encounter_id": 2,
        },
    ]

    br, _meta = detect_break(
        fights,
        window_start_min=20,
        window_end_min=20,
        min_break_min=10,
        max_break_min=30,
    )
    assert br == (10 * 60000, 30 * 60000)


def test_detect_break_returns_none_when_no_gap_is_in_window():
    fights = [
        {"fight_abs_start_ms": 0, "fight_abs_end_ms": 10 * 60000, "encounter_id": 1},
        {
            "fight_abs_start_ms": 25 * 60000,
            "fight_abs_end_ms": 35 * 60000,
            "encounter_id": 2,
        },
    ]

    br, meta = detect_break(
        fights,
        window_start_min=40,
        window_end_min=60,
        min_break_min=10,
        max_break_min=30,
    )
    assert br is None
    assert meta["candidates"] == []
