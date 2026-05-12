from pebble.envelope import (
    non_mythic_boss_gap_intervals,
    split_pre_post,
    split_pre_post_from_fights,
)


def test_split_pre_post_adds_post_extension():
    envelope = (0, 120_000)
    break_range = (60_000, 70_000)
    res = split_pre_post(envelope, break_range, post_extension_ms=30_000)
    assert res["pre_ms"] == 60_000
    # Base post duration would be 50_000ms; expect +30_000ms extension.
    assert res["post_ms"] == 80_000


def test_split_pre_post_extension_skipped_without_break():
    envelope = (0, 90_000)
    res = split_pre_post(envelope, None, post_extension_ms=45_000)
    assert res == {"pre_ms": 90_000, "post_ms": 0}


def test_split_pre_post_extension_skipped_without_post_mythic():
    envelope = (0, 50_000)
    break_range = (60_000, 70_000)
    res = split_pre_post(envelope, break_range, post_extension_ms=45_000)
    assert res == {"pre_ms": 50_000, "post_ms": 0}


def test_split_pre_post_excludes_non_mythic_gap_time_across_halves():
    envelope = (0, 120_000)
    break_range = (60_000, 70_000)
    excluded_intervals = [(40_000, 90_000)]
    res = split_pre_post(envelope, break_range, excluded_intervals=excluded_intervals)
    assert res == {"pre_ms": 40_000, "post_ms": 30_000}


def test_non_mythic_boss_gap_intervals_excludes_full_gap_within_same_half():
    fights_mythic = [
        {"fight_abs_start_ms": 0, "fight_abs_end_ms": 20_000, "is_mythic": True},
        {"fight_abs_start_ms": 80_000, "fight_abs_end_ms": 100_000, "is_mythic": True},
    ]
    fights_all = fights_mythic + [
        {
            "fight_abs_start_ms": 35_000,
            "fight_abs_end_ms": 50_000,
            "is_mythic": False,
            "encounter_id": 123,
        },
        {
            "fight_abs_start_ms": 55_000,
            "fight_abs_end_ms": 60_000,
            "is_mythic": False,
            "encounter_id": 124,
        },
    ]
    assert non_mythic_boss_gap_intervals(fights_all, fights_mythic) == [(20_000, 80_000)]


def test_split_pre_post_from_fights_keeps_halves_separate_around_break():
    fights_all = [
        {"fight_abs_start_ms": 10_000, "fight_abs_end_ms": 20_000, "is_mythic": True, "encounter_id": 1},
        {"fight_abs_start_ms": 25_000, "fight_abs_end_ms": 35_000, "is_mythic": True, "encounter_id": 2},
        {"fight_abs_start_ms": 40_000, "fight_abs_end_ms": 50_000, "is_mythic": False, "encounter_id": 101},
        {"fight_abs_start_ms": 80_000, "fight_abs_end_ms": 90_000, "is_mythic": False, "encounter_id": 102},
        {"fight_abs_start_ms": 100_000, "fight_abs_end_ms": 110_000, "is_mythic": True, "encounter_id": 3},
    ]
    fights_mythic = [fight for fight in fights_all if fight["is_mythic"]]
    res = split_pre_post_from_fights(fights_all, fights_mythic, (60_000, 70_000))
    assert res == {"pre_ms": 25_000, "post_ms": 10_000}
