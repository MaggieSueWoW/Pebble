import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from pebble.bench_calc import bench_minutes_for_night


def test_bench_minutes_uses_split_durations():
    blocks = [
        {"main": "A", "half": "pre", "start_ms": 0, "end_ms": 5 * 60000},
        {"main": "A", "half": "post", "start_ms": 0, "end_ms": 10 * 60000},
        {"main": "B", "half": "post", "start_ms": 0, "end_ms": 10 * 60000},
    ]
    res = bench_minutes_for_night(blocks, pre_ms=10 * 60000, post_ms=20 * 60000)
    res_by_main = {r["main"]: r for r in res}

    a = res_by_main["A"]
    assert a["played_pre_min"] == 5
    assert a["played_post_min"] == 10
    assert a["bench_pre_min"] == 5  # 10 - 5
    assert a["bench_post_min"] == 10  # 20 - 10

    b = res_by_main["B"]
    assert b["played_pre_min"] == 0
    assert b["played_post_min"] == 10
    assert b["bench_pre_min"] == 10  # pre half inferred available via post
    assert b["bench_post_min"] == 10
