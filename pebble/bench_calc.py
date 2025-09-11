from __future__ import annotations
from typing import List

# Availability inference policy (V1):
# - If a player has *any* block in pre, we infer availability for the *entire* post (benched when not playing).
# - If they have any block in post, we infer availability for entire pre.
# - If present in last non‑Mythic fight pre‑switch (not implemented yet), treat as available for full envelope.
# - Officers can override via Availability Overrides sheet; overrides win.


def bench_minutes_for_night(blocks: List[dict], pre_ms: int, post_ms: int) -> List[dict]:
    """Aggregate bench/played minutes for a night.

    ``pre_ms`` and ``post_ms`` are the durations of the pre- and post-break
    halves of the night expressed in milliseconds.
    """

    # aggregate playtime per main+half in milliseconds
    from collections import defaultdict

    agg = defaultdict(lambda: {"pre": 0, "post": 0})
    for b in blocks:
        duration = b["end_ms"] - b["start_ms"]
        agg[b["main"]][b["half"]] += duration

    out: List[dict] = []
    pre_full = pre_ms
    post_full = post_ms
    for main, halves in agg.items():
        pre_played_ms = halves.get("pre", 0)
        post_played_ms = halves.get("post", 0)
        # infer availability + bench
        pre_avail = pre_played_ms > 0 or post_played_ms > 0
        post_avail = pre_played_ms > 0 or post_played_ms > 0
        pre_bench_ms = max(0, pre_full - pre_played_ms) if pre_avail else 0
        post_bench_ms = max(0, post_full - post_played_ms) if post_avail else 0
        out.append({
            "main": main,
            "bench_pre_min": pre_bench_ms // 60000,
            "bench_post_min": post_bench_ms // 60000,
            "played_pre_min": pre_played_ms // 60000,
            "played_post_min": post_played_ms // 60000,
        })
    return out
