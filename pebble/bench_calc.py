from __future__ import annotations
from typing import List, Dict

# Availability inference policy (V1):
# - If a player has *any* block in pre, we infer availability for the *entire* post (benched when not playing).
# - If they have any block in post, we infer availability for entire pre.
# - If present in last non‑Mythic fight pre‑switch (not implemented yet), treat as available for full envelope.
# - Officers can override via Availability Overrides sheet; overrides win.


def bench_minutes_for_night(blocks: List[dict], envelope: tuple[int, int]) -> List[dict]:
    s, e = envelope
    full = (e - s) // 60000

    # aggregate per main+half
    from collections import defaultdict
    agg = defaultdict(lambda: {"pre": 0, "post": 0})
    for b in blocks:
        minutes = (b["end_ms"] - b["start_ms"]) // 60000
        agg[b["main"]][b["half"]] += minutes

    out: List[dict] = []
    for main, halves in agg.items():
        pre_played = halves.get("pre", 0)
        post_played = halves.get("post", 0)
        # infer availability + bench
        pre_avail = pre_played > 0 or post_played > 0
        post_avail = pre_played > 0 or post_played > 0
        pre_bench = max(0, full - pre_played) if pre_avail else 0
        post_bench = max(0, full - post_played) if post_avail else 0
        out.append({
            "main": main,
            "bench_pre_min": pre_bench,
            "bench_post_min": post_bench,
            "played_pre_min": pre_played,
            "played_post_min": post_played,
        })
    return out
