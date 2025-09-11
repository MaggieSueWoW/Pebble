from __future__ import annotations
from typing import List, Optional, Tuple


def detect_break(all_fights: List[dict], *,
                 window_start_min: int = 30,
                 window_end_min: int = 120,
                 min_break_min: int = 10,
                 max_break_min: int = 30) -> Optional[Tuple[int, int]]:
    """Return (break_start_ms, break_end_ms) or None if no valid break.

    We take the largest inter‑fight gap whose *gap midpoint* lies in the configured window
    (from first fight start). Then clamp by min/max lengths.
    """
    if not all_fights:
        return None
    fights = sorted(all_fights, key=lambda f: f["fight_abs_start_ms"])  # expects absolute times
    night0 = fights[0]["fight_abs_start_ms"]

    best = None
    best_gap = 0
    for a, b in zip(fights, fights[1:]):
        gap = (b["fight_abs_start_ms"] - a["fight_abs_end_ms"]) // 1000 // 60  # minutes
        if gap <= 0:
            continue
        mid_min = ((a["fight_abs_end_ms"] + b["fight_abs_start_ms"]) // 2 - night0) // 1000 // 60
        if window_start_min <= mid_min <= window_end_min:
            if gap > best_gap:
                best_gap = gap
                best = (a["fight_abs_end_ms"], b["fight_abs_start_ms"])  # ms

    if not best:
        return None

    start, end = best
    length_min = (end - start) // 1000 // 60
    if length_min < min_break_min or length_min > max_break_min:
        return None
    return (start, end)
