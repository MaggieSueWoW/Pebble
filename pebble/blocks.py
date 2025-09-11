from __future__ import annotations
from typing import List, Dict


def build_blocks(participation_rows: List[dict], *, break_range: tuple[int, int] | None) -> List[dict]:
    """Collapse per‑fight rows into contiguous blocks per (main, night_id, half).
    This includes trash bridging: any adjacent fights with <= 10 min gap fuse.
    """
    if not participation_rows:
        return []

    # group by main+night
    from collections import defaultdict
    groups: Dict[tuple, list] = defaultdict(list)
    for r in participation_rows:
        groups[(r["main"], r["night_id"])].append(r)

    blocks: List[dict] = []
    for (main, night), rows in groups.items():
        rows.sort(key=lambda r: r["start_ms"])
        current = None
        for r in rows:
            half = None
            if break_range:
                bs, be = break_range
                mid = (r["start_ms"] + r["end_ms"]) // 2
                half = "pre" if mid < bs else "post"
            else:
                half = "pre"

            if current and current["half"] == half and r["start_ms"] - current["end_ms"] <= 10 * 60 * 1000:
                current["end_ms"] = max(current["end_ms"], r["end_ms"])
            else:
                if current:
                    blocks.append(current)
                current = {
                    "main": main,
                    "night_id": night,
                    "half": half,
                    "start_ms": r["start_ms"],
                    "end_ms": r["end_ms"],
                }
        if current:
            blocks.append(current)
    return blocks
