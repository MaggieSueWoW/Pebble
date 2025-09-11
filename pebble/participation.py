from __future__ import annotations
from typing import List


# TODO: V1 keeps participation simple (boss pulls only). Trash bridging handled in blocks.
# A later iteration can integrate WCL tables/events to derive exact attendance.


def build_mythic_participation(fights_mythic: List[dict]) -> List[dict]:
    """Return rows of per‑player participation for Mythic fights.

    Each fight is expected to include absolute start/end times and a
    ``participants`` list containing player dictionaries with at least a
    ``name`` field.  The returned rows use natural keys so callers can
    upsert them idempotently.
    """

    rows: List[dict] = []
    for f in fights_mythic:
        for p in f.get("participants", []):
            name = p.get("name")
            if not name:
                continue
            rows.append({
                "main": name,
                "report_code": f.get("report_code"),
                "fight_id": f.get("id"),
                "start_ms": f.get("fight_abs_start_ms"),
                "end_ms": f.get("fight_abs_end_ms"),
                "night_id": f.get("night_id"),
            })
    return rows
