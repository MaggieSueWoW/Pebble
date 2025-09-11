from __future__ import annotations
from typing import Dict, List

# TODO: V1 keeps participation simple (boss pulls only). Trash bridging handled in blocks.
# A later iteration can integrate WCL tables/events to derive exact attendance.


def build_mythic_participation(fights_mythic: List[dict], friendlies: List[dict]) -> List[dict]:
    """Return rows of {main, report_code, fight_id, start_ms, end_ms}.
    For now, assume everyone from friendlies who is of type 'Player' is eligible.
    Officers can override availability; next iteration: per‑fight participants.
    """
    players = [f["name"] for f in friendlies if str(f.get("type")).lower() == "player"]
    rows: List[dict] = []
    for f in fights_mythic:
        for name in players:
            rows.append({
                "main": name,
                "report_code": f["report_code"],
                "fight_id": f["id"],
                "start_ms": f["start_ms"],
                "end_ms": f["end_ms"],
                "night_id": f["night_id"],
            })
    return rows