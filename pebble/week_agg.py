from __future__ import annotations
from typing import List
from datetime import datetime


def week_id_from_night_id(night_id: str) -> str:
    # Tuesday‑based game week id = ISO date of that Tuesday (PT). For simplicity V1 uses ISO week.
    # TODO: implement Tuesday reset based on project rules.
    return night_id  # placeholder: one night per week key (okay for iteration)


def materialize_week_totals(db) -> int:
    nights = list(db["bench_night_totals"].find({}, {"_id": 0}))
    # group by (game_week, main)
    from collections import defaultdict
    agg = defaultdict(lambda: {"played": 0, "bench": 0})

    for r in nights:
        wk = week_id_from_night_id(r["night_id"])  # TODO real week id
        key = (wk, r["main"])
        agg[key]["played"] += int(r.get("played_pre_min", 0)) + int(r.get("played_post_min", 0))
        agg[key]["bench"] += int(r.get("bench_pre_min", 0)) + int(r.get("bench_post_min", 0))

    ops = []
    from pymongo import UpdateOne
    for (wk, main), v in agg.items():
        doc = {
            "game_week": wk,
            "main": main,
            "played_min": v["played"],
            "bench_min": v["bench"],
            "updated_at": datetime.utcnow(),
        }
        ops.append(UpdateOne({"game_week": wk, "main": main}, {"$set": doc}, upsert=True))

    if ops:
        db["bench_week_totals"].bulk_write(ops, ordered=False)
    return len(ops)
