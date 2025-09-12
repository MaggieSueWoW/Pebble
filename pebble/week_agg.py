from __future__ import annotations
from typing import List
from datetime import datetime, timedelta

from .utils.time import PT


def week_id_from_night_id(night_id: str) -> str:
    """Map a night id (PT date string) to the Tuesday of that week.

    The game week resets each Tuesday at 00:00 PT. Nights occurring on any day
    of that game week share the same week id represented by that Tuesday's ISO
    date (YYYY-MM-DD).
    """

    dt = datetime.strptime(night_id, "%Y-%m-%d").replace(tzinfo=PT)
    offset = (dt.weekday() - 1) % 7  # Tuesday is weekday() == 1
    tuesday = dt - timedelta(days=offset)
    return tuesday.strftime("%Y-%m-%d")


def materialize_week_totals(db) -> int:
    nights = list(db["bench_night_totals"].find({}, {"_id": 0}))
    # group by (game_week, main)
    from collections import defaultdict

    agg = defaultdict(
        lambda: {
            "played": 0,
            "bench": 0,
            "bench_pre": 0,
            "bench_post": 0,
            "role": None,
        }
    )
    weeks = set()

    for r in nights:
        wk = week_id_from_night_id(r["night_id"])
        weeks.add(wk)
        key = (wk, r["main"])
        agg[key]["played"] += int(r.get("played_pre_min", 0)) + int(
            r.get("played_post_min", 0)
        )
        agg[key]["bench_pre"] += int(r.get("bench_pre_min", 0))
        agg[key]["bench_post"] += int(r.get("bench_post_min", 0))
        agg[key]["bench"] = agg[key]["bench_pre"] + agg[key]["bench_post"]
        if not agg[key]["role"] and r.get("role"):
            agg[key]["role"] = r.get("role")

    # Include roster mains active during observed weeks even if they didn't play
    roster = list(db["team_roster"].find({}, {"_id": 0}))
    for row in roster:
        main = row.get("main")
        if not main or row.get("active") is False:
            continue
        join = row.get("join_night") or "1970-01-01"
        leave = row.get("leave_night") or "9999-12-31"
        join_wk = week_id_from_night_id(join)
        leave_wk = week_id_from_night_id(leave)
        for wk in weeks:
            if join_wk <= wk <= leave_wk:
                key = (wk, main)
                if key not in agg:
                    agg[key] = {
                        "played": 0,
                        "bench": 0,
                        "bench_pre": 0,
                        "bench_post": 0,
                        "role": row.get("role"),
                    }
                elif not agg[key].get("role") and row.get("role"):
                    agg[key]["role"] = row.get("role")

    count = 0
    for (wk, main), v in agg.items():
        bench_pre = v.get("bench_pre", 0)
        bench_post = v.get("bench_post", 0)
        bench_total = bench_pre + bench_post
        doc = {
            "game_week": wk,
            "main": main,
            "role": v.get("role"),
            "played_min": v["played"],
            "bench_min": bench_total,
            "bench_pre_min": bench_pre,
            "bench_post_min": bench_post,
            "updated_at": datetime.utcnow(),
        }
        db["bench_week_totals"].update_one(
            {"game_week": wk, "main": main}, {"$set": doc}, upsert=True
        )
        count += 1

    return count


def materialize_rankings(db) -> int:
    """Materialize season-to-date bench rankings ordered by bench minutes."""

    pipeline = [
        {
            "$group": {
                "_id": "$main",
                "bench_min": {"$sum": "$bench_min"},
                "role": {"$first": "$role"},
            }
        },
        {"$sort": {"bench_min": -1, "_id": 1}},
    ]
    rows: List[dict] = list(db["bench_week_totals"].aggregate(pipeline))

    count = 0
    for idx, r in enumerate(rows, start=1):
        doc = {
            "rank": idx,
            "main": r["_id"],
            "role": r.get("role"),
            "bench_min": r["bench_min"],
            "updated_at": datetime.utcnow(),
        }
        db["bench_rankings"].update_one({"main": r["_id"]}, {"$set": doc}, upsert=True)
        count += 1

    return count
