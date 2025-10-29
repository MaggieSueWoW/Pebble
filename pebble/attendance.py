from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List

from .week_agg import week_id_from_night_id

STATUS_ORDER: tuple[str, ...] = ("P", "B", "O")


@dataclass
class HalfMeta:
    minutes: float


@dataclass
class NightMeta:
    night_id: str
    pre: HalfMeta
    post: HalfMeta

    @property
    def total_minutes(self) -> float:
        return (self.pre.minutes or 0.0) + (self.post.minutes or 0.0)


def _has_out_minutes(doc: dict | None) -> bool:
    if not doc:
        return False

    for key, value in doc.items():
        if "out" not in key or not key.endswith("_min"):
            continue
        minutes = float(value or 0)
        if minutes > 0:
            return True
    return False


def _normalize_minutes(minutes: float) -> float | int:
    if abs(minutes - round(minutes)) < 1e-6:
        return int(round(minutes))
    return round(minutes, 1)


def _night_meta_from_doc(doc: dict) -> NightMeta | None:
    night_id = doc.get("night_id")
    if not night_id:
        return None

    pre_min = math.floor(float(doc.get("mythic_pre_min", 0) or 0))
    post_min = math.floor(float(doc.get("mythic_post_min", 0) or 0))

    pre_meta = HalfMeta(minutes=pre_min)
    post_meta = HalfMeta(minutes=post_min)

    return NightMeta(night_id=night_id, pre=pre_meta, post=post_meta)


def build_attendance_rows(db) -> List[List]:
    night_docs = list(db["night_qa"].find({}, {"_id": 0}))
    night_meta_by_id: Dict[str, NightMeta] = {}
    weeks: Dict[str, List[str]] = defaultdict(list)

    for doc in night_docs:
        meta = _night_meta_from_doc(doc)
        if not meta:
            continue
        night_meta_by_id[meta.night_id] = meta
        week = week_id_from_night_id(meta.night_id)
        weeks[week].append(meta.night_id)

    for night_ids in weeks.values():
        night_ids.sort()

    all_night_ids = sorted(night_meta_by_id.keys())
    week_ids = sorted(weeks.keys())

    bench_docs = list(db["bench_night_totals"].find({}, {"_id": 0}))
    bench_by_key = {(doc["night_id"], doc["main"]): doc for doc in bench_docs}

    roster_docs = list(db["team_roster"].find({}, {"_id": 0}))
    roster: Dict[str, dict] = {doc["main"]: doc for doc in roster_docs if doc.get("main")}

    bench_mains = {doc["main"] for doc in bench_docs if doc.get("main")}
    mains = set(roster.keys()) | bench_mains
    sorted_mains = sorted(mains)

    header = [
        "Player",
        "Attendance %",
        "Mythic Played (min)",
        "Mythic Bench (min)",
        "Mythic Possible (min)",
    ] + week_ids

    rows: List[List] = [header]

    if not sorted_mains:
        return rows

    earliest_night = all_night_ids[0] if all_night_ids else "1970-01-01"
    latest_night = all_night_ids[-1] if all_night_ids else "9999-12-31"

    for main in sorted_mains:
        roster_entry = roster.get(main, {})
        if roster_entry.get("active", True) is False and main not in bench_mains:
            continue

        join = roster_entry.get("join_night") or earliest_night
        leave = roster_entry.get("leave_night") or latest_night

        membership_nights = [n for n in all_night_ids if join <= n <= leave]

        total_played = 0.0
        total_bench = 0.0
        total_possible = 0.0

        week_status: Dict[str, set[str]] = {week: set() for week in week_ids}

        for night_id in membership_nights:
            night_meta = night_meta_by_id.get(night_id)
            if not night_meta:
                continue

            bench_doc = bench_by_key.get((night_id, main))
            played_total = float(bench_doc.get("played_total_min", 0) or 0) if bench_doc else 0.0
            bench_total = float(bench_doc.get("bench_total_min", 0) or 0) if bench_doc else 0.0
            if bench_doc:
                total_played += played_total
                total_bench += bench_total
            total_possible += night_meta.total_minutes

            week = week_id_from_night_id(night_id)
            letters = week_status.setdefault(week, set())

            if bench_doc is None:
                if night_meta.total_minutes > 0:
                    letters.add("O")
                continue

            if played_total > 0:
                letters.add("P")
            if bench_total > 0:
                letters.add("B")

            if _has_out_minutes(bench_doc):
                letters.add("O")
            else:
                for half in ("pre", "post"):
                    half_minutes = getattr(night_meta, half).minutes
                    if half_minutes <= 0:
                        continue
                    if not bool(bench_doc.get(f"avail_{half}", False)):
                        letters.add("O")
                        break

        available = total_played + total_bench
        if total_possible > 0:
            attendance_pct = f"{(available / total_possible) * 100:.1f}%"
        else:
            attendance_pct = ""

        row = [
            main,
            attendance_pct,
            _normalize_minutes(total_played),
            _normalize_minutes(total_bench),
            _normalize_minutes(total_possible),
        ]

        for week in week_ids:
            letters = week_status.get(week, set())
            status_str = "".join(letter for letter in STATUS_ORDER if letter in letters)
            row.append(status_str)

        rows.append(row)

    return rows
