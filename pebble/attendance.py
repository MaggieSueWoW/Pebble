from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Tuple

from .week_agg import week_id_from_night_id

STATUS_ORDER: tuple[str, ...] = ("P", "B", "O")


@dataclass
class PlayerAttendance:
    main: str
    total_played: float
    total_bench: float
    total_possible: float
    week_status: Dict[str, set[str]]
    attendance_probability: float | None

    @property
    def available_minutes(self) -> float:
        return self.total_played + self.total_bench


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


def _collect_attendance_stats(db) -> Tuple[List[str], List[PlayerAttendance]]:
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

    if not sorted_mains:
        return week_ids, []

    earliest_night = all_night_ids[0] if all_night_ids else None
    latest_night = all_night_ids[-1] if all_night_ids else None
    default_join = earliest_night or "1970-01-01"
    default_leave = latest_night or "9999-12-31"

    players: List[PlayerAttendance] = []

    for main in sorted_mains:
        roster_entry = roster.get(main)

        if roster_entry:
            if not bool(roster_entry.get("active", True)):
                continue

            if latest_night:
                join_night = roster_entry.get("join_night")
                if join_night and join_night > latest_night:
                    continue

                leave_night = roster_entry.get("leave_night")
                if leave_night and leave_night < latest_night:
                    continue

        join = (roster_entry or {}).get("join_night") or default_join
        leave = (roster_entry or {}).get("leave_night") or default_leave

        membership_nights = [n for n in all_night_ids if join <= n <= leave]

        if roster_entry and all_night_ids and not membership_nights:
            continue

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
        attendance_probability = (
            (available / total_possible) if total_possible > 0 else None
        )

        players.append(
            PlayerAttendance(
                main=main,
                total_played=total_played,
                total_bench=total_bench,
                total_possible=total_possible,
                week_status=week_status,
                attendance_probability=attendance_probability,
            )
        )

    return week_ids, players


def build_attendance_rows(db) -> List[List]:
    week_ids, players = _collect_attendance_stats(db)

    header = [
        "Player",
        "Attendance",
        "Played",
        "Bench",
        "Possible",
    ] + week_ids

    rows: List[List] = [header]

    for player in players:
        if player.total_possible > 0 and player.attendance_probability is not None:
            attendance_pct = f"{player.attendance_probability * 100:.1f}%"
        else:
            attendance_pct = ""

        row = [
            player.main,
            attendance_pct,
            _normalize_minutes(player.total_played),
            _normalize_minutes(player.total_bench),
            _normalize_minutes(player.total_possible),
        ]

        for week in week_ids:
            letters = player.week_status.get(week, set())
            status_str = "".join(letter for letter in STATUS_ORDER if letter in letters)
            row.append(status_str)

        rows.append(row)

    return rows


def build_attendance_probability_rows(db, min_players: int = 20) -> List[List]:
    _, players = _collect_attendance_stats(db)

    header = ["Players", "Predicted", "At least K", "Delta", "Exactly K"]
    rows: List[List] = [header]

    total_rows = 12

    if not players:
        blank_row = [""] * len(header)
        rows.extend([blank_row[:] for _ in range(total_rows)])
        return rows

    attendance_rates = [
        (player.attendance_probability or 0.0)
        for player in players
    ]

    team_size = len(attendance_rates)
    dp_actual: List[float] = [0.0] * (team_size + 1)
    dp_actual[0] = 1.0

    dp_predicted: List[float] = [0.0] * (team_size + 1)
    dp_predicted[0] = 1.0

    for rate in attendance_rates:
        for attendees in range(team_size, 0, -1):
            dp_actual[attendees] = (
                dp_actual[attendees] * (1 - rate)
                + dp_actual[attendees - 1] * rate
            )
        dp_actual[0] *= 1 - rate

    predicted_rate = 0.9
    for _ in range(team_size):
        for attendees in range(team_size, 0, -1):
            dp_predicted[attendees] = (
                dp_predicted[attendees] * (1 - predicted_rate)
                + dp_predicted[attendees - 1] * predicted_rate
            )
        dp_predicted[0] *= 1 - predicted_rate

    cumulative_actual: List[float] = [0.0] * (team_size + 1)
    cumulative_predicted: List[float] = [0.0] * (team_size + 1)
    running_actual = 0.0
    running_predicted = 0.0

    for attendees in range(team_size, -1, -1):
        running_actual += dp_actual[attendees]
        running_predicted += dp_predicted[attendees]
        cumulative_actual[attendees] = running_actual
        cumulative_predicted[attendees] = running_predicted

    available_rows = team_size - min_players + 1
    if available_rows < 0:
        available_rows = 0
    rows_to_render = min(total_rows, available_rows)

    for minimum_players in range(min_players, min_players + rows_to_render):
        at_least_probability = cumulative_actual[minimum_players]
        predicted_probability = cumulative_predicted[minimum_players]
        delta = predicted_probability - at_least_probability
        rows.append(
            [
                minimum_players,
                f"{predicted_probability * 100:.1f}%",
                f"{at_least_probability * 100:.1f}%",
                f"{delta * 100:.1f}%",
                f"{dp_actual[minimum_players] * 100:.1f}%",
            ]
        )

    blank_row = [""] * len(header)
    remaining_rows = total_rows - rows_to_render
    rows.extend([blank_row[:] for _ in range(remaining_rows)])

    return rows
