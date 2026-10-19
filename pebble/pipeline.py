from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass

from .bench_calc import bench_minutes_for_night, last_non_mythic_boss_mains
from .blocks import build_blocks
from .breaks import detect_break
from .envelope import (
    mythic_envelope,
    mythic_half_summary_from_fights,
    split_pre_post_from_fights,
)
from .participation import build_mythic_participation
from .utils.names import NameResolver
from .utils.time import ms_to_pt_iso, ms_to_pt_sheets, pt_time_to_ms


@dataclass
class Computation:
    """Computed tables shared by the Sheets and Firestore adapters."""

    night_qa_rows: list
    bench_rows: list
    weeks_written: int
    week_total_docs: list
    ranks_written: int
    ranking_docs: list


def compute_results(
    s, db, log, *, roster_map=None, overrides_by_night=None, overrides_unmatched=None
):
    """Compute the established nightly and season results from local source data."""
    roster_map = roster_map or {}
    roster_docs = list(db["team_roster"].find({}, {"_id": 0, "main": 1, "active": 1}))
    active_mains = [
        r.get("main")
        for r in roster_docs
        if r.get("main") and r.get("active", True) is not False
    ]
    resolver = NameResolver(active_mains, roster_map)

    overrides_by_night = overrides_by_night or {}
    overrides_unmatched = overrides_unmatched or {}

    # Night loop: derive QA + bench
    nights = sorted(
        set([r["night_id"] for r in db["reports"].find({}, {"night_id": 1, "_id": 0})])
    )

    night_qa_rows = [
        [
            "Night ID",
            "Reports Involved",
            "Mains Seen",
            "Not on Roster",
            "Report Start (PT)",
            "Report End (PT)",
            "Night Start (PT)",
            "Night End (PT)",
            "Mythic Fights",
            "Break Start (PT)",
            "Break End (PT)",
            "Break Override Start (PT)",
            "Break Override End (PT)",
            "Break Duration (min)",
            "Mythic Override Start (PT)",
            "Mythic Override End (PT)",
            "Mythic Start (PT)",
            "Mythic End (PT)",
            "Mythic Pre (min)",
            "Mythic Post (min)",
            "Mythic Post Extension (min)",
            "Gap Window",
            "Min/Max Break",
            "Largest Gap (min)",
            "Candidate Gaps (JSON)",
            "Override Used?",
        ]
    ]
    bench_rows = [
        [
            "Night ID",
            "Main",
            "Played Pre (min)",
            "Played Post (min)",
            "Played Total (min)",
            "Bench Pre (min)",
            "Bench Post (min)",
            "Bench Total (min)",
            "Avail Pre?",
            "Avail Post?",
            "Status Source",
        ]
    ]

    for night in nights:
        fights_all = list(db["fights_all"].find({"night_id": night}, {"_id": 0}))
        if not fights_all:
            continue
        fights_m = [f for f in fights_all if f.get("is_mythic")]

        night_unmatched_start = set(resolver.not_on_roster)

        reports = list(db["reports"].find({"night_id": night}, {"_id": 0}))
        report_codes = sorted(r.get("code") for r in reports)
        report_start_ms = min(r.get("start_ms") for r in reports)
        report_end_ms = max(r.get("end_ms") for r in reports)
        night_start_ms = min(f["fight_abs_start_ms"] for f in fights_all)
        night_end_ms = max(f["fight_abs_end_ms"] for f in fights_all)

        mythic_override_pair = next(
            (
                (r.get("mythic_override_start_ms"), r.get("mythic_override_end_ms"))
                for r in reports
                if r.get("mythic_override_start_ms") is not None
                or r.get("mythic_override_end_ms") is not None
            ),
            (None, None),
        )
        mythic_override_start_ms, mythic_override_end_ms = mythic_override_pair

        if mythic_override_start_ms is None:
            first_boss_fight = min(
                (f for f in fights_all if int(f.get("encounter_id", 0)) > 0),
                key=lambda f: f.get("fight_abs_start_ms", 0),
                default=None,
            )
            mythic_default_start_pt = (
                getattr(s.time, "mythic_default_start_pt", "") or ""
            )
            if (
                first_boss_fight
                and first_boss_fight.get("is_mythic")
                and mythic_default_start_pt
            ):
                default_mythic_override_start_ms = pt_time_to_ms(
                    mythic_default_start_pt, report_start_ms
                )
                if default_mythic_override_start_ms is not None:
                    mythic_override_start_ms = default_mythic_override_start_ms

        env = mythic_envelope(fights_m)
        if not env:
            continue
        auto_env_start, auto_env_end = env
        env_start, env_end = auto_env_start, auto_env_end
        mythic_override_used = False
        start_extension_ms = 0
        end_extension_ms = 0
        if mythic_override_start_ms is not None:
            env_start = mythic_override_start_ms
            mythic_override_used = True
            start_extension_ms = max(0, auto_env_start - mythic_override_start_ms)
        if mythic_override_end_ms is not None:
            env_end = mythic_override_end_ms
            mythic_override_used = True
            end_extension_ms = max(0, mythic_override_end_ms - auto_env_end)
        env = (env_start, env_end)

        mains_by_report: dict[str, set[str]] = {code: set() for code in report_codes}
        for f in fights_all:
            if int(f.get("encounter_id", 0)) <= 0:
                continue
            code = f.get("report_code")
            if code not in mains_by_report:
                mains_by_report[code] = set()
            for p in f.get("participants", []) or []:
                name = p.get("name")
                if not name:
                    continue
                main = resolver.resolve(name)
                if not main:
                    continue
                mains_by_report[code].add(main)
        report_mains = [len(mains_by_report[c]) for c in report_codes]
        override_pair = next(
            (
                (r.get("break_override_start_ms"), r.get("break_override_end_ms"))
                for r in reports
                if r.get("break_override_start_ms") and r.get("break_override_end_ms")
            ),
            (None, None),
        )
        override_start_ms, override_end_ms = override_pair

        bw = s.time.break_window
        window_start_ms = pt_time_to_ms(bw.start_pt, night_start_ms)
        window_end_ms = pt_time_to_ms(bw.end_pt, night_start_ms)
        window_start_min = int((window_start_ms - night_start_ms) / 60000)
        window_end_min = int((window_end_ms - night_start_ms) / 60000)
        br_auto, gap_meta = detect_break(
            fights_all,
            window_start_min=window_start_min,
            window_end_min=window_end_min,
            min_break_min=bw.min_gap_minutes,
            max_break_min=bw.max_gap_minutes,
            night_start_ms=night_start_ms,
        )
        br_range = br_auto
        override_used = mythic_override_used
        if override_start_ms and override_end_ms:
            br_range = (override_start_ms, override_end_ms)
            override_used = True

        post_extension_min_cfg = (
            getattr(s.time, "mythic_post_extension_min", 0.0) or 0.0
        )
        post_extension_ms = int(round(max(0.0, post_extension_min_cfg) * 60000))
        base_split = split_pre_post_from_fights(
            fights_all, fights_m, br_range, envelope=env
        )
        effective_extension_ms = post_extension_ms if base_split["post_ms"] > 0 else 0
        post_extension_credit_ms = effective_extension_ms + end_extension_ms

        split = split_pre_post_from_fights(
            fights_all,
            fights_m,
            br_range,
            envelope=env,
            post_extension_ms=effective_extension_ms,
        )
        split_summary = mythic_half_summary_from_fights(
            fights_all,
            fights_m,
            br_range,
            envelope=env,
            post_extension_ms=effective_extension_ms,
        )
        break_duration = (
            round((br_range[1] - br_range[0]) / 60000.0, 2) if br_range else ""
        )
        post_extension_min = round(post_extension_credit_ms / 60000.0, 2)
        candidate_gaps_db = [
            {
                "start": ms_to_pt_iso(c["start_ms"]),
                "end": ms_to_pt_iso(c["end_ms"]),
                "gap_min": round(c["gap_min"], 2),
            }
            for c in gap_meta.get("candidates", [])
        ]
        candidate_gaps_sheet = [
            {
                "start": ms_to_pt_sheets(c["start_ms"]),
                "end": ms_to_pt_sheets(c["end_ms"]),
                "gap_min": round(c["gap_min"], 2),
            }
            for c in gap_meta.get("candidates", [])
        ]
        largest_gap = round(gap_meta.get("largest_gap_min", 0.0), 2)

        mythic_fight_windows = [
            {
                "fight_id": f.get("id"),
                "half": (
                    "pre"
                    if (
                        not br_range
                        or (
                            (
                                f.get("fight_abs_start_ms", 0)
                                + f.get("fight_abs_end_ms", 0)
                            )
                            // 2
                        )
                        < br_range[0]
                    )
                    else "post"
                ),
                "start": ms_to_pt_iso(f.get("fight_abs_start_ms")),
                "end": ms_to_pt_iso(f.get("fight_abs_end_ms")),
            }
            for f in sorted(
                fights_m, key=lambda fight: fight.get("fight_abs_start_ms", 0)
            )
        ]

        def _half_log_payload(name: str) -> dict:
            half = split_summary[name]
            return {
                "start": (
                    ms_to_pt_iso(half["start_ms"])
                    if half["start_ms"] is not None
                    else None
                ),
                "end": (
                    ms_to_pt_iso(half["end_ms"]) if half["end_ms"] is not None else None
                ),
                "duration_min": round(half["duration_ms"] / 60000.0, 2),
                "excluded_intervals": [
                    {
                        "start": ms_to_pt_iso(start),
                        "end": ms_to_pt_iso(end),
                    }
                    for start, end in half["excluded_intervals"]
                ],
            }

        log.info(
            "mythic half timing",
            extra={
                "stage": "compute",
                "night_id": night,
                "mythic_fights": mythic_fight_windows,
                "mythic_pre": _half_log_payload("pre"),
                "mythic_post": _half_log_payload("post"),
                "break_range": {
                    "start": ms_to_pt_iso(br_range[0]) if br_range else None,
                    "end": ms_to_pt_iso(br_range[1]) if br_range else None,
                },
                "mythic_envelope": {
                    "start": ms_to_pt_iso(env[0]),
                    "end": ms_to_pt_iso(env[1]),
                },
            },
        )

        first_mythic_mains: set[str] = set()
        last_mythic_mains: set[str] = set()
        if fights_m:
            first_mythic_fight = min(
                fights_m,
                key=lambda f: (
                    f.get("fight_abs_start_ms")
                    if f.get("fight_abs_start_ms") is not None
                    else f.get("fight_abs_end_ms", 0)
                ),
            )
            last_mythic_fight = max(
                fights_m,
                key=lambda f: (
                    f.get("fight_abs_end_ms")
                    if f.get("fight_abs_end_ms") is not None
                    else f.get("fight_abs_start_ms", 0)
                ),
            )

            for fight, bucket in (
                (first_mythic_fight, first_mythic_mains),
                (last_mythic_fight, last_mythic_mains),
            ):
                for p in fight.get("participants", []) or []:
                    name = p.get("name")
                    if not name:
                        continue
                    main = resolver.resolve(name)
                    if not main:
                        continue
                    bucket.add(main)

        mythic_mains: set[str] = set()
        for f in fights_m:
            for p in f.get("participants", []) or []:
                name = p.get("name")
                if not name:
                    continue
                main = resolver.resolve(name)
                if not main:
                    continue
                mythic_mains.add(main)
        new_unmatched = set(resolver.not_on_roster) - night_unmatched_start
        override_unmatched = overrides_unmatched.get(night, set())
        not_on_roster = sorted(new_unmatched | set(override_unmatched))
        not_on_roster_str = ", ".join(not_on_roster)

        night_qa_rows.append(
            [
                night,
                ",".join(report_codes),
                ",".join(str(c) for c in report_mains),
                not_on_roster_str,
                ms_to_pt_sheets(report_start_ms),
                ms_to_pt_sheets(report_end_ms),
                ms_to_pt_sheets(night_start_ms),
                ms_to_pt_sheets(night_end_ms),
                len(fights_m),
                ms_to_pt_sheets(br_range[0]) if br_range else "",
                ms_to_pt_sheets(br_range[1]) if br_range else "",
                ms_to_pt_sheets(override_start_ms) if override_start_ms else "",
                ms_to_pt_sheets(override_end_ms) if override_end_ms else "",
                f"{break_duration:.2f}" if break_duration != "" else "",
                (
                    ms_to_pt_sheets(mythic_override_start_ms)
                    if mythic_override_start_ms is not None
                    else ""
                ),
                (
                    ms_to_pt_sheets(mythic_override_end_ms)
                    if mythic_override_end_ms is not None
                    else ""
                ),
                ms_to_pt_sheets(env[0]),
                ms_to_pt_sheets(env[1]),
                f"{split['pre_ms'] / 60000.0:.2f}",
                f"{split['post_ms'] / 60000.0:.2f}",
                f"{post_extension_min:.2f}",
                f"{bw.start_pt}-{bw.end_pt}",
                f"{bw.min_gap_minutes}-{bw.max_gap_minutes}",
                f"{largest_gap:.2f}",
                json.dumps(candidate_gaps_sheet),
                "Y" if override_used else "N",
            ]
        )
        # Persist Night QA to Mongo (idempotent)
        qa_doc = {
            "night_id": night,
            "reports": report_codes,
            "report_mains": report_mains,
            "report_start_ms": report_start_ms,
            "report_end_ms": report_end_ms,
            "night_start_ms": night_start_ms,
            "night_end_ms": night_end_ms,
            "mythic_fights": len(fights_m),
            "mythic_start_ms": env[0],
            "mythic_end_ms": env[1],
            "break_start_ms": br_range[0] if br_range else None,
            "break_end_ms": br_range[1] if br_range else None,
            "break_override_start_ms": override_start_ms,
            "break_override_end_ms": override_end_ms,
            "break_duration_min": break_duration if break_duration != "" else None,
            "mythic_pre_min": round(split["pre_ms"] / 60000.0, 2),
            "mythic_post_min": round(split["post_ms"] / 60000.0, 2),
            "mythic_post_extension_min": post_extension_min,
            "mythic_override_start_ms": mythic_override_start_ms,
            "mythic_override_end_ms": mythic_override_end_ms,
            "gap_window": (bw.start_pt, bw.end_pt),
            "min_max_break": (bw.min_gap_minutes, bw.max_gap_minutes),
            "largest_gap_min": largest_gap,
            "gap_candidates": candidate_gaps_db,
            "override_used": override_used,
            "not_on_roster_mains": not_on_roster,
        }
        db["night_qa"].update_one({"night_id": night}, {"$set": qa_doc}, upsert=True)

        # Participation stage: build per-fight rows and persist
        part_rows = build_mythic_participation(fights_m, resolver=resolver)
        db["participation_m"].delete_many({"night_id": night})
        if part_rows:
            db["participation_m"].insert_many(part_rows)

        # Blocks stage
        blocks = build_blocks(part_rows, break_range=br_range, fights_all=fights_all)

        seq = defaultdict(int)
        block_docs = []
        for b in blocks:
            seq_key = (b["night_id"], b["main"], b["half"])
            seq[seq_key] += 1
            block_docs.append({**b, "block_seq": seq[seq_key]})

        db["blocks"].delete_many({"night_id": night})
        if block_docs:
            db["blocks"].insert_many(block_docs)

        # Determine participants from the last non-Mythic boss fight before Mythic start
        last_nm_mains = last_non_mythic_boss_mains(
            fights_all, env[0], resolver=resolver
        )

        bench = bench_minutes_for_night(
            blocks,
            split["pre_ms"],
            split["post_ms"],
            overrides=overrides_by_night.get(night, {}),
            last_fight_mains=last_nm_mains,
            roster_map=None,
            pre_extension_ms=start_extension_ms,
            pre_extension_mains=first_mythic_mains,
            post_extension_ms=post_extension_credit_ms,
            post_extension_mains=last_mythic_mains,
        )

        # Persist bench_night_totals for this night
        bench_docs = []
        for row in bench:
            bench_rows.append(
                [
                    night,
                    row["main"],
                    row["played_pre_min"],
                    row["played_post_min"],
                    row["played_total_min"],
                    row["bench_pre_min"],
                    row["bench_post_min"],
                    row["bench_total_min"],
                    row["avail_pre"],
                    row["avail_post"],
                    row["status_source"],
                ]
            )
            doc = {
                "night_id": night,
                "main": row["main"],
                "played_pre_min": row["played_pre_min"],
                "played_post_min": row["played_post_min"],
                "played_total_min": row["played_total_min"],
                "bench_pre_min": row["bench_pre_min"],
                "bench_post_min": row["bench_post_min"],
                "bench_total_min": row["bench_total_min"],
                "avail_pre": row["avail_pre"],
                "avail_post": row["avail_post"],
                "status_source": row["status_source"],
            }
            bench_docs.append(doc)

        db["bench_night_totals"].delete_many({"night_id": night})
        if bench_docs:
            db["bench_night_totals"].insert_many(bench_docs)

    # Refresh weekly aggregates so Mongo mirrors the latest nightly totals
    from .week_agg import materialize_rankings, materialize_week_totals

    weeks_written, week_total_docs = materialize_week_totals(db, include_docs=True)
    ranks_written, ranking_docs = materialize_rankings(db, include_docs=True)

    log.info(
        "weekly aggregates refreshed",
        extra={
            "stage": "compute.week",
            "weeks": weeks_written,
            "ranks": ranks_written,
        },
    )

    return Computation(
        night_qa_rows,
        bench_rows,
        weeks_written,
        week_total_docs,
        ranks_written,
        ranking_docs,
    )
