import click
import json
from .config_loader import load_settings
from .logging_setup import setup_logging
from .mongo_client import get_db, ensure_indexes
from .ingest import ingest_reports, _sheet_values
from .envelope import mythic_envelope, split_pre_post
from .breaks import detect_break
from .blocks import build_blocks
from .bench_calc import bench_minutes_for_night, last_non_mythic_boss_mains
from .participation import build_mythic_participation
from .export_sheets import replace_values
from .utils.time import ms_to_pt_iso, pt_time_to_ms


@click.group()
def cli():
    pass


@cli.group(help="Initialize external resources.")
def bootstrap():
    """Initialize external resources."""
    pass


@bootstrap.command()
@click.option("--config", default="config.yaml", show_default=True)
def sheets(config):
    log = setup_logging()
    s = load_settings(config)
    try:
        from .bootstrap.sheets_bootstrap import bootstrap_sheets

        res = bootstrap_sheets(s)
        log.info(
            "sheets bootstrap complete", extra={"stage": "bootstrap.sheets", **res}
        )
    except Exception as exc:
        log.info(
            "TODO: bootstrap sheets",
            extra={"stage": "bootstrap.sheets", "error": str(exc)},
        )


@bootstrap.command()
@click.option("--config", default="config.yaml", show_default=True)
def mongo(config):
    log = setup_logging()
    s = load_settings(config)
    try:
        from .bootstrap.mongo_bootstrap import bootstrap_mongo

        res = bootstrap_mongo(s)
        log.info("mongo bootstrap complete", extra={"stage": "bootstrap.mongo", **res})
    except Exception as exc:
        log.info(
            "TODO: bootstrap mongo",
            extra={"stage": "bootstrap.mongo", "error": str(exc)},
        )


@cli.command()
@click.option("--config", default="config.yaml", show_default=True)
def ingest(config):
    log = setup_logging()
    s = load_settings(config)
    res = ingest_reports(s)
    log.info("ingest complete", extra={"stage": "ingest", **res})


@cli.command()
@click.option("--config", default="config.yaml", show_default=True)
def compute(config):
    """Compute Night QA and bench tables from staged Mongo collections.

    Reads from ``fights_all`` then materializes ``participation_m`` and
    ``blocks`` before aggregating bench minutes.
    """
    log = setup_logging()
    s = load_settings(config)
    db = get_db(s)
    ensure_indexes(db)

    from pymongo import UpdateOne
    from typing import Dict, Optional

    def _parse_bool(val: str) -> Optional[bool]:
        v = val.strip().lower()
        if v in ("", "-", "na"):
            return None
        if v in ("y", "yes", "true", "1", "t"):
            return True
        if v in ("n", "no", "false", "0", "f"):
            return False
        return None

    # Load roster map from Sheets (alt -> main)
    roster_map: Dict[str, str] = {}
    rows = _sheet_values(s, s.sheets.tabs.roster_map)
    if rows:
        header = rows[0]
        try:
            alt_idx = header.index("Character (Name-Realm)")
            main_idx = header.index("Main (Name-Realm)")
            for r in rows[1:]:
                if alt_idx < len(r) and main_idx < len(r):
                    alt = r[alt_idx].split("-")[0].strip()
                    main = r[main_idx].split("-")[0].strip()
                    if alt and main:
                        roster_map[alt] = main
        except ValueError:
            pass

    # Load availability overrides from Sheets
    overrides_by_night: Dict[str, Dict[str, Dict[str, Optional[bool]]]] = {}
    rows = _sheet_values(s, s.sheets.tabs.availability_overrides)
    if rows:
        header = rows[0]
        try:
            n_idx = header.index("Night ID")
            m_idx = header.index("Main")
            pre_idx = header.index("Avail Pre?")
            post_idx = header.index("Avail Post?")
            for r in rows[1:]:
                night = r[n_idx].strip() if n_idx < len(r) else ""
                name = r[m_idx].split("-")[0].strip() if m_idx < len(r) else ""
                if not night or not name:
                    continue
                ov = {
                    "pre": _parse_bool(r[pre_idx]) if pre_idx < len(r) else None,
                    "post": _parse_bool(r[post_idx]) if post_idx < len(r) else None,
                }
                main = roster_map.get(name, name)
                overrides_by_night.setdefault(night, {})[main] = ov
        except ValueError:
            pass

    # Night loop: derive QA + bench
    nights = sorted(
        set([r["night_id"] for r in db["reports"].find({}, {"night_id": 1, "_id": 0})])
    )

    night_qa_rows = [
        [
            "Night ID",
            "Reports Involved",
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
            "Mythic Start (PT)",
            "Mythic End (PT)",
            "Mythic Pre (min)",
            "Mythic Post (min)",
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
            "Bench Pre (min)",
            "Bench Post (min)",
        ]
    ]

    for night in nights:
        fights_all = list(db["fights_all"].find({"night_id": night}, {"_id": 0}))
        if not fights_all:
            continue
        fights_m = [f for f in fights_all if f.get("is_mythic")]

        env = mythic_envelope(fights_m)
        if not env:
            continue

        reports = list(db["reports"].find({"night_id": night}, {"_id": 0}))
        report_codes = sorted(r.get("code") for r in reports)
        report_start_ms = min(r.get("start_ms") for r in reports)
        report_end_ms = max(r.get("end_ms") for r in reports)
        night_start_ms = min(f["fight_abs_start_ms"] for f in fights_all)
        night_end_ms = max(f["fight_abs_end_ms"] for f in fights_all)
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
        )
        br_range = br_auto
        override_used = False
        if override_start_ms and override_end_ms:
            br_range = (override_start_ms, override_end_ms)
            override_used = True

        split = split_pre_post(env, br_range)
        break_duration = (
            round((br_range[1] - br_range[0]) / 60000.0, 2) if br_range else ""
        )
        candidate_gaps = [
            {
                "start": ms_to_pt_iso(c["start_ms"]),
                "end": ms_to_pt_iso(c["end_ms"]),
                "gap_min": round(c["gap_min"], 2),
            }
            for c in gap_meta.get("candidates", [])
        ]
        largest_gap = round(gap_meta.get("largest_gap_min", 0.0), 2)

        night_qa_rows.append(
            [
                night,
                ",".join(report_codes),
                ms_to_pt_iso(report_start_ms),
                ms_to_pt_iso(report_end_ms),
                ms_to_pt_iso(night_start_ms),
                ms_to_pt_iso(night_end_ms),
                len(fights_m),
                ms_to_pt_iso(br_range[0]) if br_range else "",
                ms_to_pt_iso(br_range[1]) if br_range else "",
                ms_to_pt_iso(override_start_ms) if override_start_ms else "",
                ms_to_pt_iso(override_end_ms) if override_end_ms else "",
                f"{break_duration:.2f}" if break_duration != "" else "",
                ms_to_pt_iso(env[0]),
                ms_to_pt_iso(env[1]),
                f"{split['pre_ms'] / 60000.0:.2f}",
                f"{split['post_ms'] / 60000.0:.2f}",
                f"{bw.start_pt}-{bw.end_pt}",
                f"{bw.min_gap_minutes}-{bw.max_gap_minutes}",
                f"{largest_gap:.2f}",
                json.dumps(candidate_gaps),
                "Y" if override_used else "N",
            ]
        )
        # Persist Night QA to Mongo (idempotent)
        qa_doc = {
            "night_id": night,
            "reports": report_codes,
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
            "gap_window": (bw.start_pt, bw.end_pt),
            "min_max_break": (bw.min_gap_minutes, bw.max_gap_minutes),
            "largest_gap_min": largest_gap,
            "gap_candidates": candidate_gaps,
            "override_used": override_used,
        }
        db["night_qa"].update_one({"night_id": night}, {"$set": qa_doc}, upsert=True)

        # Participation stage: build per-fight rows and persist
        part_rows = build_mythic_participation(fights_m)
        ops = []
        for r in part_rows:
            key = {
                "night_id": r["night_id"],
                "report_code": r["report_code"],
                "fight_id": r["fight_id"],
                "main": r["main"],
            }
            ops.append(UpdateOne(key, {"$set": r}, upsert=True))
        if ops:
            db["participation_m"].bulk_write(ops, ordered=False)

        part_rows = list(db["participation_m"].find({"night_id": night}, {"_id": 0}))

        # Blocks stage
        blocks = build_blocks(part_rows, break_range=br_range)
        from collections import defaultdict

        seq = defaultdict(int)
        ops = []
        for b in blocks:
            seq_key = (b["night_id"], b["main"], b["half"])
            seq[seq_key] += 1
            doc = {**b, "block_seq": seq[seq_key]}
            key = {
                "night_id": b["night_id"],
                "main": b["main"],
                "half": b["half"],
                "block_seq": doc["block_seq"],
            }
            ops.append(UpdateOne(key, {"$set": doc}, upsert=True))
        if ops:
            db["blocks"].bulk_write(ops, ordered=False)

        blocks = list(db["blocks"].find({"night_id": night}, {"_id": 0}))

        # Determine participants from the last non-Mythic boss fight before Mythic start
        last_nm_mains = last_non_mythic_boss_mains(fights_all, env[0], roster_map)

        bench = bench_minutes_for_night(
            blocks,
            split["pre_ms"],
            split["post_ms"],
            overrides=overrides_by_night.get(night, {}),
            last_fight_mains=last_nm_mains,
            roster_map=roster_map,
        )

        # Persist bench_night_totals for this night
        ops = []
        for row in bench:
            bench_rows.append(
                [
                    night,
                    row["main"],
                    row["played_pre_min"],
                    row["played_post_min"],
                    row["bench_pre_min"],
                    row["bench_post_min"],
                ]
            )
            doc = {
                "night_id": night,
                "main": row["main"],
                "played_pre_min": row["played_pre_min"],
                "played_post_min": row["played_post_min"],
                "bench_pre_min": row["bench_pre_min"],
                "bench_post_min": row["bench_post_min"],
            }
            ops.append(
                UpdateOne(
                    {"night_id": night, "main": row["main"]}, {"$set": doc}, upsert=True
                )
            )
        if ops:
            db["bench_night_totals"].bulk_write(ops, ordered=False)

    # Write to Sheets
    replace_values(
        s.sheets.spreadsheet_id,
        s.sheets.tabs.night_qa,
        night_qa_rows,
        s.service_account_json,
    )
    replace_values(
        s.sheets.spreadsheet_id,
        s.sheets.tabs.bench_night_totals,
        bench_rows,
        s.service_account_json,
    )

    log.info("compute complete", extra={"stage": "compute", "nights": len(nights)})


@cli.command()
@click.option("--config", default="config.yaml", show_default=True)
def week(config):
    from .config_loader import load_settings
    from .logging_setup import setup_logging
    from .mongo_client import get_db
    from .week_agg import materialize_week_totals
    from .export_sheets import replace_values

    log = setup_logging()
    s = load_settings(config)
    db = get_db(s)
    n = materialize_week_totals(db)

    # export
    rows = [["Game Week", "Main", "Played (min)", "Bench (min)"]]
    for r in (
        db["bench_week_totals"]
        .find({}, {"_id": 0})
        .sort([("game_week", 1), ("main", 1)])
    ):
        rows.append([r["game_week"], r["main"], r["played_min"], r["bench_min"]])
    replace_values(
        s.sheets.spreadsheet_id,
        s.sheets.tabs.bench_week_totals,
        rows,
        s.service_account_json,
    )

    log.info("week export complete", extra={"stage": "week", "rows": n})


@cli.command()
@click.option("--config", default="config.yaml", show_default=True)
def export(config):
    log = setup_logging()
    load_settings(config)
    log.info("TODO: export results", extra={"stage": "export"})


@cli.command()
@click.option("--config", default="config.yaml", show_default=True)
def backfill(config):
    log = setup_logging()
    load_settings(config)
    log.info("TODO: backfill", extra={"stage": "backfill"})


@cli.command()
@click.option("--config", default="config.yaml", show_default=True)
def verify(config):
    log = setup_logging()
    load_settings(config)
    log.info("TODO: verify", extra={"stage": "verify"})


def main():
    cli()


if __name__ == "__main__":
    main()
