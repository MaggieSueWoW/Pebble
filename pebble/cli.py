import click
import json
import time
from collections import defaultdict
from typing import Dict, List, Optional, Sequence, Union

from .config_loader import load_settings
from .logging_setup import setup_logging
from .mongo_client import get_db, ensure_indexes
from .ingest import ingest_reports, ingest_roster, _sheet_values_batch
from .envelope import mythic_envelope, split_pre_post
from .breaks import detect_break
from .blocks import build_blocks
from .bench_calc import bench_minutes_for_night, last_non_mythic_boss_mains
from .participation import build_mythic_participation
from .export_sheets import build_replace_values_requests, build_value_update_requests
from .sheets_client import SheetsClient
from .week_agg import materialize_rankings, materialize_week_totals
from .attendance import build_attendance_probability_rows, build_attendance_rows
from .utils.time import (
    ms_to_pt_iso,
    ms_to_pt_sheets,
    pt_time_to_ms,
    sheets_date_str,
)
from .utils.names import NameResolver


def _require_ingest_trigger_range(settings) -> str:
    try:
        trigger_range = settings.sheets.triggers.ingest_compute_week
    except AttributeError as exc:
        raise click.ClickException("sheets.triggers.ingest_compute_week must be configured") from exc
    if not trigger_range:
        raise click.ClickException("sheets.triggers.ingest_compute_week must be configured")
    return trigger_range


def _read_ingest_trigger_checkbox(
    settings,
    *,
    client: SheetsClient,
    trigger_range: str | None = None,
) -> bool:
    rng = trigger_range or _require_ingest_trigger_range(settings)
    svc = client.svc
    resp = client.execute(svc.spreadsheets().values().get(spreadsheetId=settings.sheets.spreadsheet_id, range=rng))
    values = resp.get("values", [])
    if not values or not values[0]:
        return False

    raw = values[0][0]
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return False
    if isinstance(raw, (int, float)):
        return bool(raw)
    if isinstance(raw, str):
        return raw.strip().lower() in {"true", "1", "y", "yes", "t"}
    return False


def _set_ingest_trigger_checkbox(
    settings,
    value: bool,
    *,
    client: SheetsClient,
    trigger_range: str | None = None,
) -> None:
    rng = trigger_range or _require_ingest_trigger_range(settings)
    svc = client.svc
    body = {
        "values": [["TRUE" if value else "FALSE"]],
        "majorDimension": "ROWS",
    }
    client.execute(
        svc.spreadsheets()
        .values()
        .update(
            spreadsheetId=settings.sheets.spreadsheet_id,
            range=rng,
            valueInputOption="USER_ENTERED",
            body=body,
        )
    )


def _wait_for_ingest_trigger(
    settings,
    log,
    timeout: int,
    iteration: int,
    *,
    client: SheetsClient,
    trigger_range: str | None = None,
) -> bool:
    rng = trigger_range or _require_ingest_trigger_range(settings)
    log.info(
        "checking ingest-compute-week trigger",
        extra={
            "stage": "loop",
            "iteration": iteration,
            "timeout_seconds": timeout,
            "range": rng,
        },
    )

    deadline = time.monotonic() + timeout
    if _read_ingest_trigger_checkbox(settings, client=client, trigger_range=rng):
        log.info(
            "ingest-compute-week trigger detected",
            extra={"stage": "loop", "iteration": iteration},
        )
        return True

    now = time.monotonic()
    remaining = max(0.0, deadline - now)
    log.info(
        "ingest-compute-week trigger not detected, waiting",
        extra={"stage": "loop", "iteration": iteration, "delay": remaining},
    )
    time.sleep(remaining)
    return False


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
        log.info("sheets bootstrap complete", extra={"stage": "bootstrap.sheets", **res})
    except Exception:
        log.warning(
            "bootstrap sheets failed",
            extra={"stage": "bootstrap.sheets"},
            exc_info=True,
        )
        raise


@cli.command("flush-cache", help="Flush cached WCL reports from Redis.")
@click.option("--config", default="config.yaml", show_default=True)
def flush_cache_cmd(config):
    log = setup_logging()
    s = load_settings(config)
    from .wcl_client import flush_cache as _flush

    deleted = _flush(s.redis.url, s.redis.key_prefix)
    log.info("cache flushed", extra={"stage": "flush-cache", "keys": deleted})
def _parse_availability_value(val: str) -> Optional[Union[bool, int]]:
    v = val.strip()
    if not v:
        return None

    lowered = v.lower()
    if lowered in ("-", "na"):
        return None

    try:
        minutes = int(v)
    except ValueError:
        pass
    else:
        if minutes == 0:
            return False
        return minutes

    if lowered in ("y", "yes", "true", "t"):
        return True
    if lowered in ("n", "no", "false", "f"):
        return False
    return None


def parse_availability_overrides(
    rows: List[List[str]], resolver: NameResolver
) -> tuple[
    Dict[str, Dict[str, Dict[str, Optional[Union[bool, int]]]]],
    Dict[str, set[str]],
]:
    from collections import defaultdict

    overrides_by_night: Dict[str, Dict[str, Dict[str, Optional[Union[bool, int]]]]] = {}
    unmatched: Dict[str, set[str]] = defaultdict(set)
    if not rows:
        return overrides_by_night, {}
    header = rows[0]
    try:
        n_idx = header.index("Night")
        m_idx = header.index("Main")
        pre_idx = header.index("Avail Pre?")
        post_idx = header.index("Avail Post?")
    except ValueError:
        return overrides_by_night, {}

    for r in rows[1:]:
        night_txt = r[n_idx].strip() if n_idx < len(r) else ""
        night = sheets_date_str(night_txt)
        name = r[m_idx].strip() if m_idx < len(r) else ""
        if not night or not name:
            continue
        ov = {
            "pre": _parse_availability_value(r[pre_idx]) if pre_idx < len(r) else None,
            "post": _parse_availability_value(r[post_idx]) if post_idx < len(r) else None,
        }
        main = resolver.resolve(name)
        if not main:
            unmatched[night].add(name)
            continue
        overrides_by_night.setdefault(night, {})[main] = ov
    return overrides_by_night, {night: set(names) for night, names in unmatched.items()}


def run_pipeline(settings, log):
    """Ingest reports, compute nightly tables, and refresh weekly exports."""

    s = settings
    sheet_client = SheetsClient(s.service_account_json)
    sheet_values = _sheet_values_batch(
        s,
        [
            (
                "reports",
                s.sheets.tabs.reports,
                s.sheets.starts.reports,
            ),
            (
                "team_roster",
                s.sheets.tabs.team_roster,
                s.sheets.starts.team_roster,
            ),
            (
                "roster_map",
                s.sheets.tabs.roster_map,
                s.sheets.starts.roster_map,
            ),
            (
                "availability_overrides",
                s.sheets.tabs.availability_overrides,
                s.sheets.starts.availability_overrides,
            ),
            (
                "attendance_header",
                s.sheets.tabs.attendance,
                s.sheets.starts.attendance,
            ),
        ],
        client=sheet_client,
    )

    report_res = ingest_reports(
        settings, rows=sheet_values.get("reports", []), client=sheet_client
    )
    sheet_value_updates = list(report_res.pop("sheet_updates", []))
    roster_count = ingest_roster(
        settings,
        rows=sheet_values.get("team_roster", []),
        client=sheet_client,
    )
    log.info(
        "ingest complete",
        extra={"stage": "ingest", **report_res, "team_roster": roster_count},
    )

    db = get_db(s)
    ensure_indexes(db)

    # Load roster map from Sheets (alt -> main)
    roster_map: Dict[str, str] = {}
    rows = sheet_values.get("roster_map", [])
    if rows:
        header = rows[0]
        try:
            alt_idx = header.index("Alt")
            main_idx = header.index("Main")
            for r in rows[1:]:
                alt = r[alt_idx].strip() if alt_idx < len(r) else ""
                main = r[main_idx].strip() if main_idx < len(r) else ""
                if alt and main:
                    roster_map[alt] = main
        except ValueError:
            pass

    roster_docs = list(db["team_roster"].find({}, {"_id": 0, "main": 1, "active": 1}))
    active_mains = [r.get("main") for r in roster_docs if r.get("main") and r.get("active", True) is not False]
    resolver = NameResolver(active_mains, roster_map)

    # Load availability overrides from Sheets
    rows = sheet_values.get("availability_overrides", [])
    overrides_by_night, overrides_unmatched = parse_availability_overrides(rows, resolver)

    # Night loop: derive QA + bench
    nights = sorted(set([r["night_id"] for r in db["reports"].find({}, {"night_id": 1, "_id": 0})]))

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

        env = mythic_envelope(fights_m)
        if not env:
            continue

        night_unmatched_start = set(resolver.not_on_roster)

        reports = list(db["reports"].find({"night_id": night}, {"_id": 0}))
        report_codes = sorted(r.get("code") for r in reports)
        report_start_ms = min(r.get("start_ms") for r in reports)
        report_end_ms = max(r.get("end_ms") for r in reports)
        night_start_ms = min(f["fight_abs_start_ms"] for f in fights_all)
        night_end_ms = max(f["fight_abs_end_ms"] for f in fights_all)

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
        override_used = False
        if override_start_ms and override_end_ms:
            br_range = (override_start_ms, override_end_ms)
            override_used = True

        post_extension_min_cfg = getattr(s.time, "mythic_post_extension_min", 0.0) or 0.0
        post_extension_ms = int(round(max(0.0, post_extension_min_cfg) * 60000))
        effective_extension_ms = post_extension_ms if br_range else 0

        split = split_pre_post(env, br_range, post_extension_ms=effective_extension_ms)
        break_duration = round((br_range[1] - br_range[0]) / 60000.0, 2) if br_range else ""
        post_extension_min = round(effective_extension_ms / 60000.0, 2)
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

        last_mythic_mains: set[str] = set()
        if fights_m:
            last_mythic_fight = max(
                fights_m,
                key=lambda f: (
                    f.get("fight_abs_end_ms")
                    if f.get("fight_abs_end_ms") is not None
                    else f.get("fight_abs_start_ms", 0)
                ),
            )
            for p in last_mythic_fight.get("participants", []) or []:
                name = p.get("name")
                if not name:
                    continue
                main = resolver.resolve(name)
                if not main:
                    continue
                last_mythic_mains.add(main)

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

        part_rows = list(db["participation_m"].find({"night_id": night}, {"_id": 0}))

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

        blocks = list(db["blocks"].find({"night_id": night}, {"_id": 0}))

        # Determine participants from the last non-Mythic boss fight before Mythic start
        last_nm_mains = last_non_mythic_boss_mains(fights_all, env[0], resolver=resolver)

        bench = bench_minutes_for_night(
            blocks,
            split["pre_ms"],
            split["post_ms"],
            overrides=overrides_by_night.get(night, {}),
            last_fight_mains=last_nm_mains,
            roster_map=None,
            post_extension_ms=effective_extension_ms,
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

    weeks_written = materialize_week_totals(db)
    ranks_written = materialize_rankings(db)

    log.info(
        "weekly aggregates refreshed",
        extra={
            "stage": "compute.week",
            "weeks": weeks_written,
            "ranks": ranks_written,
        },
    )

    sheet_requests: list[dict] = []

    def queue_sheet_write(
        tab: str,
        values: list[list],
        *,
        start_cell: str,
        last_processed_cell: str | None = None,
        ensure_tail_space: bool = False,
        include_last_processed: bool = False,
        existing_header_row: Sequence[str] | None = None,
    ) -> None:
        sheet_requests.extend(
            build_replace_values_requests(
                s.sheets.spreadsheet_id,
                tab,
                values,
                client=sheet_client,
                start_cell=start_cell,
                last_processed_cell=last_processed_cell,
                ensure_tail_space=ensure_tail_space,
                include_last_processed=include_last_processed,
                existing_header_row=existing_header_row,
            )
        )

    # Queue Sheet writes
    queue_sheet_write(
        s.sheets.tabs.night_qa,
        night_qa_rows,
        start_cell=s.sheets.starts.night_qa,
    )
    queue_sheet_write(
        s.sheets.tabs.bench_night_totals,
        bench_rows,
        start_cell=s.sheets.starts.bench_night_totals,
    )

    log.info("compute complete", extra={"stage": "compute", "nights": len(nights)})

    totals = materialize_week_totals(db)
    rankings = materialize_rankings(db)

    rows = [
        [
            "Game Week",
            "Main",
            "Played Week (min)",
            "Bench Week (min)",
            "Bench Pre (min)",
            "Bench Post (min)",
        ]
    ]
    for rec in db["bench_week_totals"].find({}, {"_id": 0}).sort([("game_week", 1), ("main", 1)]):
        rows.append(
            [
                rec["game_week"],
                rec["main"],
                rec.get("played_min", 0),
                rec.get("bench_min", 0),
                rec.get("bench_pre_min", 0),
                rec.get("bench_post_min", 0),
            ]
        )
    queue_sheet_write(
        settings.sheets.tabs.bench_week_totals,
        rows,
        start_cell=settings.sheets.starts.bench_week_totals,
    )

    attendance_rows = build_attendance_rows(db)
    attendance_existing_header_rows = sheet_values.get("attendance_header", [])
    attendance_existing_header = (
        attendance_existing_header_rows[0]
        if attendance_existing_header_rows
        else None
    )
    queue_sheet_write(
        settings.sheets.tabs.attendance,
        attendance_rows,
        start_cell=settings.sheets.starts.attendance,
        ensure_tail_space=True,
        existing_header_row=attendance_existing_header,
    )

    rank_rows = [
        [
            "Rank",
            "Main",
            "Bench Season-to-date (min)",
            "Time Played (min)",
            "Bench:Played Ratio",
        ]
    ]
    for rec in db["bench_rankings"].find({}, {"_id": 0}).sort([("rank", 1)]):
        ratio = rec.get("bench_to_played_ratio")
        ratio_display = "" if ratio is None else f"{ratio:.2f}"
        rank_rows.append(
            [
                rec["rank"],
                rec["main"],
                rec.get("bench_min", 0),
                rec.get("played_min", 0),
                ratio_display,
            ]
        )
    queue_sheet_write(
        settings.sheets.tabs.bench_rankings,
        rank_rows,
        start_cell=settings.sheets.starts.bench_rankings,
        last_processed_cell=settings.sheets.last_processed.bench_rankings,
        include_last_processed=True,
    )

    if sheet_value_updates:
        sheet_requests.extend(
            build_value_update_requests(
                settings.sheets.spreadsheet_id,
                sheet_value_updates,
                client=sheet_client,
            )
        )

    if sheet_requests:
        sheet_client.execute(
            sheet_client.svc.spreadsheets()
            .batchUpdate(
                spreadsheetId=settings.sheets.spreadsheet_id,
                body={"requests": sheet_requests},
            )
        )

    # probability_rows = build_attendance_probability_rows(db, min_players=18)
    # replace_values(
    #     settings.sheets.spreadsheet_id,
    #     settings.sheets.tabs.attendance,
    #     probability_rows,
    #     settings.service_account_json,
    #     start_cell=settings.sheets.starts.attendance_probability,
    #     clear_range=False,
    # )

    log.info(
        "week export complete",
        extra={
            "stage": "week",
            "totals_updated": totals,
            "rankings_updated": rankings,
        },
    )


@cli.command()
@click.option("--config", default="config.yaml", show_default=True)
@click.option(
    "--max-errors",
    default=5,
    show_default=True,
    type=click.IntRange(0, None),
    help="Maximum consecutive errors before the loop exits. Use 0 to never exit.",
)
@click.option(
    "--trigger-timeout",
    default=0,
    show_default=True,
    type=click.IntRange(0, None),
    help=(
        "Seconds to wait for the ingest-compute-week trigger checkbox before running "
        "the pipeline. Use 0 to wait indefinitely."
    ),
)
@click.option(
    "--max-iterations",
    default=0,
    show_default=True,
    type=click.IntRange(0, None),
    help="Maximum loop iterations to execute. Use 0 to run indefinitely.",
)
@click.option(
    "--ignore-trigger-state/--respect-trigger-state",
    default=False,
    show_default=True,
    help=(
        "Run the pipeline even if the ingest-compute-week trigger checkbox is not "
        "checked."
    ),
)
def loop(
    config,
    max_errors,
    trigger_timeout,
    max_iterations,
    ignore_trigger_state,
):
    """Continuously ingest and compute outputs for the configured spreadsheet."""

    log = setup_logging()
    iteration = 0
    consecutive_errors = 0

    try:
        while True:
            if max_iterations > 0 and iteration >= max_iterations:
                log.info(
                    "max iterations reached, stopping loop",
                    extra={"stage": "loop", "iteration": iteration},
                )
                break
            iteration += 1
            log.info(
                "loop iteration started",
                extra={"stage": "loop", "iteration": iteration},
            )

            settings = None
            should_run = False
            trigger_range: str | None = None
            trigger_client: SheetsClient | None = None
            try:
                settings = load_settings(config)
                trigger_range = _require_ingest_trigger_range(settings)

                trigger_client = SheetsClient(settings.service_account_json)
                if ignore_trigger_state:
                    should_run = True
                    log.info(
                        "trigger state ignored; running pipeline",
                        extra={"stage": "loop", "iteration": iteration},
                    )
                else:
                    should_run = _wait_for_ingest_trigger(
                        settings,
                        log,
                        trigger_timeout,
                        iteration,
                        client=trigger_client,
                        trigger_range=trigger_range,
                    )
                if not should_run:
                    consecutive_errors = 0
                    log.info(
                        "loop iteration skipped",
                        extra={
                            "stage": "loop",
                            "iteration": iteration,
                            "reason": "trigger_timeout",
                        },
                    )
                else:
                    run_pipeline(settings, log)
                    consecutive_errors = 0
            except click.ClickException:
                raise
            except Exception:
                consecutive_errors += 1
                log.error(
                    "loop iteration failed",
                    extra={
                        "stage": "loop",
                        "iteration": iteration,
                        "consecutive_errors": consecutive_errors,
                    },
                    exc_info=True,
                )
                if max_errors > 0 and consecutive_errors >= max_errors:
                    log.error(
                        "max consecutive errors reached, stopping loop",
                        extra={
                            "stage": "loop",
                            "iteration": iteration,
                            "consecutive_errors": consecutive_errors,
                        },
                    )
                    break
            finally:
                if (
                    should_run
                    and settings is not None
                    and trigger_range is not None
                    and trigger_client is not None
                ):
                    try:
                        _set_ingest_trigger_checkbox(
                            settings,
                            False,
                            client=trigger_client,
                            trigger_range=trigger_range,
                        )
                    except Exception:
                        log.warning(
                            "failed to reset ingest-compute-week trigger",
                            extra={"stage": "loop", "iteration": iteration},
                            exc_info=True,
                        )

    except KeyboardInterrupt:
        log.info(
            "loop interrupted by user",
            extra={"stage": "loop", "iteration": iteration},
        )


def main():
    cli()


if __name__ == "__main__":
    main()
