import click
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Sequence, Union

from .config_loader import (
    Settings,
    get_cached_settings,
    load_settings,
    load_settings_entry,
)
from .logging_setup import setup_logging
from .mongo_client import get_db, ensure_indexes
from .ingest import ingest_reports, ingest_roster, _sheet_values_batch
from .export_sheets import build_replace_values_requests, build_value_update_requests
from .sheets_client import SheetsClient
from .attendance import build_attendance_rows
from .pipeline import compute_results
from .utils.sheets import parse_tab_cell
from .utils.time import sheets_date_str
from .utils.names import NameResolver


def _require_ingest_trigger_range(settings) -> str:
    try:
        trigger_range = settings.sheets.triggers.ingest_compute_week
    except AttributeError as exc:
        raise click.ClickException("sheets.triggers.ingest_compute_week must be configured") from exc
    if not trigger_range:
        raise click.ClickException("sheets.triggers.ingest_compute_week must be configured")
    tab, cell = parse_tab_cell(trigger_range)
    if tab is None or not cell:
        raise click.ClickException(
            "sheets.triggers.ingest_compute_week must include a tab and cell reference"
        )
    return trigger_range


def _read_ingest_trigger_checkbox(
    settings,
    *,
    client: SheetsClient,
    trigger_range: str | None = None,
    prefetched_values: list[list[Any]] | None = None,
) -> bool:
    rng = trigger_range or _require_ingest_trigger_range(settings)
    if prefetched_values is not None:
        values = prefetched_values
    else:
        svc = client.svc
        resp = client.execute(
            svc.spreadsheets().values().get(spreadsheetId=settings.sheets.spreadsheet_id, range=rng)
        )
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
    prefetched_values: list[list[Any]] | None = None,
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
    if _read_ingest_trigger_checkbox(
        settings,
        client=client,
        trigger_range=rng,
        prefetched_values=prefetched_values,
    ):
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


@cli.command("ensure-indexes", help="Ensure MongoDB indexes are created.")
@click.option("--config", default="config.yaml", show_default=True)
def ensure_indexes_cmd(config):
    log = setup_logging()
    s = load_settings(config)
    db = get_db(s)
    ensure_indexes(db)
    log.info("indexes ensured", extra={"stage": "ensure-indexes"})
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


def _pipeline_sheet_requests(settings: Settings) -> list[tuple[str, str, str]]:
    trigger_tab, trigger_cell = parse_tab_cell(_require_ingest_trigger_range(settings))

    return [
        (
            "reports",
            settings.sheets.tabs.reports,
            settings.sheets.starts.reports,
        ),
        (
            "team_roster",
            settings.sheets.tabs.team_roster,
            settings.sheets.starts.team_roster,
        ),
        (
            "roster_map",
            settings.sheets.tabs.roster_map,
            settings.sheets.starts.roster_map,
        ),
        (
            "availability_overrides",
            settings.sheets.tabs.availability_overrides,
            settings.sheets.starts.availability_overrides,
        ),
        (
            "attendance_header",
            settings.sheets.tabs.attendance,
            settings.sheets.starts.attendance,
        ),
        ("ingest_trigger", trigger_tab or "", trigger_cell or ""),
    ]


def _load_settings_and_pipeline_values(
    config_path: str,
) -> tuple[Settings, SheetsClient, dict[str, list[list[Any]]]]:
    cached_entry = get_cached_settings(config_path)

    if cached_entry is None:
        entry = load_settings_entry(config_path)
        settings = entry.settings
        sheet_client = SheetsClient(settings.service_account_json)
        sheet_values = _sheet_values_batch(
            settings, _pipeline_sheet_requests(settings), client=sheet_client
        )
        return settings, sheet_client, sheet_values

    settings = cached_entry.settings
    sheet_client = SheetsClient(settings.service_account_json)
    sheet_requests = _pipeline_sheet_requests(settings)
    sheet_ranges = [f"{tab}!{start}:Z" for _, tab, start in sheet_requests]
    ranges = cached_entry.ranges + sheet_ranges

    response = sheet_client.execute(
        sheet_client.svc.spreadsheets()
        .values()
        .batchGet(spreadsheetId=settings.sheets.spreadsheet_id, ranges=ranges)
    )
    value_ranges = response.get("valueRanges", [])

    expected_ranges = len(ranges)
    if len(value_ranges) != expected_ranges:
        raise ValueError("Unexpected response when reading pipeline sheets")

    settings_value_ranges = value_ranges[: len(cached_entry.ranges)]
    data_value_ranges = value_ranges[len(cached_entry.ranges) :]

    fresh_entry = load_settings_entry(
        config_path,
        sheets_client=sheet_client,
        settings_value_ranges=settings_value_ranges,
        update_cache=False,
    )

    if fresh_entry.values != cached_entry.values:
        load_settings_entry(
            config_path,
            sheets_client=sheet_client,
            settings_value_ranges=settings_value_ranges,
            update_cache=True,
        )
        updated_settings = fresh_entry.settings
        sheet_values = _sheet_values_batch(
            updated_settings,
            _pipeline_sheet_requests(updated_settings),
            client=sheet_client,
        )
        return updated_settings, sheet_client, sheet_values

    sheet_values = _sheet_values_batch(
        settings,
        sheet_requests,
        client=sheet_client,
        prefetched_value_ranges=data_value_ranges,
    )
    return settings, sheet_client, sheet_values


def run_pipeline(
    settings,
    log,
    force_full_reingest: bool = False,
    *,
    sheet_values: dict[str, list[list[Any]]] | None = None,
    sheet_client: SheetsClient | None = None,
):
    """Ingest reports, compute nightly tables, and refresh weekly exports."""

    s = settings
    sheet_client = sheet_client or SheetsClient(s.service_account_json)
    sheet_values = sheet_values or _sheet_values_batch(
        s,
        _pipeline_sheet_requests(s),
        client=sheet_client,
    )

    report_res = ingest_reports(
        settings,
        rows=sheet_values.get("reports", []),
        client=sheet_client,
        force_full_reingest=force_full_reingest,
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

    roster_docs = list(db["team_roster"].find({}, {"_id": 0}))
    resolver = NameResolver(
        [r["main"] for r in roster_docs if r.get("active", True)], roster_map
    )
    overrides, unmatched = parse_availability_overrides(
        sheet_values.get("availability_overrides", []), resolver
    )
    computed = compute_results(
        s, db, log, roster_map=roster_map,
        overrides_by_night=overrides, overrides_unmatched=unmatched,
    )
    night_qa_rows, bench_rows = computed.night_qa_rows, computed.bench_rows
    weeks_written, week_total_docs = computed.weeks_written, computed.week_total_docs
    ranks_written, ranking_docs = computed.ranks_written, computed.ranking_docs

    sheet_requests: list[dict] = []

    def queue_sheet_write(
        tab: str,
        values: list[list],
        *,
        start_cell: str,
        last_processed_cell: str | None = None,
        last_processed_tab: str | None = None,
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
                last_processed_tab=last_processed_tab,
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

    log.info("compute complete", extra={"stage": "compute", "nights": len(night_qa_rows) - 1})

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
    for rec in sorted(week_total_docs, key=lambda r: (r["game_week"], r["main"])):
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
    for rec in sorted(ranking_docs, key=lambda r: r["rank"]):
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

    last_processed_tab, last_processed_cell = parse_tab_cell(
        settings.sheets.last_processed
    )
    last_processed_tab = last_processed_tab or settings.sheets.tabs.bench_rankings
    queue_sheet_write(
        settings.sheets.tabs.bench_rankings,
        rank_rows,
        start_cell=settings.sheets.starts.bench_rankings,
        last_processed_cell=last_processed_cell,
        last_processed_tab=last_processed_tab,
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

    log.info(
        "week export complete",
        extra={
            "stage": "week",
            "totals_updated": weeks_written,
            "rankings_updated": ranks_written,
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
@click.option(
    "--force-full-reingest",
    is_flag=True,
    default=False,
    show_default=True,
    help="Force ingest of all reports even if they were previously ingested.",
)
def loop(
    config,
    max_errors,
    trigger_timeout,
    max_iterations,
    ignore_trigger_state,
    force_full_reingest,
):
    """Continuously ingest and compute outputs for the configured spreadsheet."""

    log = setup_logging()
    if force_full_reingest:
        log.info(
            "forcing full report reingest for each loop iteration",
            extra={"stage": "loop"},
        )
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
            sheet_client: SheetsClient | None = None
            sheet_values: dict[str, list[list[Any]]] | None = None
            should_run = False
            trigger_range: str | None = None
            trigger_client: SheetsClient | None = None
            try:
                settings, sheet_client, sheet_values = _load_settings_and_pipeline_values(
                    config
                )
                trigger_range = _require_ingest_trigger_range(settings)

                trigger_client = sheet_client or SheetsClient(settings.service_account_json)
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
                        prefetched_values=sheet_values.get("ingest_trigger"),
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
                    run_pipeline(
                        settings,
                        log,
                        force_full_reingest=force_full_reingest,
                        sheet_values=sheet_values,
                        sheet_client=sheet_client,
                    )
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


from .web_cli import web

cli.add_command(web)


def main():
    cli()


if __name__ == "__main__":
    main()
