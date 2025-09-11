import click
from .config_loader import load_settings
from .logging_setup import setup_logging
from .mongo_client import get_db, ensure_indexes
from .ingest import ingest_reports
from .envelope import mythic_envelope, split_pre_post
from .breaks import detect_break
from .blocks import build_blocks
from .bench_calc import bench_minutes_for_night
from .participation import build_mythic_participation
from .export_sheets import replace_values


@click.group()
def cli():
    pass


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

    # Night loop: derive QA + bench
    nights = sorted(set([r["night_id"] for r in db["reports"].find({}, {"night_id": 1, "_id": 0})]))

    night_qa_rows = [[
        "Night ID",
        "Mythic Start (ms)",
        "Mythic End (ms)",
        "Break Start (ms)",
        "Break End (ms)",
        "Mythic Pre (min)",
        "Mythic Post (min)",
    ]]
    bench_rows = [[
        "Night ID",
        "Main",
        "Played Pre (min)",
        "Played Post (min)",
        "Bench Pre (min)",
        "Bench Post (min)",
    ]]

    for night in nights:
        fights_all = list(db["fights_all"].find({"night_id": night}, {"_id": 0}))
        fights_m = [f for f in fights_all if f.get("is_mythic")]

        env = mythic_envelope(fights_m)
        if not env:
            continue

        br = detect_break(
            fights_all,
            window_start_min=s.time.break_window_start_min,
            window_end_min=s.time.break_window_end_min,
            min_break_min=s.time.break_min_minutes,
            max_break_min=s.time.break_max_minutes,
        )
        split = split_pre_post(env, br)
        night_qa_rows.append([
            night,
            env[0],
            env[1],
            br[0] if br else "",
            br[1] if br else "",
            (split["pre_ms"] // 60000),
            (split["post_ms"] // 60000),
        ])
        # Persist Night QA to Mongo (idempotent)
        qa_doc = {
            "night_id": night,
            "mythic_start_ms": env[0],
            "mythic_end_ms": env[1],
            "break_start_ms": br[0] if br else None,
            "break_end_ms": br[1] if br else None,
            "mythic_pre_min": (split["pre_ms"] // 60000),
            "mythic_post_min": (split["post_ms"] // 60000),
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
        blocks = build_blocks(part_rows, break_range=br)
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
        bench = bench_minutes_for_night(blocks, split["pre_ms"], split["post_ms"])

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
    replace_values(s.sheets.spreadsheet_id, s.sheets.tabs.night_qa, night_qa_rows, s.service_account_json)
    replace_values(s.sheets.spreadsheet_id, s.sheets.tabs.bench_night_totals, bench_rows, s.service_account_json)

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
    for r in db["bench_week_totals"].find({}, {"_id": 0}).sort([("game_week", 1), ("main", 1)]):
        rows.append([r["game_week"], r["main"], r["played_min"], r["bench_min"]])
    replace_values(s.sheets.spreadsheet_id, s.sheets.tabs.bench_week_totals, rows, s.service_account_json)

    log.info("week export complete", extra={"stage": "week", "rows": n})


def main():
    cli()


if __name__ == "__main__":
    main()
