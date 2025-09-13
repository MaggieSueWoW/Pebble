from __future__ import annotations
from pymongo import MongoClient, ASCENDING
from .config_loader import Settings


def get_client(s: Settings) -> MongoClient:
    return MongoClient(s.mongo.uri)


def get_db(s: Settings):
    return get_client(s)[s.mongo.db]


def ensure_indexes(db) -> None:
    # reports (one per WCL report)
    db["reports"].create_index([("code", ASCENDING)], unique=True)
    db["reports"].create_index([("night_id", ASCENDING)])

    # fights (all difficulties), one doc per fight, participants embedded
    # Canonical key dedupes fights across overlapping reports
    db["fights_all"].create_index(
        [
            ("encounter_id", ASCENDING),
            ("difficulty", ASCENDING),
            ("start_rounded_ms", ASCENDING),
            ("end_rounded_ms", ASCENDING),
        ],
        unique=True,
    )
    # still index by report and fight id for lookups (non‑unique)
    db["fights_all"].create_index([("report_code", ASCENDING), ("id", ASCENDING)])
    db["fights_all"].create_index([("night_id", ASCENDING)])
    db["fights_all"].create_index([("is_mythic", ASCENDING), ("night_id", ASCENDING)])

    # participation rows per Mythic fight
    db["participation_m"].create_index(
        [
            ("night_id", ASCENDING),
            ("report_code", ASCENDING),
            ("fight_id", ASCENDING),
            ("main", ASCENDING),
        ],
        unique=True,
    )
    db["participation_m"].create_index([("night_id", ASCENDING)])

    # contiguous blocks of participation
    db["blocks"].create_index(
        [
            ("night_id", ASCENDING),
            ("main", ASCENDING),
            ("half", ASCENDING),
            ("block_seq", ASCENDING),
        ],
        unique=True,
    )
    db["blocks"].create_index([("night_id", ASCENDING)])

    # optional actor cache per report (kept small; useful for audits)
    db["actors"].create_index(
        [("report_code", ASCENDING), ("actor_id", ASCENDING)], unique=True
    )

    # results
    db["night_qa"].create_index([("night_id", ASCENDING)], unique=True)
    db["bench_night_totals"].create_index(
        [("night_id", ASCENDING), ("main", ASCENDING)], unique=True
    )
    db["bench_week_totals"].create_index(
        [("game_week", ASCENDING), ("main", ASCENDING)], unique=True
    )
    db["bench_rankings"].create_index([("main", ASCENDING)], unique=True)
    db["team_roster"].create_index([("main", ASCENDING)], unique=True)
    db["service_log"].create_index([("ts", ASCENDING)])
