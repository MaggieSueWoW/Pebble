from __future__ import annotations
from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection
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
    db["fights"].create_index([("report_code", ASCENDING), ("id", ASCENDING)], unique=True)
    db["fights"].create_index([("night_id", ASCENDING)])
    db["fights"].create_index([("is_mythic", ASCENDING), ("night_id", ASCENDING)])

    # optional actor cache per report (kept small; useful for audits)
    db["actors"].create_index([("report_code", ASCENDING), ("actor_id", ASCENDING)], unique=True)

    # results
    db["night_qa"].create_index([("night_id", ASCENDING)], unique=True)
    db["bench_night_totals"].create_index([("night_id", ASCENDING), ("main", ASCENDING)], unique=True)
    db["bench_week_totals"].create_index([("game_week", ASCENDING), ("main", ASCENDING)], unique=True)
    db["service_log"].create_index([("ts", ASCENDING)])
