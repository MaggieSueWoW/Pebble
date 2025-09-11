from __future__ import annotations
from datetime import datetime, timezone
import zoneinfo

PT = zoneinfo.ZoneInfo("America/Los_Angeles")


def ms_to_dt_utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def utc_to_pt(dt_utc: datetime) -> datetime:
    return dt_utc.astimezone(PT)


def ms_to_pt(ms: int) -> datetime:
    return utc_to_pt(ms_to_dt_utc(ms))


def night_id_from_ms(ms: int) -> str:
    # Night ID = local PT calendar date (YYYY-MM-DD) of the night start
    return ms_to_pt(ms).strftime("%Y-%m-%d")
