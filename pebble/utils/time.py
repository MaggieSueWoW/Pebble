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


def ms_to_pt_iso(ms: int) -> str:
    """Return ISO-8601 string of the given epoch ms in PT."""
    return ms_to_pt(ms).isoformat()


def night_id_from_ms(ms: int) -> str:
    # Night ID = local PT calendar date (YYYY-MM-DD) of the night start
    return ms_to_pt(ms).strftime("%Y-%m-%d")


def pt_iso_to_ms(txt: str) -> int | None:
    """Parse an ISO-8601 string presumed to be in PT into epoch ms.

    Returns ``None`` if parsing fails or the input is falsy.
    """
    if not txt:
        return None
    try:
        dt = datetime.fromisoformat(txt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=PT)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def pt_time_to_ms(hhmm: str, ref_ms: int) -> int | None:
    """Convert an ``HH:MM`` PT time to epoch ms using ``ref_ms``'s date.

    ``hhmm`` is interpreted as a 24-hour clock in Pacific Time. The calendar
    date is taken from ``ref_ms`` (also presumed to be in PT). ``None`` is
    returned if parsing fails.
    """
    if not hhmm:
        return None
    try:
        hour, minute = (int(part) for part in hhmm.split(":", 1))
        dt_ref = ms_to_pt(ref_ms)
        dt = dt_ref.replace(hour=hour, minute=minute, second=0, microsecond=0)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None
