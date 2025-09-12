from __future__ import annotations
from datetime import datetime, timezone, timedelta
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


def ms_to_pt_sheets(ms: int) -> str:
    """Return a PT datetime string Google Sheets parses natively.

    The format produced is ``YYYY-MM-DD HH:MM:SS`` so Sheets interprets the
    value as a real datetime rather than plain text.
    """
    return ms_to_pt(ms).strftime("%Y-%m-%d %H:%M:%S")


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


def pt_time_to_ms(txt: str, ref_ms: int) -> int | None:
    """Convert a local PT time string to epoch ms using ``ref_ms``'s date.

    Accepts flexible formats such as ``21:15``, ``9:15 PM`` or ``9:15:00 PM``.
    The returned timestamp is the first occurrence of the parsed time on or
    after ``ref_ms`` (rolling forward in 12 hour increments if needed). ``None``
    is returned if parsing fails or the adjusted time falls more than 24 hours
    after ``ref_ms``.
    """
    if not txt:
        return None

    txt = txt.strip().upper()
    dt_ref = ms_to_pt(ref_ms)

    formats = ["%H:%M", "%H:%M:%S", "%I:%M %p", "%I:%M:%S %p"]
    t = None
    for fmt in formats:
        try:
            t = datetime.strptime(txt, fmt).time()
            break
        except ValueError:
            continue
    if t is None:
        return None

    dt = dt_ref.replace(hour=t.hour, minute=t.minute, second=t.second, microsecond=0)
    while dt.timestamp() * 1000 < ref_ms:
        dt += timedelta(hours=12)
        if dt.timestamp() * 1000 - ref_ms > 24 * 3600 * 1000:
            return None

    if dt.timestamp() * 1000 - ref_ms > 24 * 3600 * 1000:
        return None

    return int(dt.timestamp() * 1000)
