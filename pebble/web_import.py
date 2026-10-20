"""One-time import of existing Sheets inputs into a web-managed season."""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation

from dateutil import parser

from .cli import _parse_availability_value
from .config_loader import load_settings
from .ingest import WOW_CLASS_COLORS, _extract_code_from_url, _sheet_values_batch
from .sheets_client import SheetsClient
from .utils.time import PT, pt_time_to_ms, sheets_date_str
from .web_data import import_domain
from .web_models import wall_ms


def sheet_day(value) -> str:
    """Accept formatted legacy dates and numeric Sheets serial dates explicitly."""
    if value is None or str(value).strip() == "":
        return ""
    try:
        serial = Decimal(str(value))
    except InvalidOperation:
        result = sheets_date_str(str(value))
    else:
        if not serial.is_finite() or not 1 <= serial <= 100000:
            raise ValueError("Invalid Sheets serial date")
        result = (datetime(1899, 12, 30) + timedelta(days=float(serial))).strftime(
            "%Y-%m-%d"
        )
    if not result:
        raise ValueError("Invalid Sheets date")
    return result


def sheet_time(value, anchor):
    """Parse formatted wall times or a numeric fraction of a Sheets day."""
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        text = str(value)
    else:
        if not number.is_finite() or not 0 <= number < 1:
            raise ValueError("Invalid Sheets time fraction")
        minute = number * 1440
        if abs(minute - minute.to_integral_value()) > Decimal("0.00001"):
            raise ValueError("Timing overrides require whole-minute precision")
        minutes = int(minute.to_integral_value())
        text = f"{minutes // 60:02d}:{minutes % 60:02d}"
    # Validate the requested wall time before timestamp conversion can normalize
    # a nonexistent spring-forward time into a different valid clock reading.
    parsed = parser.parse(
        text, default=datetime.fromtimestamp(anchor / 1000, PT).replace(tzinfo=None)
    )
    local = parsed.astimezone(PT) if parsed.tzinfo else parsed
    wall_ms(local.strftime("%Y-%m-%dT%H:%M"))
    ms = pt_time_to_ms(text, anchor)
    if ms is not None and ms % 60_000:
        raise ValueError("Timing overrides require whole-minute precision")
    return ms


def sheet_availability(value):
    text = str(value).strip()
    try:
        number = Decimal(text)
    except InvalidOperation:
        pass
    else:
        if not number.is_finite() or number != number.to_integral_value():
            raise ValueError("Availability minutes must be whole numbers")
        text = str(int(number))
    parsed = _parse_availability_value(text)
    if parsed is None and text.lower() not in ("", "-", "na"):
        raise ValueError(
            "Availability value must be automatic, a boolean or whole signed minutes"
        )
    if parsed is None:
        return {"mode": "automatic"}
    if isinstance(parsed, bool):
        return {"mode": "available" if parsed else "unavailable"}
    return {
        "mode": "partial" if parsed > 0 else "unavailableFor",
        "minutes": abs(parsed),
    }


def import_sheets(
    client, season_id: str, config_path: str, credentials_path: str
) -> dict:
    """Copy missing domain inputs and current calculation settings, without running."""
    sheets = SheetsClient(credentials_path)
    settings = load_settings(config_path, sheets_client=sheets)
    tabs, starts = settings.sheets.tabs, settings.sheets.starts
    rows = _sheet_values_batch(
        settings,
        [
            (key, getattr(tabs, key), getattr(starts, key))
            for key in [
                "reports",
                "team_roster",
                "roster_map",
                "availability_overrides",
            ]
        ],
        client=sheets,
    )

    def records(key):
        values = rows[key]
        return [dict(zip(values[0], row)) for row in values[1:]] if values else []

    domain = {
        "players": {},
        "characters": {},
        "reports": {},
        "nightOverrides": {},
        "availabilityOverrides": {},
    }
    mains = {}
    labels = {"DEATHKNIGHT": "Death Knight", "DEMONHUNTER": "Demon Hunter"}
    colors = {
        color.upper(): labels.get(name, name.title())
        for name, color in WOW_CLASS_COLORS.items()
    }
    for row in records("team_roster"):
        name = str(row.get("Main", "")).strip()
        if not name:
            if any(str(value).strip() for value in row.values()):
                raise ValueError("Roster row requires a main name")
            continue
        pid = "player_" + hashlib.sha256(name.casefold().encode()).hexdigest()[:16]
        if name.casefold() in mains:
            raise ValueError("Duplicate roster main")
        mains[name.casefold()] = pid
        color = str(row.get("Class Color", "")).upper().strip()
        domain["players"][pid] = {
            "name": name,
            "className": colors.get(color, "Unknown"),
            "role": "Unknown",
            "active": str(row.get("Active?", "")).lower().strip()
            not in ("false", "f", "n", "no", "0"),
            "joinDate": sheet_day(row.get("Join Date", "")) or "",
            "leaveDate": sheet_day(row.get("Leave Date", "")) or "",
            "notes": str(row.get("Notes", "")),
        }
    for row in records("roster_map"):
        alias, main = str(row.get("Alt", "")).strip(), str(row.get("Main", "")).strip()
        if not alias and not main:
            continue
        if not alias or main.casefold() not in mains:
            raise ValueError("Character mapping has a missing main reference")
        name, separator, realm = alias.partition("-")
        cid = "character_" + hashlib.sha256(alias.casefold().encode()).hexdigest()[:16]
        if cid in domain["characters"]:
            raise ValueError("Duplicate character mapping")
        # An empty realm preserves an intentional name-only alias within US.
        domain["characters"][cid] = {
            "name": name,
            "realm": realm if separator else "",
            "region": "us",
            "playerId": mains[main.casefold()],
        }
    for row in records("reports"):
        code = _extract_code_from_url(str(row.get("Report URL", "")).strip())
        if not code:
            if any(str(value).strip() for value in row.values()):
                raise ValueError("Report row has an invalid or missing report code")
            continue
        status = str(row.get("Status", "")).lower().strip()
        if code in domain["reports"]:
            raise ValueError("Duplicate report row")
        domain["reports"][code] = {
            "enabled": status not in ("bad report link", "disabled", "excluded"),
            "notes": str(row.get("Notes", "")),
        }
        night = sheet_day(row.get("Report Start (PT)", ""))
        if not night:
            if any(
                row.get(column)
                for column in (
                    "Mythic Override Start (PT)",
                    "Mythic Override End (PT)",
                    "Break Override Start (PT)",
                    "Break Override End (PT)",
                )
            ):
                raise ValueError("Timing override requires a valid report date")
            continue
        anchor = int(
            datetime.fromisoformat(night).replace(tzinfo=PT).timestamp() * 1000
        )
        timing = {}
        for key, column in [
            ("start", "Mythic Override Start (PT)"),
            ("end", "Mythic Override End (PT)"),
            ("breakStart", "Break Override Start (PT)"),
            ("breakEnd", "Break Override End (PT)"),
        ]:
            value = row.get(column, "")
            ms = (
                sheet_time(value, anchor) if value != "" and value is not None else None
            )
            if value and ms is None:
                raise ValueError("Report timing override could not be parsed")
            timing[key] = (
                datetime.fromtimestamp(ms / 1000, PT).isoformat()[:16]
                if ms is not None
                else ""
            )
        if any(timing.values()):
            if (
                night in domain["nightOverrides"]
                and domain["nightOverrides"][night] != timing
            ):
                raise ValueError(
                    f"Conflicting report timing overrides on {night}; consolidate them before importing"
                )
            domain["nightOverrides"][night] = timing
    for row in records("availability_overrides"):
        night, main = (
            sheet_day(row.get("Night", "")),
            str(row.get("Main", "")).strip(),
        )
        pid = mains.get(main.casefold())
        if not any(str(value).strip() for value in row.values()):
            continue
        if not night or not pid:
            raise ValueError(
                "Availability override has an invalid night or missing main reference"
            )

        if f"{night}_{pid}" in domain["availabilityOverrides"]:
            raise ValueError("Duplicate availability override")
        domain["availabilityOverrides"][f"{night}_{pid}"] = {
            "nightId": night,
            "playerId": pid,
            "pre": sheet_availability(row.get("Avail Pre?", "")),
            "post": sheet_availability(row.get("Avail Post?", "")),
            "notes": str(row.get("Reason", "")),
        }
    cfg = settings.time
    bw = cfg.break_window
    calculation = {
        "tz": cfg.tz,
        "breakWindowStart": parser.parse(bw.start_pt).strftime("%H:%M"),
        "breakWindowEnd": parser.parse(bw.end_pt).strftime("%H:%M"),
        "minBreakMinutes": bw.min_gap_minutes,
        "maxBreakMinutes": bw.max_gap_minutes,
        "defaultMythicStart": (
            parser.parse(cfg.mythic_default_start_pt).strftime("%H:%M")
            if cfg.mythic_default_start_pt
            else ""
        ),
        "postExtensionMinutes": cfg.mythic_post_extension_min,
    }
    return import_domain(client, season_id, domain, calculation)
