"""One-time import of existing Sheets inputs into a web-managed season."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

from dateutil import parser
from google.cloud import firestore

from .cli import _parse_availability_value
from .config_loader import load_settings
from .ingest import WOW_CLASS_COLORS, _extract_code_from_url, _sheet_values_batch
from .sheets_client import SheetsClient
from .utils.time import PT, pt_time_to_ms, sheets_date_str


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
            continue
        pid = "player_" + hashlib.sha256(name.casefold().encode()).hexdigest()[:16]
        mains[name.casefold()] = pid
        color = str(row.get("Class Color", "")).upper().strip()
        domain["players"][pid] = {
            "name": name,
            "className": colors.get(color, "Unknown"),
            "role": "Unknown",
            "active": str(row.get("Active?", "")).lower().strip()
            not in ("false", "n", "no", "0"),
            "joinDate": sheets_date_str(row.get("Join Date", "")) or "",
            "leaveDate": sheets_date_str(row.get("Leave Date", "")) or "",
            "notes": str(row.get("Notes", ""))[:1000],
        }
    for row in records("roster_map"):
        alias, main = str(row.get("Alt", "")).strip(), str(row.get("Main", "")).strip()
        if not alias or main.casefold() not in mains:
            continue
        name, separator, realm = alias.partition("-")
        cid = "character_" + hashlib.sha256(alias.casefold().encode()).hexdigest()[:16]
        # An empty realm preserves legacy name-only aliases; worker adds a short alias.
        domain["characters"][cid] = {
            "name": name,
            "realm": realm if separator else "",
            "region": "us",
            "playerId": mains[main.casefold()],
        }
    for row in records("reports"):
        code = _extract_code_from_url(str(row.get("Report URL", "")).strip())
        if not code:
            continue
        status = str(row.get("Status", "")).lower().strip()
        domain["reports"][code] = {
            "enabled": status not in ("bad report link", "disabled", "excluded"),
            "notes": str(row.get("Notes", ""))[:1000],
        }
        night = sheets_date_str(row.get("Report Start (PT)", ""))
        if not night:
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
            ms = pt_time_to_ms(str(value), anchor) if value else None
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
            sheets_date_str(row.get("Night", "")),
            str(row.get("Main", "")).strip(),
        )
        pid = mains.get(main.casefold())
        if not night or not pid:
            continue

        def availability(value):
            parsed = _parse_availability_value(str(value))
            if parsed is None:
                return {"mode": "automatic"}
            if isinstance(parsed, bool):
                return {"mode": "available" if parsed else "unavailable"}
            return {
                "mode": "partial" if parsed > 0 else "unavailableFor",
                "minutes": abs(parsed),
            }

        domain["availabilityOverrides"][f"{night}_{pid}"] = {
            "nightId": night,
            "playerId": pid,
            "pre": availability(row.get("Avail Pre?", "")),
            "post": availability(row.get("Avail Post?", "")),
            "notes": str(row.get("Reason", ""))[:1000],
        }
    from .web_models import validate_snapshot

    meta = {
        "version": 1,
        "updatedBy": "local-admin",
        "updatedAt": datetime.now(timezone.utc),
    }
    validated = {
        name: {key: {**value, **meta} for key, value in values.items()}
        for name, values in domain.items()
    }
    validate_snapshot(validated)
    counts = {}
    season = client.document(f"seasons/{season_id}")
    for name, values in validated.items():
        counts[name] = 0
        for key, value in values.items():
            ref = season.collection(name).document(key)
            if not ref.get().exists:
                ref.create(value)
                counts[name] += 1
    cfg = settings.time
    bw = cfg.break_window
    ref = season.collection("settings").document("calculation")
    current = ref.get().to_dict() or {}
    ref.set(
        {
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
            "version": current.get("version", 0) + 1,
            "updatedBy": "local-admin",
            "updatedAt": firestore.SERVER_TIMESTAMP,
        }
    )
    return counts
