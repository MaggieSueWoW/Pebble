from __future__ import annotations
from typing import Any, List, Optional
from datetime import datetime
from pymongo import UpdateOne
import re
from .sheets_client import SheetsClient
from .config_loader import Settings, load_settings
from .mongo_client import get_db, ensure_indexes
from .wcl_client import WCLClient
from .utils.time import night_id_from_ms, ms_to_pt_iso, PT, pt_time_to_ms


REPORT_HEADERS = {
    "Report URL": "report_url",
    "Status": "status",
    "Last Checked PT": "last_checked_pt",
    "Notes": "notes",
    "Break Override Start (PT)": "break_override_start",
    "Break Override End (PT)": "break_override_end",
}


ABS_MS_THRESHOLD = 10**12  # heuristic: anything below this is treated as relative ms


def _extract_code_from_url(url: str | None) -> Optional[str]:
    if not url:
        return None
    try:
        part = url.split("/reports/")[1]
        code = part.split("/")[0].split("?")[0].split("#")[0]
        return code or None
    except Exception:
        return None


def _sheet_values(s: Settings, tab: str, start: str = "A1") -> List[List[Any]]:
    client = SheetsClient(s.service_account_json)
    svc = client.svc
    rng = f"{tab}!{start}:Z"
    return (
        client.execute(
            svc.spreadsheets()
            .values()
            .get(spreadsheetId=s.sheets.spreadsheet_id, range=rng)
        ).get("values", [])
    )


def _normalize_fight_times(
    report_start_ms: int, fight_start: int, fight_end: int
) -> tuple[int, int, int, int]:
    """Return (rel_start, rel_end, abs_start, abs_end) in ms.
    WCL GraphQL fights are *usually* relative to report start; use a robust heuristic.
    """
    fs, fe = int(fight_start or 0), int(fight_end or 0)
    if fs < ABS_MS_THRESHOLD and fe < ABS_MS_THRESHOLD:
        rel_start, rel_end = fs, fe
        abs_start, abs_end = report_start_ms + fs, report_start_ms + fe
    else:
        # appears absolute already
        abs_start, abs_end = fs, fe
        rel_start, rel_end = max(0, fs - report_start_ms), max(0, fe - report_start_ms)
    return rel_start, rel_end, abs_start, abs_end


def canonical_fight_key(
    fight: dict, abs_start_ms: int, abs_end_ms: int
) -> dict:
    """Return canonical key for a fight.

    The key is independent of the report code so that the same pull logged in
    multiple reports maps to a single document in ``fights_all``.  Start and end
    times are rounded to the nearest 100 ms to absorb minor timestamp drift
    while avoiding collisions for distinct fights that occur close together.
    """

    def _round_ms(ms: int) -> int:
        """Round ``ms`` to the nearest 100 ms."""
        return int(round(ms / 100.0) * 100)

    return {
        "encounter_id": int(fight.get("encounterID") or 0),
        "difficulty": int(fight.get("difficulty") or 0),
        "start_rounded_ms": _round_ms(abs_start_ms),
        "end_rounded_ms": _round_ms(abs_end_ms),
    }


def ingest_reports(s: Settings | None = None) -> dict:
    s = s or load_settings()
    db = get_db(s)
    ensure_indexes(db)

    client = SheetsClient(s.service_account_json)
    svc = client.svc
    start = s.sheets.starts.reports
    rng = f"{s.sheets.tabs.reports}!{start}:Z"
    rows = (
        client.execute(
            svc.spreadsheets()
            .values()
            .get(spreadsheetId=s.sheets.spreadsheet_id, range=rng)
        ).get("values", [])
    )
    if not rows:
        return {"reports": 0, "fights": 0}

    header = rows[0]
    colmap = {name: header.index(name) for name in REPORT_HEADERS if name in header}
    last_checked_idx = colmap.get("Last Checked PT")

    # Collect targets
    targets: List[dict] = []
    start_row = int(re.search(r"\d+", start).group()) if re.search(r"\d+", start) else 1
    for r_index, row in enumerate(rows[1:], start=start_row + 1):

        def val(col: str) -> str:
            idx = colmap.get(col)
            return row[idx] if idx is not None and idx < len(row) else ""

        status = val("Status").strip().lower()
        if status not in ("", "in-progress", "in‑progress", "in progress"):
            continue
        code = _extract_code_from_url(val("Report URL").strip())
        if not code:
            continue
        targets.append(
            {
                "row": r_index,
                "code": code,
                "notes": val("Notes"),
                "break_override_start": val("Break Override Start (PT)"),
                "break_override_end": val("Break Override End (PT)"),
            }
        )

    if not targets:
        return {"reports": 0, "fights": 0}

    wcl = WCLClient(
        s.wcl.client_id,
        s.wcl.client_secret,
        base_url=s.wcl.base_url,
        token_url=s.wcl.token_url,
        redis_url=s.redis.url,
        cache_prefix=s.redis.key_prefix,
    )

    total_fights = 0
    updates = []
    for rep in targets:
        code = rep["code"]
        bundle = wcl.fetch_report_bundle(code)

        # reports upsert
        report_start_ms = int(bundle.get("startTime"))
        report_end_ms = int(bundle.get("endTime"))
        night_id = night_id_from_ms(report_start_ms)
        bos_ms = pt_time_to_ms(rep.get("break_override_start"), report_start_ms)
        boe_ms = pt_time_to_ms(rep.get("break_override_end"), report_start_ms)
        now_dt = datetime.now(PT)
        now_ms = int(now_dt.timestamp() * 1000)
        now_iso = ms_to_pt_iso(now_ms)
        rep_doc = {
            "code": code,
            "title": bundle.get("title"),
            "start_ms": report_start_ms,
            "end_ms": report_end_ms,
            "start_pt": ms_to_pt_iso(report_start_ms),
            "end_pt": ms_to_pt_iso(report_end_ms),
            "night_id": night_id,
            "notes": rep.get("notes", ""),
            "break_override_start_ms": bos_ms,
            "break_override_end_ms": boe_ms,
            "break_override_start_pt": (
                ms_to_pt_iso(bos_ms) if bos_ms is not None else ""
            ),
            "break_override_end_pt": ms_to_pt_iso(boe_ms) if boe_ms is not None else "",
            "ingested_at": now_dt,
            "last_checked_pt": now_iso,
        }
        db["reports"].update_one({"code": code}, {"$set": rep_doc}, upsert=True)

        if last_checked_idx is not None:
            col_letter = chr(ord("A") + last_checked_idx)
            rng = f"{s.sheets.tabs.reports}!{col_letter}{rep['row']}"
            updates.append({"range": rng, "values": [[now_iso]]})

        # actors (players) per report — small, useful for audits; dedup by (report_code, actor_id)
        actors = (bundle.get("masterData") or {}).get("actors") or []
        actor_map = {
            int(a.get("id")): {
                "actor_id": int(a.get("id")),
                "name": (
                    f"{a.get('name')}-{a.get('server')}"
                    if a.get("server")
                    else a.get("name")
                ),
                "type": a.get("type"),
                "subType": a.get("subType"),
                "server": a.get("server"),
            }
            for a in actors
        }
        if actor_map:
            ops = []
            for aid, a in actor_map.items():
                key = {"report_code": code, "actor_id": aid}
                ops.append(UpdateOne(key, {"$set": {**key, **a}}, upsert=True))
            if ops:
                db["actors"].bulk_write(ops, ordered=False)

        # fights (single unified collection persisted to ``fights_all``)
        fights = bundle.get("fights", []) or []
        fops = []
        for f in fights:
            rel_s, rel_e, abs_s, abs_e = _normalize_fight_times(
                report_start_ms, f.get("startTime"), f.get("endTime")
            )
            participants = []
            for pid in f.get("friendlyPlayers") or []:
                a = actor_map.get(int(pid))
                if not a:
                    continue
                if str(a.get("type", "")).lower() != "player":
                    continue
                participants.append(
                    {
                        "actor_id": a["actor_id"],
                        "name": a.get("name"),
                        "class": a.get("subType"),  # WoW class
                        "server": a.get("server"),
                    }
                )

            key = canonical_fight_key(f, abs_s, abs_e)
            base = {
                **key,
                "report_code": code,
                "id": int(f.get("id")),
                "night_id": night_id,
                "name": f.get("name"),
                "is_mythic": int(f.get("difficulty") or 0) == 5,
                "kill": bool(f.get("kill")),
                # times
                "report_start_ms": report_start_ms,
                "report_start_pt": ms_to_pt_iso(report_start_ms),
                "fight_rel_start_ms": rel_s,
                "fight_rel_end_ms": rel_e,
                "fight_abs_start_ms": abs_s,
                "fight_abs_start_pt": ms_to_pt_iso(abs_s),
                "fight_abs_end_ms": abs_e,
                "fight_abs_end_pt": ms_to_pt_iso(abs_e),
            }
            # Use $setOnInsert so the first observed report for a given fight
            # establishes the document; subsequent overlapping reports only add
            # participants but do not clobber the original report metadata.
            fops.append(
                UpdateOne(
                    key,
                    {
                        "$setOnInsert": base,
                        "$addToSet": {"participants": {"$each": participants}},
                    },
                    upsert=True,
                )
            )
        if fops:
            db["fights_all"].bulk_write(fops, ordered=False)
        total_fights += len(fights)

    if updates:
        client.execute(
            svc.spreadsheets().values().batchUpdate(
                spreadsheetId=s.sheets.spreadsheet_id,
                body={"valueInputOption": "RAW", "data": updates},
            )
        )

    return {"reports": len(targets), "fights": total_fights}
