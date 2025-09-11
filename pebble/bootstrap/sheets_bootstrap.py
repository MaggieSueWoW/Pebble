from __future__ import annotations
from typing import List, Dict, Any
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from ..config_loader import Settings

HEADERS = {
    "Control & Ingest": [
        "Report URL",
        "Report Code",
        "Status",
        "Last Checked PT",
        "Notes",
        "Break Override Start (PT)",
        "Break Override End (PT)",
        "Poll Seconds",
        "Min Break (min)",
        "Max Break (min)",
        "Break Window Start (PT)",
        "Break Window End (PT)",
    ],
    "Roster Map": ["Character (Name-Realm)", "Main (Name-Realm)", "Role"],
    "Team Roster": [
        "Main",
        "Join Night (YYYY-MM-DD)",
        "Leave Night (YYYY-MM-DD)",
        "Active?",
        "Notes",
    ],
    "Availability Overrides": [
        "Night ID",
        "Main",
        "Status",
        "Avail Pre?",
        "Avail Post?",
        "Reason",
    ],
    "Night QA": [
        "Night ID",
        "Reports Involved",
        "Night Start (PT)",
        "Night End (PT)",
        "Break Start (PT)",
        "Break End (PT)",
        "Break Duration (min)",
        "Mythic Start (PT)",
        "Mythic End (PT)",
        "Mythic Pre (min)",
        "Mythic Post (min)",
        "Gap Window",
        "Min/Max Break",
        "Largest Gap (min)",
        "Candidate Gaps (JSON)",
        "Override Used?",
    ],
    "Bench Night Totals": [
        "Night ID",
        "Main",
        "Role",
        "Played Pre (min)",
        "Played Post (min)",
        "Played Total (min)",
        "Bench Pre (min)",
        "Bench Post (min)",
        "Bench Total (min)",
        "Avail Pre?",
        "Avail Post?",
        "Status Source",
    ],
    "Bench Week Totals": [
        "Game Week (YYYY-MM-DD)",
        "Main",
        "Role",
        "Played Week (min)",
        "Bench Week (min)",
        "Bench Pre (min)",
        "Bench Post (min)",
    ],
    "Service Log (Summary)": [
        "Timestamp PT",
        "Stage",
        "Night ID",
        "Message",
        "Counts JSON",
        "Level",
    ],
}


def _svc(settings: Settings):
    creds = Credentials.from_service_account_file(
        settings.google.service_account_json_path,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds)


def _get_sheet_names(svc, sheet_id: str) -> List[str]:
    meta = svc.spreadsheets().get(spreadsheetId=sheet_id).execute()
    return [s["properties"]["title"] for s in meta.get("sheets", [])]


def _ensure_tab(svc, sheet_id: str, name: str):
    existing = _get_sheet_names(svc, sheet_id)
    if name in existing:
        return False
    svc.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body={"requests": [{"addSheet": {"properties": {"title": name}}}]},
    ).execute()
    return True


def _ensure_headers(svc, sheet_id: str, name: str, headers: list[str]):
    rng = f"'{name}'!1:1"
    svc.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=rng,
        valueInputOption="RAW",
        body={"values": [headers]},
    ).execute()


def bootstrap_sheets(settings: Settings) -> Dict[str, Any]:
    svc = _svc(settings)
    desired = {
        settings.app.sheets.control: "Control & Ingest",
        settings.app.sheets.roster_map: "Roster Map",
        settings.app.sheets.team_roster: "Team Roster",
        settings.app.sheets.availability_overrides: "Availability Overrides",
        settings.app.sheets.night_qa: "Night QA",
        settings.app.sheets.bench_night_totals: "Bench Night Totals",
        settings.app.sheets.bench_week_totals: "Bench Week Totals",
        settings.app.sheets.service_log: "Service Log (Summary)",
    }
    tabs = []
    for name, canonical in desired.items():
        _ensure_tab(svc, settings.app.sheet_id, name)
        _ensure_headers(svc, settings.app.sheet_id, name, HEADERS[canonical])
        tabs.append(name)
    return {"ok": True, "tabs": tabs}
