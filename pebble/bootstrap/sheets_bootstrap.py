from __future__ import annotations
from typing import List, Dict, Any

from ..sheets_client import SheetsClient
from ..config_loader import Settings

HEADERS = {
    "Reports": [
        "Report URL",
        "Status",
        "Last Checked PT",
        "Notes",
        "Break Override Start (PT)",
        "Break Override End (PT)",
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
        "Report Start (PT)",
        "Report End (PT)",
        "Night Start (PT)",
        "Night End (PT)",
        "Mythic Fights",
        "Break Start (PT)",
        "Break End (PT)",
        "Break Override Start (PT)",
        "Break Override End (PT)",
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
    "Bench Rankings": [
        "Rank",
        "Main",
        "Role",
        "Bench Season-to-date (min)",
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


def _get_sheet_names(client: SheetsClient, sheet_id: str) -> List[str]:
    meta = client.execute(client.svc.spreadsheets().get(spreadsheetId=sheet_id))
    return [s["properties"]["title"] for s in meta.get("sheets", [])]


def _ensure_tab(client: SheetsClient, sheet_id: str, name: str):
    existing = _get_sheet_names(client, sheet_id)
    if name in existing:
        return False
    client.execute(
        client.svc.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": name}}}]},
        )
    )
    return True


def _ensure_headers(
    client: SheetsClient, sheet_id: str, name: str, headers: list[str]
):
    rng = f"'{name}'!1:1"
    client.execute(
        client.svc.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=rng,
            valueInputOption="RAW",
            body={"values": [headers]},
        )
    )


def bootstrap_sheets(settings: Settings) -> Dict[str, Any]:
    client = SheetsClient(settings.service_account_json)
    sheet_id = settings.sheets.spreadsheet_id
    desired = {
        settings.sheets.tabs.reports: "Reports",
        settings.sheets.tabs.roster_map: "Roster Map",
        settings.sheets.tabs.team_roster: "Team Roster",
        settings.sheets.tabs.availability_overrides: "Availability Overrides",
        settings.sheets.tabs.night_qa: "Night QA",
        settings.sheets.tabs.bench_night_totals: "Bench Night Totals",
        settings.sheets.tabs.bench_week_totals: "Bench Week Totals",
        settings.sheets.tabs.bench_rankings: "Bench Rankings",
        settings.sheets.tabs.service_log: "Service Log (Summary)",
    }
    tabs = []
    for name, canonical in desired.items():
        _ensure_tab(client, sheet_id, name)
        _ensure_headers(client, sheet_id, name, HEADERS[canonical])
        tabs.append(name)
    return {"ok": True, "tabs": tabs}
