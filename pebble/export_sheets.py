from __future__ import annotations
from typing import List
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


def _svc(creds_path: str):
    creds = Credentials.from_service_account_file(creds_path, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets", "v4", credentials=creds)


def replace_values(spreadsheet_id: str, tab: str, values: List[List], creds_path: str) -> None:
    svc = _svc(creds_path)
    rng = f"{tab}!A1"
    body = {"values": values, "majorDimension": "ROWS"}
    svc.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=f"{tab}!A:Z").execute()
    svc.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=rng, valueInputOption="RAW", body=body).execute()
