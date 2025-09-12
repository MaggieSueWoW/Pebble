from __future__ import annotations
from typing import List

from .sheets_client import SheetsClient


def replace_values(
    spreadsheet_id: str, tab: str, values: List[List], creds_path: str
) -> None:
    client = SheetsClient(creds_path)
    svc = client.svc
    rng = f"{tab}!A1"
    body = {"values": values, "majorDimension": "ROWS"}
    client.execute(
        svc.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id, range=f"{tab}!A:Z"
        )
    )
    client.execute(
        svc.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=rng, valueInputOption="RAW", body=body
        )
    )

