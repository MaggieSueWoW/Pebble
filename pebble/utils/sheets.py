from __future__ import annotations

from datetime import datetime

from ..sheets_client import SheetsClient
from .time import PT, ms_to_pt_sheets


def update_last_processed(
    spreadsheet_id: str,
    tab: str,
    cell: str = "B3",
    *,
    client: SheetsClient,
) -> None:
    """Write the current PT datetime to ``cell`` on ``tab``.

    The datetime is formatted so Google Sheets parses it as a proper datetime.
    """
    svc = client.svc
    now_ms = int(datetime.now(tz=PT).timestamp() * 1000)
    body = {"values": [[ms_to_pt_sheets(now_ms)]], "majorDimension": "ROWS"}
    rng = f"{tab}!{cell}"
    client.execute(
        svc.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=rng,
            valueInputOption="USER_ENTERED",
            body=body,
        )
    )


def parse_tab_cell(tab_cell: str) -> tuple[str | None, str]:
    """Split a fully-qualified cell like ``"Tab!C3"`` into its parts.

    Returns a tuple of ``(tab, cell)`` where ``tab`` may be ``None`` when no tab
    prefix is present.
    """

    if "!" not in tab_cell:
        return None, tab_cell

    tab, cell = tab_cell.split("!", 1)
    tab = tab.strip()
    cell = cell.strip()

    if not cell:
        raise ValueError(f"Invalid tab/cell reference: {tab_cell}")

    return (tab or None, cell)
