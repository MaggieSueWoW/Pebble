from __future__ import annotations
import logging
import re
from typing import List

from googleapiclient.errors import HttpError

from .sheets_client import SheetsClient
from .utils.sheets import update_last_processed


logger = logging.getLogger(__name__)


def _col_to_index(col: str) -> int:
    idx = 0
    for char in col.upper():
        idx = idx * 26 + (ord(char) - ord("A") + 1)
    return idx - 1


def _index_to_col(idx: int) -> str:
    idx += 1
    col = ""
    while idx:
        idx, rem = divmod(idx - 1, 26)
        col = chr(ord("A") + rem) + col
    return col


def _split_cell(cell: str) -> tuple[int, int]:
    match = re.match(r"^([A-Za-z]+)(\d+)$", cell or "")
    if not match:
        raise ValueError(f"Invalid cell reference: {cell}")
    col, row = match.groups()
    return _col_to_index(col), int(row)


def _get_sheet_properties(client: SheetsClient, spreadsheet_id: str, tab: str) -> dict | None:
    meta = client.execute(
        client.svc.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields="sheets.properties.sheetId,sheets.properties.title",
        )
    )
    for sheet in meta.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == tab:
            return props
    return None


def _get_header_row(
    client: SheetsClient,
    spreadsheet_id: str,
    tab: str,
    start_cell: str,
) -> List[str]:
    try:
        start_col_idx, start_row = _split_cell(start_cell)
    except ValueError:
        return []

    start_col = _index_to_col(start_col_idx)
    rng = f"{tab}!{start_col}{start_row}:{start_row}"
    resp = client.execute(
        client.svc.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=rng)
    )
    rows = resp.get("values", [])
    if not rows:
        return []
    return rows[0]


def _ensure_table_capacity(
    client: SheetsClient,
    spreadsheet_id: str,
    tab: str,
    start_cell: str,
    values: List[List],
) -> None:
    if not values or not values[0]:
        return

    header = values[0]
    existing_header = _get_header_row(
        client,
        spreadsheet_id,
        tab,
        start_cell,
    )
    if not existing_header:
        return

    additional_columns = len(header) - len(existing_header)
    if additional_columns <= 0:
        return

    try:
        start_col_idx, _ = _split_cell(start_cell)
    except ValueError:
        return

    props = _get_sheet_properties(client, spreadsheet_id, tab)
    if not props:
        return

    sheet_id = props.get("sheetId")
    if sheet_id is None:
        return

    insert_start = start_col_idx + max(len(existing_header) - 1, 0)
    body = {
        "requests": [
            {
                "insertDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": insert_start,
                        "endIndex": insert_start + additional_columns,
                    },
                    "inheritFromBefore": insert_start > 0,
                }
            }
        ]
    }
    client.execute(
        client.svc.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body,
        )
    )


def replace_values(
    spreadsheet_id: str,
    tab: str,
    values: List[List],
    creds_path: str,
    start_cell: str = "A5",
    last_processed_cell: str = "B3",
    ensure_tail_space: bool = False,
) -> None:
    """Replace all values in ``tab`` with ``values``.

    ``USER_ENTERED`` is used so that any date/time strings are parsed by
    Google Sheets and treated as proper datetimes rather than plain text.
    """
    client = SheetsClient(creds_path)
    svc = client.svc

    if ensure_tail_space:
        try:
            _ensure_table_capacity(client, spreadsheet_id, tab, start_cell, values)
        except HttpError:
            raise
        except Exception:
            logger.warning(
                "failed to ensure table capacity", extra={"tab": tab}, exc_info=True
            )
    rng = f"{tab}!{start_cell}"
    body = {"values": values, "majorDimension": "ROWS"}
    client.execute(svc.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=f"{tab}!{start_cell}:Z"))
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
    update_last_processed(
        spreadsheet_id,
        tab,
        creds_path,
        last_processed_cell,
        client,
    )
