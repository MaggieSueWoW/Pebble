from __future__ import annotations
import logging
import logging
import re
from datetime import datetime
from typing import Dict, List

from googleapiclient.errors import HttpError

from .sheets_client import SheetsClient
from .utils.sheets import update_last_processed
from .utils.time import PT, ms_to_pt_sheets


logger = logging.getLogger(__name__)


def _format_paste_value(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return str(value)


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


_SHEET_PROPERTIES_CACHE: Dict[tuple[int, str], Dict[str, dict]] = {}


def _get_sheet_properties(client: SheetsClient, spreadsheet_id: str, tab: str) -> dict | None:
    cache_key = (id(client), spreadsheet_id)
    cached_props = _SHEET_PROPERTIES_CACHE.get(cache_key)
    if cached_props is None:
        meta = client.execute(
            client.svc.spreadsheets().get(
                spreadsheetId=spreadsheet_id,
                fields="sheets.properties.sheetId,sheets.properties.title",
            )
        )
        cached_props = {}
        for sheet in meta.get("sheets", []):
            props = sheet.get("properties", {})
            title = props.get("title")
            if title:
                cached_props[title] = props
        _SHEET_PROPERTIES_CACHE[cache_key] = cached_props
    return cached_props.get(tab)


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


def _timestamp_for_sheets() -> str:
    now_ms = int(datetime.now(tz=PT).timestamp() * 1000)
    return ms_to_pt_sheets(now_ms)


def build_replace_values_requests(
    spreadsheet_id: str,
    tab: str,
    values: List[List],
    *,
    client: SheetsClient,
    start_cell: str = "A5",
    last_processed_cell: str | None = None,
    ensure_tail_space: bool = False,
    clear_range: bool = True,
    include_last_processed: bool = False,
) -> List[dict]:
    """Build the batchUpdate requests necessary to replace table contents."""

    if ensure_tail_space:
        try:
            _ensure_table_capacity(client, spreadsheet_id, tab, start_cell, values)
        except HttpError:
            raise
        except Exception:
            logger.warning(
                "failed to ensure table capacity", extra={"tab": tab}, exc_info=True
            )

    try:
        start_col_idx, start_row = _split_cell(start_cell)
    except ValueError as exc:
        raise ValueError(f"Invalid start cell for replace_values: {start_cell}") from exc

    props = _get_sheet_properties(client, spreadsheet_id, tab)
    if not props or props.get("sheetId") is None:
        raise ValueError(
            f"Unable to locate sheet '{tab}' in spreadsheet '{spreadsheet_id}'"
        )

    sheet_id = props["sheetId"]
    requests: List[dict] = []

    if clear_range:
        clear_end_col_idx = _col_to_index("Z") + 1
        if start_col_idx >= clear_end_col_idx:
            clear_end_col_idx = start_col_idx + 1
        requests.append(
            {
                "updateCells": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row - 1,
                        "startColumnIndex": start_col_idx,
                        "endColumnIndex": clear_end_col_idx,
                    },
                    "fields": "userEnteredValue",
                }
            }
        )

    if values:
        paste_data = "\n".join(
            "\t".join(_format_paste_value(cell) for cell in row)
            for row in values
        )
        if paste_data:
            requests.append(
                {
                    "pasteData": {
                        "coordinate": {
                            "sheetId": sheet_id,
                            "rowIndex": start_row - 1,
                            "columnIndex": start_col_idx,
                        },
                        "data": paste_data,
                        "delimiter": "\t",
                        "type": "PASTE_NORMAL",
                    }
                }
            )

    if include_last_processed and last_processed_cell:
        last_col_idx, last_row = _split_cell(last_processed_cell)
        requests.append(
            {
                "pasteData": {
                    "coordinate": {
                        "sheetId": sheet_id,
                        "rowIndex": last_row - 1,
                        "columnIndex": last_col_idx,
                    },
                    "data": _timestamp_for_sheets(),
                    "delimiter": "\t",
                    "type": "PASTE_NORMAL",
                }
            }
        )

    return requests


def replace_values(
    spreadsheet_id: str,
    tab: str,
    values: List[List],
    *,
    client: SheetsClient,
    start_cell: str = "A5",
    last_processed_cell: str | None = None,
    ensure_tail_space: bool = False,
    clear_range: bool = True,
) -> None:
    """Replace all values in ``tab`` with ``values``."""

    svc = client.svc
    requests = build_replace_values_requests(
        spreadsheet_id,
        tab,
        values,
        client=client,
        start_cell=start_cell,
        last_processed_cell=last_processed_cell,
        ensure_tail_space=ensure_tail_space,
        clear_range=clear_range,
    )

    if requests:
        client.execute(
            svc.spreadsheets()
            .batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": requests},
            )
        )

    if last_processed_cell:
        update_last_processed(
            spreadsheet_id,
            tab,
            last_processed_cell,
            client=client,
        )
