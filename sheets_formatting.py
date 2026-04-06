"""Rich dashboard formatting for the summary block above data tables (Google Sheets API)."""

from __future__ import annotations

import logging

import gspread

logger = logging.getLogger(__name__)

_TITLE_BG = {"red": 0.10, "green": 0.45, "blue": 0.91}
_TITLE_FG = {"red": 1.0, "green": 1.0, "blue": 1.0}
_SUMMARY_BG = {"red": 0.95, "green": 0.97, "blue": 0.99}
_BORDER = {"red": 0.85, "green": 0.87, "blue": 0.90}


def apply_summary_dashboard_format(
    ws: gspread.Worksheet,
    *,
    header_row_1based: int,
    num_cols: int,
) -> None:
    """
    Rows 1 .. header_row-1 = analytics block: merged title row, KPI rows with light fill,
    outer border. header_row_1based = 1-based index of the dataframe header row.
    """
    if header_row_1based <= 1:
        return
    sheet_id = ws.id
    n = num_cols
    # 0-based index of last row above the table (usually a blank spacer)
    last_block_0 = header_row_1based - 2
    requests: list[dict] = []

    requests.append(
        {
            "mergeCells": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": n,
                },
                "mergeType": "MERGE_ALL",
            }
        }
    )
    requests.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": n,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": _TITLE_BG,
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "wrapStrategy": "WRAP",
                        "textFormat": {
                            "bold": True,
                            "foregroundColor": _TITLE_FG,
                            "fontSize": 11,
                        },
                    }
                },
                "fields": (
                    "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,"
                    "verticalAlignment,wrapStrategy)"
                ),
            }
        }
    )
    requests.append(
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": 0,
                    "endIndex": 1,
                },
                "properties": {"pixelSize": 38},
                "fields": "pixelSize",
            }
        }
    )

    if last_block_0 >= 1:
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": last_block_0 + 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": n,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": _SUMMARY_BG,
                            "horizontalAlignment": "CENTER",
                            "verticalAlignment": "MIDDLE",
                            "wrapStrategy": "WRAP",
                            "textFormat": {"fontSize": 10},
                        }
                    },
                    "fields": (
                        "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,"
                        "verticalAlignment,wrapStrategy)"
                    ),
                }
            }
        )

    requests.append(
        {
            "updateBorders": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": last_block_0 + 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": n,
                },
                "top": {"style": "SOLID", "width": 1, "color": _BORDER},
                "bottom": {"style": "SOLID", "width": 1, "color": _BORDER},
                "left": {"style": "SOLID", "width": 1, "color": _BORDER},
                "right": {"style": "SOLID", "width": 1, "color": _BORDER},
            }
        }
    )

    requests.append(
        {
            "autoResizeDimensions": {
                "dimensions": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": min(n, 26),
                }
            }
        }
    )

    try:
        ws.spreadsheet.batch_update({"requests": requests})
    except Exception as exc:
        logger.warning("Dashboard formatting partially failed: %s", exc)


def apply_center_alignment(
    ws: gspread.Worksheet,
    *,
    num_rows: int,
    num_cols: int,
) -> None:
    """Center text in all uploaded cells (only alignment fields — keeps fills from dashboard format)."""
    if num_rows < 1 or num_cols < 1:
        return
    try:
        ws.spreadsheet.batch_update(
            {
                "requests": [
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": ws.id,
                                "startRowIndex": 0,
                                "endRowIndex": num_rows,
                                "startColumnIndex": 0,
                                "endColumnIndex": num_cols,
                            },
                            "cell": {
                                "userEnteredFormat": {
                                    "horizontalAlignment": "CENTER",
                                    "verticalAlignment": "MIDDLE",
                                }
                            },
                            "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment)",
                        }
                    }
                ]
            }
        )
    except Exception as exc:
        logger.warning("Center alignment skipped: %s", exc)


def apply_data_column_widths(ws: gspread.Worksheet, num_cols: int, min_width: int = 112) -> None:
    try:
        ws.spreadsheet.batch_update(
            {
                "requests": [
                    {
                        "updateDimensionProperties": {
                            "range": {
                                "sheetId": ws.id,
                                "dimension": "COLUMNS",
                                "startIndex": i,
                                "endIndex": i + 1,
                            },
                            "properties": {"pixelSize": min_width},
                            "fields": "pixelSize",
                        }
                    }
                    for i in range(min(num_cols, 16))
                ]
            }
        )
    except Exception as exc:
        logger.warning("Column width update skipped: %s", exc)
