"""Rich dashboard formatting for the summary block above data tables (Google Sheets API)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import gspread

if TYPE_CHECKING:
    from config import Settings

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
            "unmergeCells": {
                "range": {
                    "sheetId": sheet_id,
                }
            }
        }
    )
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


def clear_worksheet_conditional_format_rules(ws: gspread.Worksheet) -> None:
    """Remove all conditional formatting rules on this tab (pipeline overwrites data each run)."""
    try:
        meta = ws.spreadsheet.fetch_sheet_metadata()
        for sheet in meta.get("sheets", []):
            if sheet.get("properties", {}).get("sheetId") != ws.id:
                continue
            rules = sheet.get("conditionalFormatRules") or []
            if not rules:
                return
            requests = [
                {"deleteConditionalFormatRule": {"sheetId": ws.id, "index": i}}
                for i in range(len(rules) - 1, -1, -1)
            ]
            ws.spreadsheet.batch_update({"requests": requests})
            return
    except Exception as exc:
        logger.warning("Could not clear conditional format rules: %s", exc)


def _col_index(columns: list[str], name: str) -> int | None:
    try:
        return list(columns).index(name)
    except ValueError:
        return None


_TWO_DECIMAL_COLUMNS: frozenset[str] = frozenset(
    {
        "Revenue",
        "Revenue_USD",
        "Product_Cost",
        "Gross_Profit",
        "Gross_Profit_USD",
        "Ad_Spend",
        "Ad_Spend_USD",
        "Purchase_Value",
        "Purchase_Value_USD",
        "Marketing_ROAS",
        "Net_Profit",
        "Sales_Revenue",
        "COGS",
        "Marketing_Spend",
        "Gross_profit",
        "Operating_income",
        "Sales_Tax_Collected",
        "Refunds",
        "Net_Sales",
        "Shipping_Charged",
        "Discounts",
        "Subtotal",
        "GMV",
    }
)

_INTEGER_COLUMNS: frozenset[str] = frozenset(
    {
        "Order_ID",
        "Line_Item_ID",
        "Quantity",
        "Days_In_Transit",
        "Orders_Total",
        "Orders_Delivered",
        "Orders_Undelivered",
        "Impressions",
        "Clicks",
        "Adds_to_Cart",
        "Checkouts_Initiated",
        "Purchases",
    }
)


def _number_format_for_column(name: str) -> dict[str, str] | None:
    if name in _TWO_DECIMAL_COLUMNS:
        return {"type": "NUMBER", "pattern": "#,##0.00"}
    if name in _INTEGER_COLUMNS:
        return {"type": "NUMBER", "pattern": "0"}
    return None


def _grid(
    sheet_id: int,
    *,
    r0: int,
    r1: int,
    c0: int,
    c1: int,
) -> dict[str, Any]:
    return {
        "sheetId": sheet_id,
        "startRowIndex": r0,
        "endRowIndex": r1,
        "startColumnIndex": c0,
        "endColumnIndex": c1,
    }


_NEG_PROFIT_BG = {"red": 0.96, "green": 0.78, "blue": 0.78}
_ROAS_WARN_BG = {"red": 1.0, "green": 0.94, "blue": 0.75}
_DELIVERED_BG = {"red": 0.82, "green": 0.94, "blue": 0.84}


def _a1_column_letters_0based(col_idx: int) -> str:
    """0-based column index → A1 column letters (A=0, Z=25, AA=26)."""
    n = col_idx + 1
    letters = ""
    while n:
        n, r = divmod(n - 1, 26)
        letters = chr(65 + r) + letters
    return letters


def apply_data_conditional_formatting(
    ws: gspread.Worksheet,
    *,
    settings: "Settings",
    header_row_1based: int,
    num_sheet_rows: int,
    columns: list[str],
    layout_kind: str | None,
    num_cols: int | None = None,
) -> None:
    """
    Data rows only: delivered (word match, case-insensitive) → light green on ORDERS_DB / ORDER_LEVEL.
    Uses ``Delivery_Status`` and/or ``Carrier_Tracking_Status`` (17TRACK / long carrier text).
    Gross_Profit < 0 → light red; Net_Profit < 0 → light red (daily USD mode);
    Marketing_ROAS < threshold → light yellow (daily).
    """
    if not settings.sheets_conditional_format:
        return
    if not columns or header_row_1based >= num_sheet_rows:
        return

    sheet_id = ws.id
    # First data row 0-based; end exclusive
    d0 = header_row_1based
    d1 = num_sheet_rows
    if d0 >= d1:
        return

    clear_worksheet_conditional_format_rules(ws)
    requests: list[dict[str, Any]] = []

    nc = num_cols if num_cols is not None else len(columns)
    ds_i = _col_index(columns, "Delivery_Status")
    cts_i = _col_index(columns, "Carrier_Tracking_Status")
    if layout_kind in ("orders", "order_level") and nc > 0:
        first_data_row_1based = d0 + 1
        # Word-boundary match on either column (carrier strings are often long, e.g. "DELIVERED WITH SAFE DROP").
        _delivered_pat = r'"(?i)\bdelivered\b"'
        delivered_fragments: list[str] = []
        for idx in (ds_i, cts_i):
            if idx is None:
                continue
            col_letter = _a1_column_letters_0based(idx)
            delivered_fragments.append(
                f"REGEXMATCH(${col_letter}{first_data_row_1based}, {_delivered_pat})"
            )
        if delivered_fragments:
            if len(delivered_fragments) == 1:
                formula = "=" + delivered_fragments[0]
            else:
                formula = "=OR(" + ",".join(delivered_fragments) + ")"
            requests.append(
                {
                    "addConditionalFormatRule": {
                        "rule": {
                            "ranges": [
                                _grid(sheet_id, r0=d0, r1=d1, c0=0, c1=nc),
                            ],
                            "booleanRule": {
                                "condition": {
                                    "type": "CUSTOM_FORMULA",
                                    "values": [{"userEnteredValue": formula}],
                                },
                                "format": {"backgroundColor": _DELIVERED_BG},
                            },
                        },
                        "index": len(requests),
                    }
                }
            )

    gp = _col_index(columns, "Gross_Profit")
    gp_us = _col_index(columns, "Gross_profit")
    if gp is not None and layout_kind in ("orders", "order_level", "daily", "bookkeeping"):
        requests.append(
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [_grid(sheet_id, r0=d0, r1=d1, c0=gp, c1=gp + 1)],
                        "booleanRule": {
                            "condition": {
                                "type": "NUMBER_LESS",
                                "values": [{"userEnteredValue": "0"}],
                            },
                            "format": {"backgroundColor": _NEG_PROFIT_BG},
                        },
                    },
                    "index": len(requests),
                }
            }
        )

    if gp_us is not None and layout_kind == "bookkeeping":
        requests.append(
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [_grid(sheet_id, r0=d0, r1=d1, c0=gp_us, c1=gp_us + 1)],
                        "booleanRule": {
                            "condition": {
                                "type": "NUMBER_LESS",
                                "values": [{"userEnteredValue": "0"}],
                            },
                            "format": {"backgroundColor": _NEG_PROFIT_BG},
                        },
                    },
                    "index": len(requests),
                }
            }
        )

    net_i = _col_index(columns, "Net_Profit")
    if net_i is not None and layout_kind in ("daily", "bookkeeping"):
        requests.append(
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [_grid(sheet_id, r0=d0, r1=d1, c0=net_i, c1=net_i + 1)],
                        "booleanRule": {
                            "condition": {
                                "type": "NUMBER_LESS",
                                "values": [{"userEnteredValue": "0"}],
                            },
                            "format": {"backgroundColor": _NEG_PROFIT_BG},
                        },
                    },
                    "index": len(requests),
                }
            }
        )

    op_i = _col_index(columns, "Operating_income")
    if op_i is not None and layout_kind == "bookkeeping":
        requests.append(
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [_grid(sheet_id, r0=d0, r1=d1, c0=op_i, c1=op_i + 1)],
                        "booleanRule": {
                            "condition": {
                                "type": "NUMBER_LESS",
                                "values": [{"userEnteredValue": "0"}],
                            },
                            "format": {"backgroundColor": _NEG_PROFIT_BG},
                        },
                    },
                    "index": len(requests),
                }
            }
        )

    thr = settings.sheets_roas_warn_below
    roas_i = _col_index(columns, "Marketing_ROAS")
    if (
        thr is not None
        and roas_i is not None
        and layout_kind == "daily"
    ):
        # Avoid highlighting blanks: number less than threshold (empty cells are not numbers).
        requests.append(
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [_grid(sheet_id, r0=d0, r1=d1, c0=roas_i, c1=roas_i + 1)],
                        "booleanRule": {
                            "condition": {
                                "type": "NUMBER_LESS",
                                "values": [{"userEnteredValue": str(thr)}],
                            },
                            "format": {"backgroundColor": _ROAS_WARN_BG},
                        },
                    },
                    "index": len(requests),
                }
            }
        )

    if not requests:
        return
    try:
        ws.spreadsheet.batch_update({"requests": requests})
    except Exception as exc:
        logger.warning("Conditional formatting failed: %s", exc)


def apply_data_number_formats(
    ws: gspread.Worksheet,
    *,
    header_row_1based: int,
    num_sheet_rows: int,
    columns: list[str],
) -> None:
    """Force stable numeric display: money/metriky with exactly 2 decimals, counts as integers."""
    if not columns or header_row_1based >= num_sheet_rows:
        return
    requests: list[dict[str, Any]] = []
    for idx, name in enumerate(columns):
        fmt = _number_format_for_column(name)
        if fmt is None:
            continue
        requests.append(
            {
                "repeatCell": {
                    "range": _grid(
                        ws.id,
                        r0=header_row_1based,
                        r1=num_sheet_rows,
                        c0=idx,
                        c1=idx + 1,
                    ),
                    "cell": {"userEnteredFormat": {"numberFormat": fmt}},
                    "fields": "userEnteredFormat.numberFormat",
                }
            }
        )
    if not requests:
        return
    try:
        ws.spreadsheet.batch_update({"requests": requests})
    except Exception as exc:
        logger.warning("Number formatting failed: %s", exc)


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
