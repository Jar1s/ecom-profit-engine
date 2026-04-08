"""Embedded charts for Google Sheets (DAILY_SUMMARY tab) via spreadsheets.batchUpdate."""

from __future__ import annotations

import logging
from typing import Any

import gspread
import pandas as pd

logger = logging.getLogger(__name__)


def _grid_range(
    sheet_id: int,
    *,
    start_row: int,
    end_row: int,
    start_col: int,
    end_col: int,
) -> dict[str, Any]:
    return {
        "sheetId": sheet_id,
        "startRowIndex": start_row,
        "endRowIndex": end_row,
        "startColumnIndex": start_col,
        "endColumnIndex": end_col,
    }


def _delete_charts_on_worksheet(ws: gspread.Worksheet) -> None:
    """Remove all charts on this worksheet (pipeline refresh replaces them)."""
    sh = ws.spreadsheet
    # Field mask must use `charts` on Sheet (not `sheets(charts)` — that yields API 400).
    try:
        meta = sh.fetch_sheet_metadata(
            params={"fields": "sheets(properties(sheetId),charts)"}
        )
    except gspread.exceptions.APIError:
        meta = sh.fetch_sheet_metadata()
    targets: list[int] = []
    sid = ws.id
    for sheet in meta.get("sheets", []):
        if sheet.get("properties", {}).get("sheetId") != sid:
            continue
        for ch in sheet.get("charts") or []:
            cid = ch.get("chartId")
            if cid is not None:
                targets.append(int(cid))
    if not targets:
        return
    try:
        sh.batch_update(
            {
                "requests": [
                    {"deleteEmbeddedObject": {"objectId": oid}} for oid in targets
                ]
            }
        )
    except Exception as exc:
        logger.warning("Could not delete old charts: %s", exc)


def apply_daily_summary_charts(
    ws: gspread.Worksheet,
    *,
    header_row_1based: int,
    df: pd.DataFrame,
) -> None:
    """
    Line chart: Revenue vs Ad_Spend vs Date; column chart: Marketing_ROAS.
    Expects columns present in merged daily export; skips if too few rows.
    """
    if df.empty or len(df.index) < 2:
        return
    cols = list(df.columns)
    try:
        ic_date = cols.index("Date")
        ic_rev = cols.index("Revenue")
        ic_spend = cols.index("Ad_Spend")
    except ValueError:
        logger.warning("Daily charts: missing Date/Revenue/Ad_Spend column")
        return
    ic_roas = cols.index("Marketing_ROAS") if "Marketing_ROAS" in cols else None

    sheet_id = ws.id
    n = len(df.index)
    # Data rows only (0-based indices); header is row header_row_1based - 1
    d0 = header_row_1based
    d1 = header_row_1based + n

    _delete_charts_on_worksheet(ws)

    domain = {
        "domain": {
            "sourceRange": {
                "sources": [_grid_range(sheet_id, start_row=d0, end_row=d1, start_col=ic_date, end_col=ic_date + 1)]
            }
        }
    }

    series_rev = {
        "series": {
            "sourceRange": {
                "sources": [_grid_range(sheet_id, start_row=d0, end_row=d1, start_col=ic_rev, end_col=ic_rev + 1)]
            }
        },
        "targetAxis": "LEFT_AXIS",
    }
    series_spend = {
        "series": {
            "sourceRange": {
                "sources": [_grid_range(sheet_id, start_row=d0, end_row=d1, start_col=ic_spend, end_col=ic_spend + 1)]
            }
        },
        "targetAxis": "LEFT_AXIS",
    }

    anchor0 = header_row_1based + n
    anchor1 = anchor0 + 22

    chart_line = {
        "addChart": {
            "chart": {
                "spec": {
                        "title": (
                            "Revenue vs Ad_Spend (USD)"
                            if "Net_Profit" in cols
                            else "Revenue vs výdavky na reklamu (miestna mena)"
                        ),
                    "basicChart": {
                        "chartType": "LINE",
                        "legendPosition": "BOTTOM_LEGEND",
                        "axis": [
                            {"position": "BOTTOM_AXIS"},
                            {"position": "LEFT_AXIS", "title": "Suma"},
                        ],
                        "domains": [domain],
                        "series": [series_rev, series_spend],
                        "headerCount": 0,
                    }
                },
                "position": {
                    "overlayPosition": {
                        "anchorCell": {
                            "sheetId": sheet_id,
                            "rowIndex": anchor0,
                            "columnIndex": 0,
                        },
                        "offsetXPixels": 0,
                        "offsetYPixels": 0,
                        "widthPixels": 920,
                        "heightPixels": 300,
                    }
                },
            }
        }
    }

    requests: list[dict[str, Any]] = [chart_line]

    if ic_roas is not None:
        domain_roas = {
            "domain": {
                "sourceRange": {
                    "sources": [
                        _grid_range(sheet_id, start_row=d0, end_row=d1, start_col=ic_date, end_col=ic_date + 1)
                    ]
                }
            }
        }
        series_roas = {
            "series": {
                "sourceRange": {
                    "sources": [
                        _grid_range(
                            sheet_id,
                            start_row=d0,
                            end_row=d1,
                            start_col=ic_roas,
                            end_col=ic_roas + 1,
                        )
                    ]
                }
            },
            "targetAxis": "LEFT_AXIS",
        }
        chart_roas = {
            "addChart": {
                "chart": {
                    "spec": {
                        "title": "Marketing ROAS (Revenue / Ad_Spend)",
                        "basicChart": {
                            "chartType": "COLUMN",
                            "legendPosition": "NO_LEGEND",
                            "axis": [
                                {"position": "BOTTOM_AXIS"},
                                {"position": "LEFT_AXIS", "title": "ROAS"},
                            ],
                            "domains": [domain_roas],
                            "series": [series_roas],
                            "headerCount": 0,
                        }
                    },
                    "position": {
                        "overlayPosition": {
                            "anchorCell": {
                                "sheetId": sheet_id,
                                "rowIndex": anchor1,
                                "columnIndex": 0,
                            },
                            "offsetXPixels": 0,
                            "offsetYPixels": 0,
                            "widthPixels": 920,
                            "heightPixels": 280,
                        }
                    },
                }
            }
        }
        requests.append(chart_roas)

    try:
        ws.spreadsheet.batch_update({"requests": requests})
    except Exception as exc:
        logger.warning("Daily charts could not be added: %s", exc)
