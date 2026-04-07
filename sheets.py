"""Push DataFrames to Google Sheets (overwrite tab)."""

from __future__ import annotations

import logging

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1

from config import Settings

from sheets_charts import apply_daily_summary_charts
from sheets_formatting import (
    apply_center_alignment,
    apply_data_column_widths,
    apply_data_conditional_formatting,
    apply_summary_dashboard_format,
)

logger = logging.getLogger(__name__)

_SCOPES_SHEETS_ONLY = ("https://www.googleapis.com/auth/spreadsheets",)
_SCOPES_WITH_DRIVE = (
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
)

# Google Sheets API practical chunk size for update calls
_DEFAULT_ROW_CHUNK = 500


def _authorize(settings: Settings) -> gspread.Client:
    # Opening by spreadsheet ID uses only Sheets API; opening by title lists Drive files.
    scopes = (
        list(_SCOPES_SHEETS_ONLY)
        if settings.google_sheet_id
        else list(_SCOPES_WITH_DRIVE)
    )
    if settings.google_service_account_info is not None:
        creds = Credentials.from_service_account_info(
            settings.google_service_account_info,
            scopes=scopes,
        )
        return gspread.authorize(creds)
    path = settings.google_creds_path
    if path is None or not path.is_file():
        raise FileNotFoundError(
            "Google credentials: set GOOGLE_CREDENTIALS_JSON (Vercel) or GOOGLE_CREDS file path"
        )
    creds = Credentials.from_service_account_file(str(path), scopes=scopes)
    return gspread.authorize(creds)


def _open_spreadsheet(client: gspread.Client, settings: Settings) -> gspread.Spreadsheet:
    """
    Open by GOOGLE_SHEET_ID (recommended) or by exact title (GOOGLE_SHEET_NAME).
    Title search uses Drive API; name must match the file title and the sheet must
    be shared with the service account (Editor).
    """
    try:
        if settings.google_sheet_id:
            return client.open_by_key(settings.google_sheet_id.strip())
        assert settings.google_sheet_name is not None
        name = settings.google_sheet_name.strip()
        return client.open(name)
    except gspread.exceptions.SpreadsheetNotFound as exc:
        sa_hint = (
            "Share the spreadsheet with the service account email (Editor) from the JSON."
        )
        if settings.google_sheet_id:
            raise RuntimeError(
                f"Spreadsheet id not found or no access ({settings.google_sheet_id!r}). "
                f"{sa_hint} Check GOOGLE_SHEET_ID matches the URL between /d/ and /edit."
            ) from exc
        raise RuntimeError(
            f"No spreadsheet titled {settings.google_sheet_name!r} found for this account "
            f"(search is exact). {sa_hint} "
            "Or set GOOGLE_SHEET_ID or GOOGLE_SHEET_URL (full browser URL) and redeploy."
        ) from exc
    except gspread.exceptions.APIError as exc:
        raise RuntimeError(
            f"Google Sheets API error: {exc}. "
            "Enable Sheets API (and Drive API if opening by name only) for the GCP project; "
            "share the file with the service account."
        ) from exc


def get_or_create_supplier_costs_worksheet(
    settings: Settings, worksheet_title: str
) -> gspread.Worksheet:
    """Otvorí záložku alebo ju vytvorí s hlavičkou Product | Cost."""
    client = _authorize(settings)
    sh = _open_spreadsheet(client, settings)
    title = worksheet_title.strip()
    try:
        return sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=3000, cols=10)
        ws.append_row(["Product", "Cost"], value_input_option="USER_ENTERED")
        logger.info("Created supplier worksheet %r with header row", title)
        return ws


def dataframe_to_values(df: pd.DataFrame) -> list[list[object]]:
    """2D list for gspread; NaN -> empty string."""
    filled = df.copy()
    for col in filled.columns:
        filled[col] = filled[col].apply(lambda x: "" if pd.isna(x) else x)
    header = filled.columns.tolist()
    rows = filled.values.tolist()
    return [header] + rows


def replace_worksheet_simple(
    settings: Settings,
    worksheet_title: str,
    df: pd.DataFrame,
    *,
    row_chunk: int = _DEFAULT_ROW_CHUNK,
) -> None:
    """
    Clear tab and write dataframe (header + rows), chunked. No charts or conditional format.
    Creates the worksheet if missing.
    """
    client = _authorize(settings)
    sh = _open_spreadsheet(client, settings)
    try:
        ws = sh.worksheet(worksheet_title)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_title, rows=3000, cols=10)
        logger.info("Created worksheet %r", worksheet_title)

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    values = dataframe_to_values(df)
    if not values:
        ws.clear()
        logger.info("Sheet %r: empty — cleared", worksheet_title)
        return

    num_cols = max(len(r) for r in values)
    ws.clear()
    for start in range(0, len(values), row_chunk):
        chunk = values[start : start + row_chunk]
        start_row = start + 1
        end_row = start + len(chunk)
        end_a1 = rowcol_to_a1(end_row, num_cols)
        range_a1 = f"A{start_row}:{end_a1}"
        ws.update(chunk, range_a1, value_input_option="USER_ENTERED")
    logger.info(
        "Sheet %r: wrote %s rows (simple replace)",
        worksheet_title,
        len(values),
    )


def _apply_sheet_style(
    ws: gspread.Worksheet,
    header_row: int,
    num_cols: int,
    *,
    fancy_summary: bool = False,
) -> None:
    """Bold header row, optional light tint above (plain layout), freeze through header."""
    try:
        end = rowcol_to_a1(header_row, num_cols)
        ws.format(
            f"A{header_row}:{end}",
            {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.82, "green": 0.91, "blue": 0.98},
                "horizontalAlignment": "CENTER",
                "verticalAlignment": "MIDDLE",
            },
        )
        if header_row > 1 and not fancy_summary:
            top_end = rowcol_to_a1(header_row - 1, num_cols)
            ws.format(
                f"A1:{top_end}",
                {"backgroundColor": {"red": 0.96, "green": 0.98, "blue": 1.0}},
            )
        ws.spreadsheet.batch_update(
            {
                "requests": [
                    {
                        "updateSheetProperties": {
                            "properties": {
                                "sheetId": ws.id,
                                "gridProperties": {"frozenRowCount": header_row},
                            },
                            "fields": "gridProperties.frozenRowCount",
                        }
                    }
                ]
            }
        )
    except Exception as exc:
        logger.warning("Sheet styling skipped: %s", exc)


def upload_dataframe(
    settings: Settings,
    df: pd.DataFrame,
    worksheet_title: str,
    *,
    layout_kind: str | None = None,
    row_chunk: int = _DEFAULT_ROW_CHUNK,
) -> None:
    """Clear worksheet and write dataframe. Optional summary block + styling."""
    from sheets_layout import sheet_values_plain, sheet_values_with_summary

    client = _authorize(settings)
    sh = _open_spreadsheet(client, settings)
    logger.info(
        "Sheets target: file=%r spreadsheet_id=%s tab=%r data_rows=%s",
        sh.title,
        sh.id,
        worksheet_title,
        len(df.index),
    )
    try:
        ws = sh.worksheet(worksheet_title)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_title, rows=3000, cols=40)
        logger.info("Created worksheet %r", worksheet_title)

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    if settings.sheets_fancy_layout and layout_kind is not None:
        values, header_row = sheet_values_with_summary(df, kind=layout_kind, settings=settings)
    else:
        values, header_row = sheet_values_plain(df)

    if not values:
        ws.clear()
        logger.info("Sheet %r: empty — cleared", worksheet_title)
        return

    num_cols = max(len(r) for r in values)
    ws.clear()

    for start in range(0, len(values), row_chunk):
        chunk = values[start : start + row_chunk]
        start_row = start + 1
        end_row = start + len(chunk)
        end_a1 = rowcol_to_a1(end_row, num_cols)
        range_a1 = f"A{start_row}:{end_a1}"
        ws.update(chunk, range_a1, value_input_option="USER_ENTERED")
        logger.debug("Sheet %r: wrote rows %s-%s", worksheet_title, start_row, end_row)

    if settings.sheets_fancy_layout and layout_kind is not None:
        apply_summary_dashboard_format(ws, header_row_1based=header_row, num_cols=num_cols)
        _apply_sheet_style(ws, header_row, num_cols, fancy_summary=True)
        apply_data_column_widths(ws, num_cols)
        if layout_kind == "daily":
            apply_daily_summary_charts(ws, header_row_1based=header_row, df=df)

    apply_center_alignment(ws, num_rows=len(values), num_cols=num_cols)

    apply_data_conditional_formatting(
        ws,
        settings=settings,
        header_row_1based=header_row,
        num_sheet_rows=len(values),
        columns=list(df.columns),
        layout_kind=layout_kind,
    )

    logger.info(
        "Sheet %r: uploaded %s cell rows (header row=%s)",
        worksheet_title,
        len(values),
        header_row,
    )
