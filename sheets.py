"""Push DataFrames to Google Sheets (overwrite tab)."""

from __future__ import annotations

import logging
from pathlib import Path

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1

from config import Settings

logger = logging.getLogger(__name__)

_SCOPES = (
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
)

# Google Sheets API practical chunk size for update calls
_DEFAULT_ROW_CHUNK = 500


def _authorize(settings: Settings) -> gspread.Client:
    if settings.google_service_account_info is not None:
        creds = Credentials.from_service_account_info(
            settings.google_service_account_info,
            scopes=list(_SCOPES),
        )
        return gspread.authorize(creds)
    path = settings.google_creds_path
    if path is None or not path.is_file():
        raise FileNotFoundError(
            "Google credentials: set GOOGLE_CREDENTIALS_JSON (Vercel) or GOOGLE_CREDS file path"
        )
    creds = Credentials.from_service_account_file(str(path), scopes=list(_SCOPES))
    return gspread.authorize(creds)


def dataframe_to_values(df: pd.DataFrame) -> list[list[object]]:
    """2D list for gspread; NaN -> empty string."""
    filled = df.copy()
    for col in filled.columns:
        filled[col] = filled[col].apply(lambda x: "" if pd.isna(x) else x)
    header = filled.columns.tolist()
    rows = filled.values.tolist()
    return [header] + rows


def upload_dataframe(
    settings: Settings,
    df: pd.DataFrame,
    worksheet_title: str,
    *,
    row_chunk: int = _DEFAULT_ROW_CHUNK,
) -> None:
    """Clear worksheet and write dataframe (batch by row chunks)."""
    client = _authorize(settings)
    sh = client.open(settings.google_sheet_name)
    try:
        ws = sh.worksheet(worksheet_title)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_title, rows=1000, cols=50)
        logger.info("Created worksheet %r", worksheet_title)

    values = dataframe_to_values(df)
    if not values:
        ws.clear()
        logger.info("Sheet %r: empty dataframe — cleared", worksheet_title)
        return

    num_cols = len(values[0])
    ws.clear()

    for start in range(0, len(values), row_chunk):
        chunk = values[start : start + row_chunk]
        start_row = start + 1
        end_row = start + len(chunk)
        end_a1 = rowcol_to_a1(end_row, num_cols)
        range_a1 = f"A{start_row}:{end_a1}"
        ws.update(chunk, range_a1, value_input_option="USER_ENTERED")
        logger.debug("Sheet %r: wrote rows %s-%s", worksheet_title, start_row, end_row)

    logger.info("Sheet %r: uploaded %s rows (+ header)", worksheet_title, len(values) - 1)
