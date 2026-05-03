"""Push DataFrames to Google Sheets (overwrite tab)."""

from __future__ import annotations

import logging
import os
import random
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import TypeVar

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
    apply_orders_tab_number_formats,
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

_T = TypeVar("_T")


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def pause_between_sheet_uploads() -> None:
    """
    Space out tab uploads so the pipeline stays under Sheets **writes per minute** quota.
    Override with SHEETS_PAUSE_BETWEEN_TABS_SECONDS (default 1.5).
    """
    sec = _env_float("SHEETS_PAUSE_BETWEEN_TABS_SECONDS", 1.5)
    if sec > 0:
        time.sleep(sec)


def _inter_chunk_pause() -> None:
    sec = _env_float("SHEETS_INTER_CHUNK_PAUSE_SECONDS", 0.35)
    if sec > 0:
        time.sleep(sec)


def _transient_sheet_http_status(status_code: int | None) -> bool:
    """429 quota; 5xx often transient on Google's side (Sheets metadata open, reads, writes)."""
    if status_code is None:
        return False
    if status_code == 429:
        return True
    return status_code in (500, 502, 503, 504)


def _retry_sheet_api(fn: Callable[[], _T], *, what: str = "Sheets API") -> _T:
    """Retry on HTTP 429 and transient 5xx (Google occasional internal errors)."""
    max_attempts = 10
    for attempt in range(max_attempts):
        try:
            return fn()
        except gspread.exceptions.APIError as exc:
            sc = getattr(exc.response, "status_code", None)
            if _transient_sheet_http_status(sc) and attempt < max_attempts - 1:
                if sc == 429:
                    delay = min(120.0, (2.0**attempt) * 1.25) + random.uniform(0.25, 1.5)
                else:
                    delay = min(90.0, (1.6**attempt) * 2.0) + random.uniform(0.2, 1.0)
                logger.warning(
                    "%s HTTP %s, sleeping %.1fs [%s/%s]: %s",
                    what,
                    sc or "?",
                    delay,
                    attempt + 1,
                    max_attempts,
                    exc,
                )
                time.sleep(delay)
                continue
            raise


# Backwards-compatible name (writes used this helper first)
_retry_sheet_write = _retry_sheet_api


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
    def _open_once() -> gspread.Spreadsheet:
        if settings.google_sheet_id:
            return client.open_by_key(settings.google_sheet_id.strip())
        assert settings.google_sheet_name is not None
        name = settings.google_sheet_name.strip()
        return client.open(name)

    try:
        return _retry_sheet_api(_open_once, what="open spreadsheet")
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
        sc = getattr(exc.response, "status_code", None)
        if sc in (500, 502, 503, 504):
            hint = (
                "Transient Google server error after retries — wait a minute and re-run the pipeline. "
                "If it keeps happening, check https://www.google.com/appsstatus — not a missing API enable."
            )
        elif sc == 429:
            hint = (
                "Quota (429) after retries — increase SHEETS_PAUSE_BETWEEN_TABS_SECONDS or spread jobs; "
                "share the spreadsheet with the service account (Editor)."
            )
        else:
            hint = (
                "Enable Sheets API (and Drive API if opening by name only) for the GCP project; "
                "share the file with the service account (Editor)."
            )
        raise RuntimeError(f"Google Sheets API error: {exc}. {hint}") from exc


def get_or_create_supplier_costs_worksheet(
    settings: Settings, worksheet_title: str
) -> gspread.Worksheet:
    """Otvorí záložku alebo ju vytvorí s hlavičkou Product | Cost."""
    client = _authorize(settings)
    sh = _open_spreadsheet(client, settings)
    title = worksheet_title.strip()
    try:
        return _retry_sheet_api(lambda: sh.worksheet(title), what="worksheet")
    except gspread.exceptions.WorksheetNotFound:
        ws = _retry_sheet_api(
            lambda: sh.add_worksheet(title=title, rows=3000, cols=10),
            what="add_worksheet",
        )
        _retry_sheet_api(
            lambda: ws.append_row(
                ["Product", "Cost", "SKU"], value_input_option="USER_ENTERED"
            ),
            what="append_row",
        )
        logger.info("Created supplier worksheet %r with header row", title)
        return ws


def worksheet_values_to_dataframe(
    values: list[list[str]],
    worksheet_title: str,
    *,
    required_headers: list[str] | tuple[str, ...] | None = None,
) -> pd.DataFrame | None:
    """Turn raw A1 grid from Sheets into a DataFrame (same rules as try_read_worksheet_dataframe)."""
    title = worksheet_title
    if not values or len(values) < 2:
        return None
    header_row_idx = 0
    if required_headers:
        req = {str(h).strip().lower() for h in required_headers if str(h).strip()}
        for i, row in enumerate(values):
            present = {str(c).strip().lower() for c in row if str(c).strip()}
            if req.issubset(present):
                header_row_idx = i
                break
        else:
            logger.warning(
                "Worksheet %r: required headers %s not found in any row",
                title,
                sorted(req),
            )
            return None
    if header_row_idx + 1 >= len(values):
        return None
    header = [str(c).strip() for c in values[header_row_idx]]
    rows = values[header_row_idx + 1 :]
    return pd.DataFrame(rows, columns=header)


def try_read_worksheet_dataframe(
    settings: Settings,
    worksheet_title: str,
    *,
    required_headers: list[str] | tuple[str, ...] | None = None,
) -> pd.DataFrame | None:
    """Read a tab as DataFrame, or None if missing / empty / error (non-fatal)."""
    title = (worksheet_title or "").strip()
    if not title:
        return None
    try:
        client = _authorize(settings)
        sh = _open_spreadsheet(client, settings)
        ws = _retry_sheet_api(lambda: sh.worksheet(title), what="worksheet")
    except Exception as exc:
        logger.debug("Worksheet %r not available: %s", title, exc)
        return None
    try:
        values = _retry_sheet_api(lambda: ws.get_all_values(), what="read")
    except Exception as exc:
        logger.warning("Could not read worksheet %r: %s", title, exc)
        return None
    return worksheet_values_to_dataframe(values, title, required_headers=required_headers)


def read_dashboard_sheet_tabs(
    settings: Settings,
    specs: list[tuple[str, tuple[str, ...] | None]],
) -> list[pd.DataFrame]:
    """
    Read several tabs with one OAuth + spreadsheet open, then parallel get_all_values.
    Much faster than calling try_read_worksheet_dataframe once per tab (repeated open_by_key).
    """
    out: list[pd.DataFrame] = [pd.DataFrame() for _ in specs]
    jobs: list[tuple[int, str, tuple[str, ...] | None]] = []
    for i, (tab, req) in enumerate(specs):
        t = (tab or "").strip()
        if t:
            jobs.append((i, t, req))
    if not jobs:
        return out

    client = _authorize(settings)
    sh = _open_spreadsheet(client, settings)
    max_workers = max(
        1,
        min(
            len(jobs),
            int(os.getenv("DASHBOARD_SHEET_READ_CONCURRENCY", "10")),
            16,
        ),
    )

    def _read_job(job: tuple[int, str, tuple[str, ...] | None]) -> tuple[int, pd.DataFrame]:
        idx, title, req = job
        try:
            ws = _retry_sheet_api(lambda: sh.worksheet(title), what="worksheet")
            values = _retry_sheet_api(lambda: ws.get_all_values(), what="read")
        except Exception as exc:
            logger.debug("Worksheet %r not available: %s", title, exc)
            return idx, pd.DataFrame()
        df = worksheet_values_to_dataframe(values, title, required_headers=req)
        if df is None:
            return idx, pd.DataFrame()
        return idx, df.copy()

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        results = list(ex.map(_read_job, jobs))
    for idx, df in results:
        out[idx] = df
    return out


def dataframe_to_values(df: pd.DataFrame) -> list[list[object]]:
    """2D list for gspread; NaN -> empty string."""
    from normalize import SHEET_DATE_COLUMN_NAMES, sheet_date_to_iso

    filled = df.copy()
    for col in filled.columns:
        if col in SHEET_DATE_COLUMN_NAMES:
            filled[col] = filled[col].map(lambda x: "" if pd.isna(x) else sheet_date_to_iso(x))
        else:
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
        ws = _retry_sheet_api(lambda: sh.worksheet(worksheet_title), what="worksheet")
    except gspread.exceptions.WorksheetNotFound:
        ws = _retry_sheet_api(
            lambda: sh.add_worksheet(title=worksheet_title, rows=3000, cols=10),
            what="add_worksheet",
        )
        logger.info("Created worksheet %r", worksheet_title)

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    values = dataframe_to_values(df)
    if not values:
        _retry_sheet_write(lambda: ws.clear(), what="clear")
        logger.info("Sheet %r: empty — cleared", worksheet_title)
        return

    num_cols = max(len(r) for r in values)
    _retry_sheet_write(lambda: ws.clear(), what="clear")
    for start in range(0, len(values), row_chunk):
        chunk = values[start : start + row_chunk]
        start_row = start + 1
        end_row = start + len(chunk)
        end_a1 = rowcol_to_a1(end_row, num_cols)
        range_a1 = f"A{start_row}:{end_a1}"

        def _write_chunk(
            c: list[list[object]] = chunk,
            rng: str = range_a1,
        ) -> None:
            ws.update(c, rng, value_input_option="RAW")

        _retry_sheet_write(_write_chunk, what="update")
        _inter_chunk_pause()
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
        ws = _retry_sheet_api(lambda: sh.worksheet(worksheet_title), what="worksheet")
    except gspread.exceptions.WorksheetNotFound:
        ws = _retry_sheet_api(
            lambda: sh.add_worksheet(title=worksheet_title, rows=3000, cols=40),
            what="add_worksheet",
        )
        logger.info("Created worksheet %r", worksheet_title)

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    if settings.sheets_fancy_layout and layout_kind is not None:
        values, header_row = sheet_values_with_summary(df, kind=layout_kind, settings=settings)
    else:
        values, header_row = sheet_values_plain(df)

    if not values:
        _retry_sheet_write(lambda: ws.clear(), what="clear")
        logger.info("Sheet %r: empty — cleared", worksheet_title)
        return

    num_cols = max(len(r) for r in values)
    _retry_sheet_write(lambda: ws.clear(), what="clear")

    for start in range(0, len(values), row_chunk):
        chunk = values[start : start + row_chunk]
        start_row = start + 1
        end_row = start + len(chunk)
        end_a1 = rowcol_to_a1(end_row, num_cols)
        range_a1 = f"A{start_row}:{end_a1}"

        def _write_chunk(
            c: list[list[object]] = chunk,
            rng: str = range_a1,
        ) -> None:
            # RAW keeps ISO Date/Shipped_Date strings as text. With USER_ENTERED,
            # Sheets parses them as dates; later TEXT formatting can display serials.
            ws.update(c, rng, value_input_option="RAW")

        _retry_sheet_write(_write_chunk, what="update")
        _inter_chunk_pause()
        logger.debug("Sheet %r: wrote rows %s-%s", worksheet_title, start_row, end_row)

    if settings.sheets_fancy_layout and layout_kind is not None:
        _retry_sheet_write(
            lambda: apply_summary_dashboard_format(
                ws, header_row_1based=header_row, num_cols=num_cols
            ),
            what="format",
        )
        _retry_sheet_write(
            lambda: _apply_sheet_style(ws, header_row, num_cols, fancy_summary=True),
            what="format",
        )
        _retry_sheet_write(
            lambda: apply_data_column_widths(ws, num_cols),
            what="format",
        )
        if layout_kind == "daily":
            _retry_sheet_write(
                lambda: apply_daily_summary_charts(
                    ws, header_row_1based=header_row, df=df
                ),
                what="charts",
            )

    _retry_sheet_write(
        lambda: apply_center_alignment(ws, num_rows=len(values), num_cols=num_cols),
        what="format",
    )

    _retry_sheet_write(
        lambda: apply_data_conditional_formatting(
            ws,
            settings=settings,
            header_row_1based=header_row,
            num_sheet_rows=len(values),
            num_cols=num_cols,
            columns=list(df.columns),
            layout_kind=layout_kind,
        ),
        what="format",
    )

    if layout_kind in ("orders", "order_level"):
        cols = list(df.columns)
        _retry_sheet_write(
            lambda: apply_orders_tab_number_formats(
                ws,
                header_row_1based=header_row,
                num_sheet_rows=len(values),
                columns=cols,
            ),
            what="format",
        )

    logger.info(
        "Sheet %r: uploaded %s cell rows (header row=%s)",
        worksheet_title,
        len(values),
        header_row,
    )
