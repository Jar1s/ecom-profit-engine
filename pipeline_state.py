"""Persistent pipeline state stored in a Google Sheets tab."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from config import Settings
from sheets import replace_worksheet_simple, try_read_worksheet_dataframe


@dataclass(frozen=True)
class PipelineState:
    schema_version: int = 1
    shopify_orders_updated_at_max: str = ""
    meta_last_until_date: str = ""
    last_core_sync_at: str = ""
    last_tracking_sync_at: str = ""
    last_reporting_sync_at: str = ""
    last_successful_run_kind: str = ""
    last_error_summary: str = ""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_pipeline_state(settings: Settings) -> PipelineState:
    df = try_read_worksheet_dataframe(
        settings,
        settings.pipeline_state_tab,
        required_headers=("key", "value"),
    )
    if df is None or df.empty:
        return PipelineState()
    values: dict[str, str] = {}
    for _, row in df.iterrows():
        key = str(row.get("key") or "").strip()
        if not key:
            continue
        values[key] = str(row.get("value") or "").strip()
    base = asdict(PipelineState())
    for key in list(base.keys()):
        if key in values:
            if key == "schema_version":
                try:
                    base[key] = int(values[key])
                except ValueError:
                    base[key] = 1
            else:
                base[key] = values[key]
    return PipelineState(**base)


def save_pipeline_state(settings: Settings, state: PipelineState) -> None:
    import pandas as pd

    rows = [{"key": key, "value": value} for key, value in asdict(state).items()]
    replace_worksheet_simple(settings, settings.pipeline_state_tab, pd.DataFrame(rows))
