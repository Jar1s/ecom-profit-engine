"""Tests for data_quality missing supplier cost report."""

from __future__ import annotations

import pandas as pd

from data_quality import build_missing_supplier_costs_report


def test_empty_dataframe() -> None:
    df = pd.DataFrame()
    r = build_missing_supplier_costs_report(df)
    assert r.empty
    assert list(r.columns) == [
        "Product",
        "SKU",
        "Line_Items",
        "Revenue_Sum",
        "First_Date",
        "Last_Date",
    ]


def test_all_costs_present() -> None:
    df = pd.DataFrame(
        [
            {
                "Date": "2026-01-01",
                "Order": "A1",
                "Product": "Widget",
                "SKU": "W-1",
                "Revenue": 10.0,
                "Product_Cost": 5.0,
            }
        ]
    )
    r = build_missing_supplier_costs_report(df)
    assert r.empty


def test_missing_costs_aggregated_and_sorted() -> None:
    df = pd.DataFrame(
        [
            {
                "Date": "2026-01-02",
                "Order": "O1",
                "Product": "Alpha",
                "SKU": "A",
                "Revenue": 30.0,
                "Product_Cost": 0.0,
            },
            {
                "Date": "2026-01-01",
                "Order": "O2",
                "Product": "Alpha",
                "SKU": "A",
                "Revenue": 20.0,
                "Product_Cost": 0.0,
            },
            {
                "Date": "2026-01-03",
                "Order": "O3",
                "Product": "Beta",
                "SKU": "B",
                "Revenue": 100.0,
                "Product_Cost": 0.0,
            },
            {
                "Date": "2026-01-01",
                "Order": "O4",
                "Product": "Gamma",
                "SKU": "G",
                "Revenue": 50.0,
                "Product_Cost": 10.0,
            },
        ]
    )
    r = build_missing_supplier_costs_report(df)
    assert len(r) == 2
    assert list(r["Product"]) == ["Alpha", "Beta"]
    assert r.iloc[0]["Line_Items"] == 2
    assert r.iloc[0]["Revenue_Sum"] == 50.0
    assert r.iloc[0]["First_Date"] == "2026-01-01"
    assert r.iloc[0]["Last_Date"] == "2026-01-02"


def test_nan_product_cost_treated_as_missing() -> None:
    df = pd.DataFrame(
        [
            {
                "Date": "2026-01-01",
                "Order": "O1",
                "Product": "X",
                "SKU": "x",
                "Revenue": 1.0,
                "Product_Cost": float("nan"),
            }
        ]
    )
    r = build_missing_supplier_costs_report(df)
    assert len(r) == 1
    assert r.iloc[0]["Line_Items"] == 1
