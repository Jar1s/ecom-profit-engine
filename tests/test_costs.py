"""Supplier cost map loading."""

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from costs import load_cost_map_from_path


class TestCosts(unittest.TestCase):
    def test_csv_headers_case_insensitive(self) -> None:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, encoding="utf-8"
        ) as f:
            f.write("product,cost\nWidget X,2.5\n")
            path = Path(f.name)
        try:
            m = load_cost_map_from_path(path)
            self.assertEqual(m.by_product.get("widget x"), 2.5)
            self.assertEqual(m.by_sku, {})
        finally:
            path.unlink(missing_ok=True)

    def test_dataframe_product_cost_columns(self) -> None:
        from costs import _cost_maps_from_dataframe

        df = pd.DataFrame([{"Product": "A", "Cost": 1.0}, {"Product": "B", "Cost": 2.0}])
        m = _cost_maps_from_dataframe(df, source="test")
        self.assertEqual(m.by_product["a"], 1.0)
        self.assertEqual(m.by_product["b"], 2.0)
        self.assertEqual(m.by_sku, {})

    def test_dataframe_sku_column(self) -> None:
        from costs import _cost_maps_from_dataframe

        df = pd.DataFrame(
            [
                {"Product": "Ignored title", "Cost": 9.0, "SKU": "SKU-1"},
            ]
        )
        m = _cost_maps_from_dataframe(df, source="test")
        self.assertEqual(m.by_sku["SKU-1"], 9.0)


if __name__ == "__main__":
    unittest.main()
