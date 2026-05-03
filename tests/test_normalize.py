"""Tests for product name normalization."""

import unittest

import pandas as pd

from normalize import (
    SHEET_DATE_COLUMN_NAMES,
    normalize_order_number,
    normalize_product_name,
    product_title_family_levels,
    sheet_date_to_iso,
)


class TestNormalize(unittest.TestCase):
    def test_trim_and_casefold(self) -> None:
        self.assertEqual(normalize_product_name("  Sweater  "), "sweater")

    def test_collapses_whitespace(self) -> None:
        self.assertEqual(normalize_product_name("Foo   Bar"), "foo bar")

    def test_empty(self) -> None:
        self.assertEqual(normalize_product_name(""), "")

    def test_normalize_order_number(self) -> None:
        self.assertEqual(normalize_order_number("#1053"), "1053")
        self.assertEqual(normalize_order_number(1053.0), "1053")
        self.assertEqual(normalize_order_number("ORD-1042-X"), "1042")

    def test_product_title_family_levels(self) -> None:
        n = normalize_product_name("Livia | Poncho - Beige / L")
        levels = product_title_family_levels(n)
        self.assertEqual(levels[0], n)
        self.assertIn("livia | poncho", levels)

    def test_unicode_dash_variants_share_family_base(self) -> None:
        beige = normalize_product_name("Livia | Oversize-Poncho-Pullover - Beige / XL")
        rose_ascii = normalize_product_name("Livia | Oversize-Poncho-Pullover - Rose / 2XL")
        rose_endash = normalize_product_name(
            "Livia | Oversize-Poncho-Pullover – Rose / 2XL"
        )  # en dash U+2013
        base = "livia | oversize-poncho-pullover"
        self.assertIn(base, product_title_family_levels(beige))
        self.assertIn(base, product_title_family_levels(rose_ascii))
        self.assertIn(base, product_title_family_levels(rose_endash))

    def test_sheet_date_to_iso_from_iso_string(self) -> None:
        self.assertEqual(sheet_date_to_iso("2026-04-11"), "2026-04-11")
        self.assertEqual(sheet_date_to_iso("2026-04-11T12:00:00Z"), "2026-04-11")

    def test_sheet_date_to_iso_from_timestamp(self) -> None:
        self.assertEqual(sheet_date_to_iso(pd.Timestamp("2026-04-11")), "2026-04-11")

    def test_sheet_date_to_iso_from_serial_string(self) -> None:
        out = sheet_date_to_iso("46102")
        self.assertRegex(out, r"^\d{4}-\d{2}-\d{2}$")
        self.assertTrue(out.startswith("2026-"), out)

    def test_sheet_date_to_iso_empty_for_zero(self) -> None:
        self.assertEqual(sheet_date_to_iso(0), "")
        self.assertEqual(sheet_date_to_iso(0.0), "")
        self.assertEqual(sheet_date_to_iso("0"), "")

    def test_sheet_date_column_names_includes_shipped(self) -> None:
        self.assertIn("Shipped_Date", SHEET_DATE_COLUMN_NAMES)


if __name__ == "__main__":
    unittest.main()
