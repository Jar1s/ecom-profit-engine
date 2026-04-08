"""Tests for product name normalization."""

import unittest

from normalize import normalize_order_number, normalize_product_name, product_title_family_levels


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


if __name__ == "__main__":
    unittest.main()
