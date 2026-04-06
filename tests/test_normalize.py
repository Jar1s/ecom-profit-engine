"""Tests for product name normalization."""

import unittest

from normalize import normalize_product_name


class TestNormalize(unittest.TestCase):
    def test_trim_and_casefold(self) -> None:
        self.assertEqual(normalize_product_name("  Sweater  "), "sweater")

    def test_collapses_whitespace(self) -> None:
        self.assertEqual(normalize_product_name("Foo   Bar"), "foo bar")

    def test_empty(self) -> None:
        self.assertEqual(normalize_product_name(""), "")


if __name__ == "__main__":
    unittest.main()
