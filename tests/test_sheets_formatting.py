"""Google Sheets formatting helpers."""

from sheets_formatting import _number_format_for_column


def test_money_columns_use_two_decimal_format() -> None:
    assert _number_format_for_column("Product_Cost") == {
        "type": "NUMBER",
        "pattern": "#,##0.00",
    }
    assert _number_format_for_column("Ad_Spend_USD") == {
        "type": "NUMBER",
        "pattern": "#,##0.00",
    }


def test_count_columns_use_integer_format() -> None:
    assert _number_format_for_column("Quantity") == {
        "type": "NUMBER",
        "pattern": "0",
    }
