"""Sheets API retry behaviour."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

import gspread

from sheets import _retry_sheet_api, _transient_sheet_http_status


def _api_error(status_code: int, message: str = "err") -> gspread.exceptions.APIError:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = {"error": {"code": status_code, "message": message}}
    resp.text = ""
    return gspread.exceptions.APIError(resp)


class TestSheetsRetry(unittest.TestCase):
    def test_transient_status_codes(self) -> None:
        self.assertTrue(_transient_sheet_http_status(429))
        self.assertTrue(_transient_sheet_http_status(500))
        self.assertTrue(_transient_sheet_http_status(502))
        self.assertFalse(_transient_sheet_http_status(400))
        self.assertFalse(_transient_sheet_http_status(None))

    @patch("sheets.time.sleep")
    def test_retry_500_then_success(self, _mock_sleep: MagicMock) -> None:
        n = {"c": 0}

        def fn() -> str:
            n["c"] += 1
            if n["c"] < 3:
                raise _api_error(500, "Internal error encountered.")
            return "ok"

        self.assertEqual(_retry_sheet_api(fn, what="unit"), "ok")
        self.assertEqual(n["c"], 3)


if __name__ == "__main__":
    unittest.main()
