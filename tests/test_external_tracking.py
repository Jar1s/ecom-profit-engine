"""Tests for optional 17TRACK enrichment."""

import unittest

from external_tracking import _status_line_from_track_info


class TestExternalTracking(unittest.TestCase):
    def test_status_line_from_track_info_doc_example(self) -> None:
        ti = {
            "latest_status": {
                "status": "InfoReceived",
                "sub_status": "InfoReceived",
                "sub_status_descr": None,
            },
            "latest_event": {
                "description": "Shipment information sent to FedEx",
            },
        }
        self.assertEqual(_status_line_from_track_info(ti), "Shipment information sent to FedEx")


if __name__ == "__main__":
    unittest.main()
