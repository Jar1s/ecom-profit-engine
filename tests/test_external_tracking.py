"""Tests for optional 17TRACK enrichment."""

import unittest

from config import normalize_carrier_tracking_source
from external_tracking import (
    _status_line_from_track_info,
    order_tracking_columns,
    resolve_17track_carrier,
)


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

    def test_resolve_17track_carrier_maps_common_names(self) -> None:
        self.assertEqual(resolve_17track_carrier("DHL Express"), 100001)
        self.assertEqual(resolve_17track_carrier("GLS"), 100005)
        self.assertEqual(resolve_17track_carrier("Packeta"), 100132)
        self.assertEqual(resolve_17track_carrier(""), 0)
        self.assertEqual(resolve_17track_carrier("Unknown Carrier XYZ"), 0)

    def test_order_tracking_columns_shopify_fallback_when_notfound(self) -> None:
        order = {"_carrier_tracking_status": "NotFound", "fulfillments": []}
        cols = order_tracking_columns(
            order,
            ship_cols={"Delivery_Status": "Delivered", "Shipment_Status": "delivered"},
        )
        self.assertEqual(cols["Carrier_Tracking_Status"], "Shopify: Delivered")

    def test_normalize_carrier_tracking_source(self) -> None:
        self.assertEqual(normalize_carrier_tracking_source(None), "17track")
        self.assertEqual(normalize_carrier_tracking_source("shopify"), "shopify")
        self.assertEqual(normalize_carrier_tracking_source("shopify_only"), "shopify")
        self.assertEqual(normalize_carrier_tracking_source("17track"), "17track")


if __name__ == "__main__":
    unittest.main()
