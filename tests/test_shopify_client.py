"""Tests for Shopify order → line rows and shipping status helpers."""

import unittest
from datetime import date

from shopify_client import order_shipping_columns, orders_to_line_rows


class TestShopifyClient(unittest.TestCase):
    def test_order_shipping_columns_unfulfilled(self) -> None:
        o = {"fulfillment_status": None, "fulfillments": []}
        c = order_shipping_columns(o)
        self.assertEqual(c["Fulfillment_Status"], "unfulfilled")
        self.assertEqual(c["Shipment_Status"], "")
        self.assertEqual(c["Delivery_Status"], "Unfulfilled")

    def test_order_shipping_columns_in_transit(self) -> None:
        o = {
            "fulfillment_status": "fulfilled",
            "fulfillments": [{"shipment_status": "in_transit"}, {"shipment_status": "label_printed"}],
        }
        c = order_shipping_columns(o)
        self.assertEqual(c["Shipment_Status"], "in_transit")
        self.assertEqual(c["Delivery_Status"], "In transit")

    def test_order_shipping_columns_delivered(self) -> None:
        o = {
            "fulfillment_status": "fulfilled",
            "fulfillments": [{"shipment_status": "delivered"}],
        }
        c = order_shipping_columns(o)
        self.assertEqual(c["Delivery_Status"], "Delivered")
        self.assertEqual(c["Days_In_Transit"], 0)

    def test_order_shipping_columns_days_since_ship(self) -> None:
        o = {
            "fulfillment_status": "fulfilled",
            "fulfillments": [
                {"shipment_status": "in_transit", "created_at": "2026-04-01T12:00:00Z"},
            ],
        }
        c = order_shipping_columns(o, today=date(2026, 4, 10))
        self.assertEqual(c["Shipped_Date"], "2026-04-01")
        self.assertEqual(c["Days_In_Transit"], 9)

    def test_order_shipping_columns_cancelled(self) -> None:
        o = {
            "cancelled_at": "2026-04-01T12:00:00Z",
            "fulfillment_status": "fulfilled",
            "fulfillments": [{"shipment_status": "in_transit"}],
        }
        c = order_shipping_columns(o)
        self.assertEqual(c["Delivery_Status"], "Cancelled")

    def test_orders_to_line_rows_includes_shipping_columns(self) -> None:
        orders = [
            {
                "created_at": "2026-04-01T10:00:00Z",
                "id": 1,
                "name": "#1001",
                "fulfillment_status": "fulfilled",
                "fulfillments": [{"shipment_status": "in_transit"}],
                "line_items": [
                    {
                        "id": 10,
                        "name": "Widget",
                        "sku": "W-1",
                        "quantity": 1,
                        "price": "10.00",
                    }
                ],
            }
        ]
        rows = orders_to_line_rows(orders)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["Fulfillment_Status"], "fulfilled")
        self.assertEqual(rows[0]["Shipment_Status"], "in_transit")
        self.assertEqual(rows[0]["Delivery_Status"], "In transit")
        self.assertEqual(rows[0]["Order"], "#1001")


if __name__ == "__main__":
    unittest.main()
