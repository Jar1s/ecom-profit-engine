import { RowsPage } from "@/components/rows-page";

export default function OrdersPage() {
  return (
    <RowsPage
      title="Orders"
      description="Objednávky, fulfillment, tracking a profit na úrovni objednávky."
      apiPath="/orders"
      tableTitle="ORDER_LEVEL"
      tableDescription="Najnovšie riadky bez preklikávania v Google Sheets."
      empty="Žiadne order-level dáta."
      monoCols={new Set(["Order_ID", "Tracking_Numbers", "Order"])}
    />
  );
}
