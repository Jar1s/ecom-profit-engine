import { RowsPage } from "@/components/rows-page";

export default function PayoutsPage() {
  return (
    <RowsPage
      title="Payouts"
      description="Shopify payout transakcie a fee náklady pre operating a accounting review."
      apiPath="/payouts"
      tableTitle="PAYOUTS_FEES"
      tableDescription="Transaction-level payout rows vrátane Fee_Amount a Net_Amount."
      empty="PAYOUTS_FEES zatiaľ nemá dáta."
    />
  );
}
