import { RowsPage } from "@/components/rows-page";

export default function AccountingPage() {
  return (
    <RowsPage
      title="Accounting"
      description="Mesačný management P&L z BOOKKEEPING vrátane refund breakdownu a payout fee impactu."
      apiPath="/accounting"
      tableTitle="BOOKKEEPING"
      tableDescription="US-style monthly management P&L (nie daňové podanie) + after-fees operating výsledok."
      empty="BOOKKEEPING zatiaľ nemá dáta."
    />
  );
}
