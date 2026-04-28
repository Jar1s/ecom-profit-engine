import { RowsPage } from "@/components/rows-page";

export default function AccountingPage() {
  return (
    <RowsPage
      title="Accounting"
      description="Mesačný management P&L z tabuľky BOOKKEEPING."
      apiPath="/accounting"
      tableTitle="BOOKKEEPING"
      tableDescription="US-style monthly management P&L (nie daňové podanie)."
      empty="BOOKKEEPING zatiaľ nemá dáta."
    />
  );
}
