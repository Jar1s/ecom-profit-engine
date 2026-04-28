import { RowsPage } from "@/components/rows-page";

export default function DailyPage() {
  return (
    <RowsPage
      title="Daily"
      description="Denné agregáty výkonu, nákladov a reklamy."
      apiPath="/daily"
      tableTitle="DAILY_SUMMARY"
      tableDescription="Posledné dni z pipeline."
      empty="DAILY_SUMMARY zatiaľ nemá dáta."
    />
  );
}
