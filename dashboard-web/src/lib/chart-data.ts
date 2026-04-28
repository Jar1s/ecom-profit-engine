export type DailyPoint = {
  day: string;
  revenue: number;
  grossProfit: number;
  adSpend: number;
  roas: number | null;
};

function num(v: unknown): number {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string") {
    const n = Number(String(v).replace(/\s/g, "").replace(",", "."));
    return Number.isFinite(n) ? n : 0;
  }
  return 0;
}

function dayLabel(v: unknown): string {
  const s = v == null ? "" : String(v).trim();
  if (!s) return "";
  return s.length >= 10 ? s.slice(0, 10) : s;
}

/** Sort ascending by date, cap last `maxPoints` for readable charts. */
export function buildDailyTrendPoints(rows: Record<string, unknown>[], maxPoints = 30): DailyPoint[] {
  const parsed = rows
    .map((r) => ({
      raw: dayLabel(r.Date),
      t: dayLabel(r.Date),
      revenue: num(r.Revenue),
      grossProfit: num(r.Gross_Profit),
      adSpend: num(r.Ad_Spend),
      roas: (() => {
        const x = num(r.Marketing_ROAS);
        return x > 0 && Number.isFinite(x) ? x : null;
      })(),
    }))
    .filter((r) => r.t)
    .sort((a, b) => (a.t < b.t ? -1 : a.t > b.t ? 1 : 0));
  const tail = parsed.slice(Math.max(0, parsed.length - maxPoints));
  return tail.map((r) => ({
    day: r.t,
    revenue: r.revenue,
    grossProfit: r.grossProfit,
    adSpend: r.adSpend,
    roas: r.roas,
  }));
}
