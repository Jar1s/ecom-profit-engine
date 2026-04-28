"use client";

import {
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
  Bar,
} from "recharts";
import type { DailyPoint } from "@/lib/chart-data";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

function fmtMoneyShort(n: number) {
  if (!Number.isFinite(n)) return "—";
  if (Math.abs(n) >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (Math.abs(n) >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return n.toFixed(0);
}

export function DashboardTrends({ points }: { points: DailyPoint[] }) {
  if (!points.length) {
    return (
      <div className="grid gap-6 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Revenue vs gross profit</CardTitle>
            <CardDescription>Posledné dni z DAILY_SUMMARY</CardDescription>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-slate-600">Nedostatok dát pre graf.</p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Ad spend vs ROAS</CardTitle>
            <CardDescription>Marketing_ROAS len keď Ad_Spend &gt; 0</CardDescription>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-slate-600">Nedostatok dát pre graf.</p>
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="grid gap-6 lg:grid-cols-2">
      <Card>
        <CardHeader>
          <CardTitle>Revenue vs gross profit</CardTitle>
          <CardDescription>Posledných až 30 dní, vzostupne podľa dátumu</CardDescription>
        </CardHeader>
        <CardContent className="h-[300px]">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={points} margin={{ top: 8, right: 12, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" vertical={false} />
              <XAxis dataKey="day" tick={{ fontSize: 11 }} stroke="#94a3b8" tickMargin={8} />
              <YAxis tick={{ fontSize: 11 }} stroke="#94a3b8" tickFormatter={fmtMoneyShort} width={44} />
              <Tooltip
                formatter={(v) => [fmtMoneyShort(Number(v)), ""]}
                labelFormatter={(l) => `Deň ${String(l)}`}
                contentStyle={{ borderRadius: 8, border: "1px solid #e2e8f0" }}
              />
              <Legend wrapperStyle={{ fontSize: 12 }} />
              <Line type="monotone" dataKey="revenue" name="Revenue" stroke="#0f172a" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="grossProfit" name="Gross profit" stroke="#2563eb" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Ad spend vs ROAS</CardTitle>
          <CardDescription>Spend (stĺpce) a ROAS (čiara, pravá os)</CardDescription>
        </CardHeader>
        <CardContent className="h-[300px]">
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={points} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" vertical={false} />
              <XAxis dataKey="day" tick={{ fontSize: 11 }} stroke="#94a3b8" tickMargin={8} />
              <YAxis yAxisId="left" tick={{ fontSize: 11 }} stroke="#94a3b8" tickFormatter={fmtMoneyShort} width={44} />
              <YAxis
                yAxisId="right"
                orientation="right"
                tick={{ fontSize: 11 }}
                stroke="#94a3b8"
                width={36}
                tickFormatter={(v) => `${Number(v).toFixed(1)}×`}
              />
              <Tooltip
                contentStyle={{ borderRadius: 8, border: "1px solid #e2e8f0" }}
                formatter={(value, name) => {
                  const n = Number(value);
                  if (String(name) === "ROAS") return [`${n.toFixed(2)}×`, name];
                  return [fmtMoneyShort(n), name];
                }}
              />
              <Legend wrapperStyle={{ fontSize: 12 }} />
              <Bar yAxisId="left" dataKey="adSpend" name="Ad spend" fill="#cbd5e1" radius={[4, 4, 0, 0]} />
              <Line
                yAxisId="right"
                type="monotone"
                dataKey="roas"
                name="ROAS"
                stroke="#0f172a"
                strokeWidth={2}
                dot={false}
                connectNulls
              />
            </ComposedChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>
    </div>
  );
}
