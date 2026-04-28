"use client";

import { useQuery } from "@tanstack/react-query";
import { Loader2 } from "lucide-react";
import { apiGet } from "@/lib/api";
import type { MarketingResponse } from "@/types";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { DataTable } from "@/components/data-table";

export function MarketingHome() {
  const q = useQuery({
    queryKey: ["marketing"],
    queryFn: () => apiGet<MarketingResponse>("/marketing"),
  });

  if (q.error && !q.data) {
    return <p className="text-sm font-medium text-red-700">{q.error.message}</p>;
  }
  if (q.isPending || !q.data) {
    return (
      <div className="flex items-center gap-2 text-slate-600">
        <Loader2 className="h-5 w-5 animate-spin" aria-hidden />
        Načítavam…
      </div>
    );
  }

  const data = q.data;
  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-3xl font-bold tracking-tight text-slate-900">Marketing</h1>
        <p className="mt-2 text-slate-600">Denný Meta spend a rozpad podľa kampaní.</p>
      </header>
      <div className="grid gap-6 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>META_DATA</CardTitle>
            <CardDescription>Denný spend, ktorý feeduje DAILY_SUMMARY.</CardDescription>
          </CardHeader>
          <CardContent>
            <DataTable rows={data.meta_daily} empty="META_DATA zatiaľ bez dát." />
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>META_CAMPAIGNS</CardTitle>
            <CardDescription>Kampaň × deň — spend a výkon.</CardDescription>
          </CardHeader>
          <CardContent>
            <DataTable rows={data.campaigns} empty="META_CAMPAIGNS zatiaľ bez dát." />
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
