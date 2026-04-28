"use client";

import { useQuery } from "@tanstack/react-query";
import { Loader2 } from "lucide-react";
import { apiGet } from "@/lib/api";
import type { RowsResponse } from "@/types";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { DataTable } from "@/components/data-table";

export function RowsPage({
  title,
  description,
  apiPath,
  tableTitle,
  tableDescription,
  empty,
  monoCols,
}: {
  title: string;
  description: string;
  apiPath: string;
  tableTitle: string;
  tableDescription: string;
  empty: string;
  monoCols?: Set<string>;
}) {
  const q = useQuery({
    queryKey: ["rows", apiPath],
    queryFn: () => apiGet<RowsResponse>(apiPath),
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

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-3xl font-bold tracking-tight text-slate-900">{title}</h1>
        <p className="mt-2 max-w-3xl text-slate-600">{description}</p>
      </header>
      <Card>
        <CardHeader>
          <CardTitle>{tableTitle}</CardTitle>
          <CardDescription>{tableDescription}</CardDescription>
        </CardHeader>
        <CardContent>
          <DataTable rows={q.data.rows} empty={empty} monoCols={monoCols} />
        </CardContent>
      </Card>
    </div>
  );
}
