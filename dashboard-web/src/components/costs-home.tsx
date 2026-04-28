"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Loader2, Upload } from "lucide-react";
import { apiGet } from "@/lib/api";
import type { RowsResponse } from "@/types";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { DataTable } from "@/components/data-table";

async function postImportBill(file: File): Promise<Record<string, unknown>> {
  const fd = new FormData();
  fd.append("file", file);
  const res = await fetch("/import-bill-detail", {
    method: "POST",
    body: fd,
    credentials: "include",
  });
  if (res.status === 401) {
    window.location.assign("/app/login");
    throw new Error("Unauthorized");
  }
  const data = (await res.json()) as Record<string, unknown>;
  if (!res.ok) {
    const msg =
      (typeof data.detail === "string" && data.detail) ||
      (typeof data.error === "string" && data.error) ||
      (data.pipeline_error != null && String(data.pipeline_error)) ||
      res.statusText;
    throw new Error(msg);
  }
  return data;
}

export function CostsHome() {
  const qc = useQueryClient();
  const q = useQuery({
    queryKey: ["rows", "/costs"],
    queryFn: () => apiGet<RowsResponse>("/costs"),
  });

  const upload = useMutation({
    mutationFn: (file: File) => postImportBill(file),
    onSuccess: async () => {
      await qc.invalidateQueries({ queryKey: ["rows", "/costs"] });
    },
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

  const uploadLines = upload.isError
    ? upload.error instanceof Error
      ? upload.error.message
      : "Chyba uploadu"
    : upload.isSuccess && upload.data
      ? [
          typeof upload.data.rows === "number" ? `Hotovo. Zapísaných produktov: ${upload.data.rows}` : "",
          upload.data.tab ? `Záložka: ${String(upload.data.tab)}` : "",
          upload.data.spreadsheet ? `Tabuľka: ${String(upload.data.spreadsheet)}` : "",
          upload.data.pipeline_ran
            ? upload.data.pipeline_ok
              ? "Pipeline: OK"
              : `Pipeline: chyba — ${String(upload.data.pipeline_error ?? upload.data.pipeline_exit_code ?? "")}`
            : "",
        ]
          .filter(Boolean)
          .join("\n")
      : null;

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-3xl font-bold tracking-tight text-slate-900">Costs</h1>
        <p className="mt-2 text-slate-600">BillDetail import a chýbajúce supplier náklady.</p>
      </header>

      <div className="grid gap-6 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>BillDetail import</CardTitle>
            <CardDescription>
              Vyber <strong>.xls</strong> alebo <strong>.xlsx</strong>. Import zapíše supplier costs; pipeline sa môže
              spustiť automaticky podľa env.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <label className="flex cursor-pointer flex-col gap-3">
              <span className="sr-only">Súbor</span>
              <div className="flex flex-wrap items-center gap-3">
                <span className="inline-flex items-center gap-2 rounded-lg border border-slate-200 bg-white px-4 py-2.5 text-sm font-semibold text-slate-900 shadow-sm hover:bg-slate-50">
                  {upload.isPending ? (
                    <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
                  ) : (
                    <Upload className="h-4 w-4" aria-hidden />
                  )}
                  Vybrať súbor
                </span>
                <input
                  type="file"
                  accept=".xls,.xlsx,application/vnd.ms-excel"
                  className="sr-only"
                  disabled={upload.isPending}
                  onChange={(e) => {
                    const file = e.target.files?.[0] ?? null;
                    if (file) upload.mutate(file);
                    e.target.value = "";
                  }}
                />
              </div>
            </label>
            {uploadLines ? (
              <pre className="mt-4 whitespace-pre-wrap rounded-lg border border-slate-200 bg-slate-50 p-4 font-mono text-xs text-slate-700">
                {uploadLines}
              </pre>
            ) : null}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Missing supplier costs</CardTitle>
            <CardDescription>Agregát z tabuľky MISSING_SUPPLIER_COSTS (alebo podľa config).</CardDescription>
          </CardHeader>
          <CardContent>
            <DataTable rows={q.data.rows} empty="Žiadne chýbajúce supplier costs." />
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
