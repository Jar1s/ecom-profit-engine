"use client";

import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Loader2, Play, Settings2 } from "lucide-react";
import { apiGet, apiPostJson } from "@/lib/api";
import type { PipelineRunOverrides, PipelineRunResult, RunResponse, RunRow } from "@/types";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

function RunList({ runs }: { runs: RunRow[] }) {
  return (
    <div className="space-y-2">
      {runs.map((r) => {
        const isErr = r.job === "last_error";
        return (
          <div
            key={r.job + r.last_run}
            className={`flex flex-col gap-1 rounded-lg border px-4 py-3 sm:flex-row sm:items-start sm:justify-between ${
              isErr ? "border-red-200 bg-red-50" : "border-slate-200 bg-slate-50/60"
            }`}
          >
            <div>
              <p className="font-mono text-sm font-semibold text-slate-900">{r.job}</p>
              <p className="mt-1 text-xs text-slate-600">{r.purpose}</p>
            </div>
            <p className={`shrink-0 font-mono text-xs sm:text-right ${isErr ? "text-red-700" : "text-slate-500"}`}>
              {r.last_run}
            </p>
          </div>
        );
      })}
    </div>
  );
}

export function JobsHome() {
  const qc = useQueryClient();
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [overrides, setOverrides] = useState<PipelineRunOverrides>({
    meta_campaign_insights: true,
    meta_continue_on_error: true,
    sheets_fancy_layout: false,
    sheets_conditional_format: true,
    shopify_fulfillment_enrich: true,
    shopify_fulfillment_refetch_early: false,
    shopify_graphql_fulfillment_verify: true,
    track17_enabled: true,
  });
  const q = useQuery({
    queryKey: ["jobs"],
    queryFn: () => apiGet<RunResponse>("/jobs"),
  });

  const runMut = useMutation({
    mutationFn: (mode: string) => apiPostJson<PipelineRunResult>(`/run/${mode}`, { overrides }),
    onSettled: () => {
      void qc.invalidateQueries({ queryKey: ["jobs"] });
      void qc.invalidateQueries({ queryKey: ["dashboard"] });
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

  const jobRows: [string, string, string, "primary" | "secondary" | "ghost"][] = [
    ["auto", "Vyberie business / tracking / reporting podľa PIPELINE_STATE", "Spustiť", "primary"],
    ["business", "Shopify + Meta + payouts + refunds + BOOKKEEPING", "Spustiť", "primary"],
    ["core", "Shopify + supplier costs + daily Meta + hlavné taby", "Spustiť", "primary"],
    ["tracking", "17TRACK + delivery refresh len pre aktívne zásielky", "Spustiť", "secondary"],
    ["reporting", "META_CAMPAIGNS a BOOKKEEPING", "Spustiť", "secondary"],
    ["full", "Fallback / debug celý pipeline v jednom kroku", "Spustiť", "ghost"],
  ];
  const optionRows: [keyof PipelineRunOverrides, string, string][] = [
    ["sheets_fancy_layout", "Sheets dashboard layout", "Súhrnné bloky, grafy a šírky stĺpcov; pomalšie na Verceli."],
    ["sheets_conditional_format", "Conditional formatting", "Zelené doručené riadky, červený profit a ROAS warning."],
    ["meta_campaign_insights", "Meta campaigns", "Campaign × day rozpad pre marketing a reporting."],
    ["meta_continue_on_error", "Pokračovať pri Meta chybe", "Shopify a Sheets dobehnú aj keď Meta token/API zlyhá."],
    ["shopify_fulfillment_enrich", "Shopify fulfillment detail", "REST detail objednávok, keď list response nemá shipment status."],
    ["shopify_graphql_fulfillment_verify", "Shopify GraphQL delivery verify", "Overí Fulfillment.displayStatus pre presnejšie delivered stavy."],
    ["shopify_fulfillment_refetch_early", "Refetch early shipment states", "Dodatočné REST volania aj pre label/confirmed stavy."],
    ["track17_enabled", "17TRACK carrier status", "Externý tracking status, ak je nastavený TRACK17_API_KEY."],
  ];

  const setOption = (key: keyof PipelineRunOverrides, checked: boolean) => {
    setOverrides((current) => ({ ...current, [key]: checked }));
  };

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-3xl font-bold tracking-tight text-slate-900">Jobs</h1>
        <p className="mt-2 text-slate-600">
          Manuálna operatíva nad schedulerom a snapshot z <span className="font-mono text-sm">PIPELINE_STATE</span>.
        </p>
      </header>

      <div className="grid gap-6 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Run jobs</CardTitle>
            <CardDescription>Pipeline režimy a per-run voľby bez zmeny Vercel env.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="rounded-lg border border-slate-200 bg-white">
              <button
                type="button"
                className="flex w-full items-center justify-between gap-3 px-4 py-3 text-left"
                onClick={() => setAdvancedOpen((v) => !v)}
              >
                <span>
                  <span className="flex items-center gap-2 text-sm font-semibold text-slate-900">
                    <Settings2 className="h-4 w-4" aria-hidden />
                    Run options
                  </span>
                  <span className="mt-1 block text-sm text-slate-600">
                    Voľby sa použijú len pre najbližšie manuálne spustenie.
                  </span>
                </span>
                <span className="font-mono text-xs text-slate-500">{advancedOpen ? "open" : "closed"}</span>
              </button>
              {advancedOpen ? (
                <div className="grid gap-3 border-t border-slate-200 p-4">
                  {optionRows.map(([key, title, desc]) => (
                    <label key={key} className="flex gap-3 rounded-md border border-slate-200 bg-slate-50/60 p-3">
                      <input
                        type="checkbox"
                        className="mt-1 h-4 w-4 rounded border-slate-300"
                        checked={overrides[key]}
                        onChange={(event) => setOption(key, event.target.checked)}
                      />
                      <span>
                        <span className="block text-sm font-semibold text-slate-900">{title}</span>
                        <span className="mt-1 block text-sm text-slate-600">{desc}</span>
                      </span>
                    </label>
                  ))}
                </div>
              ) : null}
            </div>
            <ul className="space-y-4">
              {jobRows.map(([mode, desc, label, kind]) => (
                <li
                  key={mode}
                  className="flex flex-col gap-3 rounded-lg border border-slate-200 bg-slate-50/50 p-4 sm:flex-row sm:items-center sm:justify-between"
                >
                  <div>
                    <p className="text-sm font-semibold capitalize text-slate-900">{mode}</p>
                    <p className="mt-1 text-sm text-slate-600">{desc}</p>
                  </div>
                  <Button
                    variant={kind === "primary" ? "primary" : kind === "secondary" ? "secondary" : "ghost"}
                    className="shrink-0"
                    disabled={runMut.isPending}
                    onClick={() => runMut.mutate(mode)}
                  >
                    {runMut.isPending && runMut.variables === mode ? (
                      <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
                    ) : (
                      <Play className="h-4 w-4" aria-hidden />
                    )}
                    {label}
                  </Button>
                </li>
              ))}
            </ul>
            {runMut.data || runMut.error ? (
              <p className="rounded-lg border border-slate-200 bg-slate-50 px-4 py-3 font-mono text-sm text-slate-700">
                {runMut.error instanceof Error
                  ? runMut.error.message
                  : runMut.data?.message || (runMut.data?.ok ? "OK" : "Zlyhalo")}
              </p>
            ) : null}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Run history snapshot</CardTitle>
            <CardDescription>Stav z PIPELINE_STATE.</CardDescription>
          </CardHeader>
          <CardContent>
            <RunList runs={q.data.runs} />
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
