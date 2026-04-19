import { useCallback, useEffect, useState } from 'react'
import { ExternalLink, Loader2, Play } from 'lucide-react'
import { apiGet, apiPostJson } from '../api'
import { DataTable } from '../components/DataTable'
import type { DashboardResponse, PipelineRunResult, UiStatus } from '../types'

function KpiGrid({ cards }: { cards: DashboardResponse['cards'] }) {
  return (
    <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
      {cards.map((c) => (
        <article
          key={c.label}
          className="group relative overflow-hidden rounded-2xl border border-[var(--color-border)] bg-gradient-to-br from-[#121a26] to-[#0c1018] p-5 shadow-[0_20px_50px_rgba(0,0,0,0.35)] transition-transform hover:-translate-y-0.5"
        >
          <div
            className="pointer-events-none absolute inset-0 opacity-40"
            style={{
              background:
                'radial-gradient(ellipse at top right, rgba(96,165,250,0.15), transparent 55%)',
            }}
          />
          <p className="relative text-xs font-medium uppercase tracking-wide text-[var(--color-muted)]">
            {c.label}
          </p>
          <p className="relative mt-3 text-2xl font-bold tabular-nums tracking-tight text-white sm:text-[1.75rem]">
            {c.value}
          </p>
          <p className="relative mt-3 text-xs leading-snug text-[var(--color-muted)]">{c.meta}</p>
        </article>
      ))}
    </div>
  )
}

function RunBlock({ runs }: { runs: DashboardResponse['runs'] }) {
  return (
    <div className="space-y-2">
      {runs.map((r, idx) => {
        const isErr = r.job === 'last_error'
        return (
          <div
            key={`${idx}-${r.job}`}
            className={`flex flex-col gap-1 rounded-xl border px-4 py-3 sm:flex-row sm:items-start sm:justify-between ${
              isErr
                ? 'border-red-500/25 bg-red-500/[0.06]'
                : 'border-[var(--color-border)] bg-white/[0.02]'
            }`}
          >
            <div>
              <p className="font-mono text-sm font-semibold text-white">{r.job}</p>
              <p className="mt-1 text-xs text-[var(--color-muted)]">{r.purpose}</p>
            </div>
            <p
              className={`shrink-0 font-mono text-xs sm:text-right ${isErr ? 'text-red-300/90' : 'text-[var(--color-muted)]'}`}
            >
              {r.last_run}
            </p>
          </div>
        )
      })}
    </div>
  )
}

export function Dashboard() {
  const [data, setData] = useState<DashboardResponse | null>(null)
  const [err, setErr] = useState<string | null>(null)
  const [runMsg, setRunMsg] = useState<string | null>(null)
  const [running, setRunning] = useState<string | null>(null)

  const load = useCallback(() => {
    setErr(null)
    apiGet<DashboardResponse>('/dashboard')
      .then(setData)
      .catch((e: Error) => setErr(e.message))
  }, [])

  useEffect(() => {
    load()
  }, [load])

  const runMode = async (mode: string) => {
    setRunMsg(null)
    setRunning(mode)
    try {
      const res = await apiPostJson<PipelineRunResult>(`/run/${mode}`)
      setRunMsg(res.message || (res.ok ? 'OK' : 'Zlyhalo'))
      load()
    } catch (e) {
      setRunMsg(e instanceof Error ? e.message : 'Chyba')
    } finally {
      setRunning(null)
    }
  }

  if (err && !data) {
    return <p className="text-red-400">{err}</p>
  }

  if (!data) {
    return (
      <div className="flex items-center gap-2 text-[var(--color-muted)]">
        <Loader2 className="h-5 w-5 animate-spin" />
        Načítavam dáta…
      </div>
    )
  }

  const st: UiStatus = data.status
  const sheetBtn = st.sheet_url ? (
    <a
      href={st.sheet_url}
      target="_blank"
      rel="noopener noreferrer"
      className="inline-flex items-center gap-2 rounded-xl border border-[var(--color-border)] bg-white/[0.04] px-4 py-2 text-sm font-medium text-white transition hover:bg-white/[0.08]"
    >
      Otvoriť Google Sheet
      <ExternalLink className="h-4 w-4 opacity-70" />
    </a>
  ) : (
    <span className="text-sm text-[var(--color-muted)]">Doplň GOOGLE_SHEET_ID pre priamy odkaz.</span>
  )

  return (
    <div className="space-y-10">
      <header className="space-y-2">
        <h1 className="text-3xl font-bold tracking-tight">Dashboard</h1>
        <p className="max-w-3xl text-[var(--color-muted)]">
          Operatívny prehľad nad pipeline výstupmi, jobmi a poslednými výsledkami.
        </p>
      </header>

      <KpiGrid cards={data.cards} />

      <div className="grid gap-6 lg:grid-cols-5">
        <section className="lg:col-span-3">
          <div className="rounded-2xl border border-[var(--color-border)] bg-[var(--color-surface)]/90 p-6 shadow-lg">
            <h2 className="text-lg font-semibold">Job Control</h2>
            <p className="mt-1 text-sm text-[var(--color-muted)]">
              Spúšťanie pipeline módov priamo z aplikácie.
            </p>
            <div className="mt-6 flex flex-wrap gap-3">
              {(
                [
                  ['core', 'primary', 'Run Core'],
                  ['tracking', 'secondary', 'Run Tracking'],
                  ['reporting', 'secondary', 'Run Reporting'],
                  ['full', 'ghost', 'Full Rebuild'],
                ] as const
              ).map(([mode, kind, label]) => {
                const primary =
                  kind === 'primary'
                    ? 'bg-[var(--color-accent)] text-white shadow-lg shadow-blue-500/20 hover:brightness-110'
                    : kind === 'secondary'
                      ? 'border border-[var(--color-border)] bg-white/[0.04] text-white hover:bg-white/[0.08]'
                      : 'border border-dashed border-[var(--color-border-strong)] bg-transparent text-[var(--color-muted)] hover:border-[var(--color-border)] hover:text-white'
                return (
                  <button
                    key={mode}
                    type="button"
                    disabled={!!running}
                    onClick={() => runMode(mode)}
                    className={`inline-flex items-center gap-2 rounded-xl px-4 py-2.5 text-sm font-semibold transition disabled:opacity-50 ${primary}`}
                  >
                    {running === mode ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <Play className="h-4 w-4 opacity-80" />
                    )}
                    {label}
                  </button>
                )
              })}
            </div>
            {runMsg ? (
              <p className="mt-4 rounded-xl bg-white/[0.04] px-4 py-3 font-mono text-sm text-[var(--color-muted)]">
                {runMsg}
              </p>
            ) : null}

            <div className="mt-8 space-y-4 border-t border-[var(--color-border)] pt-8">
              <div className="flex flex-col justify-between gap-3 rounded-xl bg-white/[0.02] px-4 py-3 sm:flex-row sm:items-center">
                <div>
                  <p className="font-medium">Supplier costs source</p>
                  <p className="text-sm text-[var(--color-muted)]">{st.supplier_tab}</p>
                </div>
                {sheetBtn}
              </div>
              <div className="flex flex-col justify-between gap-3 rounded-xl bg-white/[0.02] px-4 py-3 sm:flex-row sm:items-center">
                <div>
                  <p className="font-medium">Web import</p>
                  <p className="text-sm text-[var(--color-muted)]">
                    BillDetail upload — import + pipeline podľa tvojho flow.
                  </p>
                </div>
                <a
                  href="/app/costs"
                  className="inline-flex shrink-0 items-center justify-center rounded-xl border border-[var(--color-border)] bg-white/[0.04] px-4 py-2 text-sm font-medium hover:bg-white/[0.08]"
                >
                  Open Costs
                </a>
              </div>
            </div>
          </div>
        </section>

        <section className="lg:col-span-2">
          <div className="h-full rounded-2xl border border-[var(--color-border)] bg-[var(--color-surface)]/90 p-6 shadow-lg">
            <h2 className="text-lg font-semibold">Run Status</h2>
            <p className="mt-1 text-sm text-[var(--color-muted)]">
              Checkpointy a posledné joby z <span className="font-mono text-xs">PIPELINE_STATE</span>.
            </p>
            <div className="mt-6">
              <RunBlock runs={data.runs} />
            </div>
          </div>
        </section>
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        <section className="rounded-2xl border border-[var(--color-border)] bg-[var(--color-surface)]/80 p-6">
          <h2 className="text-lg font-semibold">Recent Orders</h2>
          <p className="mt-1 text-sm text-[var(--color-muted)]">
            Posledné order-level riadky bez otvárania sheetu.
          </p>
          <div className="mt-5">
            <DataTable
              rows={data.recent_orders}
              empty="ORDER_LEVEL zatiaľ nemá dáta."
              monoCols={new Set(['Order_ID', 'Tracking_Numbers', 'Order'])}
            />
          </div>
        </section>
        <section className="rounded-2xl border border-[var(--color-border)] bg-[var(--color-surface)]/80 p-6">
          <h2 className="text-lg font-semibold">Recent Daily Summary</h2>
          <p className="mt-1 text-sm text-[var(--color-muted)]">Revenue, cost, profit a ad spend po dňoch.</p>
          <div className="mt-5">
            <DataTable rows={data.recent_daily} empty="DAILY_SUMMARY zatiaľ nemá dáta." />
          </div>
        </section>
      </div>
    </div>
  )
}
