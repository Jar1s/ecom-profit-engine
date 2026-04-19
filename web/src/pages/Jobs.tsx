import { useCallback, useEffect, useState } from 'react'
import { Loader2, Play } from 'lucide-react'
import { apiGet, apiPostJson } from '../api'
import type { PipelineRunResult, RunResponse, RunRow } from '../types'

function RunList({ runs }: { runs: RunRow[] }) {
  return (
    <div className="space-y-2">
      {runs.map((r) => {
        const isErr = r.job === 'last_error'
        return (
          <div
            key={r.job + r.last_run}
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

export function Jobs() {
  const [runs, setRuns] = useState<RunRow[] | null>(null)
  const [err, setErr] = useState<string | null>(null)
  const [running, setRunning] = useState<string | null>(null)
  const [runMsg, setRunMsg] = useState<string | null>(null)

  const load = useCallback(() => {
    setErr(null)
    apiGet<RunResponse>('/jobs')
      .then((d) => setRuns(d.runs))
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

  if (err && !runs) return <p className="text-red-400">{err}</p>
  if (!runs) {
    return (
      <div className="flex items-center gap-2 text-[var(--color-muted)]">
        <Loader2 className="h-5 w-5 animate-spin" />
        Načítavam…
      </div>
    )
  }

  const jobRows: [string, string, string, 'primary' | 'secondary' | 'ghost'][] = [
    ['core', 'Shopify + supplier costs + daily Meta + hlavné taby', 'Spustiť', 'primary'],
    ['tracking', '17TRACK + delivery refresh len pre aktívne zásielky', 'Spustiť', 'secondary'],
    ['reporting', 'META_CAMPAIGNS a BOOKKEEPING', 'Spustiť', 'secondary'],
    ['full', 'Fallback / debug celý pipeline v jednom kroku', 'Spustiť', 'ghost'],
  ]

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-3xl font-bold tracking-tight">Jobs</h1>
        <p className="mt-2 text-[var(--color-muted)]">
          Manuálna operatíva nad schedulerom a snapshot zo <span className="font-mono text-sm">PIPELINE_STATE</span>.
        </p>
      </header>

      <div className="grid gap-6 lg:grid-cols-2">
        <section className="rounded-2xl border border-[var(--color-border)] bg-[var(--color-surface)]/90 p-6">
          <h2 className="text-lg font-semibold">Run Jobs</h2>
          <p className="mt-1 text-sm text-[var(--color-muted)]">Pipeline režimy — rovnaké ako na Dashboarde.</p>
          <ul className="mt-6 space-y-4">
            {jobRows.map(([mode, desc, label, kind]) => (
              <li
                key={mode}
                className="flex flex-col gap-3 rounded-xl border border-[var(--color-border)] bg-white/[0.02] p-4 sm:flex-row sm:items-center sm:justify-between"
              >
                <div>
                  <p className="font-medium capitalize text-white">{mode}</p>
                  <p className="mt-1 text-sm text-[var(--color-muted)]">{desc}</p>
                </div>
                <button
                  type="button"
                  disabled={!!running}
                  onClick={() => runMode(mode)}
                  className={`inline-flex shrink-0 items-center gap-2 rounded-xl px-4 py-2.5 text-sm font-semibold transition disabled:opacity-50 ${
                    kind === 'primary'
                      ? 'bg-[var(--color-accent)] text-white shadow-lg shadow-blue-500/20'
                      : kind === 'secondary'
                        ? 'border border-[var(--color-border)] bg-white/[0.04] text-white'
                        : 'border border-dashed border-[var(--color-border-strong)] text-[var(--color-muted)]'
                  }`}
                >
                  {running === mode ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
                  {label}
                </button>
              </li>
            ))}
          </ul>
          {runMsg ? (
            <p className="mt-4 rounded-xl bg-white/[0.04] px-4 py-3 font-mono text-sm text-[var(--color-muted)]">
              {runMsg}
            </p>
          ) : null}
        </section>

        <section className="rounded-2xl border border-[var(--color-border)] bg-[var(--color-surface)]/90 p-6">
          <h2 className="text-lg font-semibold">Run History Snapshot</h2>
          <p className="mt-1 text-sm text-[var(--color-muted)]">Stav z PIPELINE_STATE.</p>
          <div className="mt-6">
            <RunList runs={runs} />
          </div>
        </section>
      </div>
    </div>
  )
}
