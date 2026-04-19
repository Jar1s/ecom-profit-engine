import { useEffect, useState } from 'react'
import { Loader2 } from 'lucide-react'
import { apiGet } from '../api'
import { DataTable } from '../components/DataTable'
import type { MarketingResponse } from '../types'

export function Marketing() {
  const [data, setData] = useState<MarketingResponse | null>(null)
  const [err, setErr] = useState<string | null>(null)

  useEffect(() => {
    apiGet<MarketingResponse>('/marketing')
      .then(setData)
      .catch((e: Error) => setErr(e.message))
  }, [])

  if (err) return <p className="text-red-400">{err}</p>
  if (!data) {
    return (
      <div className="flex items-center gap-2 text-[var(--color-muted)]">
        <Loader2 className="h-5 w-5 animate-spin" />
        Načítavam…
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-3xl font-bold tracking-tight">Marketing</h1>
        <p className="mt-2 text-[var(--color-muted)]">
          Denný Meta spend a rozpad podľa kampaní.
        </p>
      </header>
      <div className="grid gap-6 lg:grid-cols-2">
        <section className="rounded-2xl border border-[var(--color-border)] bg-[var(--color-surface)]/90 p-6">
          <h2 className="text-lg font-semibold">META_DATA</h2>
          <p className="mt-1 text-sm text-[var(--color-muted)]">
            Denný spend, ktorý feeduje DAILY_SUMMARY.
          </p>
          <div className="mt-5">
            <DataTable rows={data.meta_daily} empty="META_DATA zatiaľ bez dát." />
          </div>
        </section>
        <section className="rounded-2xl border border-[var(--color-border)] bg-[var(--color-surface)]/90 p-6">
          <h2 className="text-lg font-semibold">META_CAMPAIGNS</h2>
          <p className="mt-1 text-sm text-[var(--color-muted)]">Kampaň × deň — spend a výkon.</p>
          <div className="mt-5">
            <DataTable rows={data.campaigns} empty="META_CAMPAIGNS zatiaľ bez dát." />
          </div>
        </section>
      </div>
    </div>
  )
}
