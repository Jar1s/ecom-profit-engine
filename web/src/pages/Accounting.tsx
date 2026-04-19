import { useEffect, useState } from 'react'
import { Loader2 } from 'lucide-react'
import { apiGet } from '../api'
import { DataTable } from '../components/DataTable'
import type { RowsResponse } from '../types'

export function Accounting() {
  const [rows, setRows] = useState<Record<string, unknown>[] | null>(null)
  const [err, setErr] = useState<string | null>(null)

  useEffect(() => {
    apiGet<RowsResponse>('/accounting')
      .then((d) => setRows(d.rows))
      .catch((e: Error) => setErr(e.message))
  }, [])

  if (err) return <p className="text-red-400">{err}</p>
  if (!rows) {
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
        <h1 className="text-3xl font-bold tracking-tight">Accounting</h1>
        <p className="mt-2 text-[var(--color-muted)]">
          Mesačný management P&amp;L (nie účtovné závierky).
        </p>
      </header>
      <section className="rounded-2xl border border-[var(--color-border)] bg-[var(--color-surface)]/90 p-6">
        <h2 className="text-lg font-semibold">BOOKKEEPING</h2>
        <p className="mt-1 text-sm text-[var(--color-muted)]">
          Manažérsky mesačný P&amp;L z pipeline.
        </p>
        <div className="mt-5">
          <DataTable rows={rows} empty="BOOKKEEPING zatiaľ bez dát." />
        </div>
      </section>
    </div>
  )
}
