import { useEffect, useState } from 'react'
import { Loader2, Upload } from 'lucide-react'
import { apiGet } from '../api'
import { DataTable } from '../components/DataTable'
import type { RowsResponse } from '../types'

async function postImportBill(file: File): Promise<Record<string, unknown>> {
  const fd = new FormData()
  fd.append('file', file)
  const res = await fetch('/import-bill-detail', {
    method: 'POST',
    body: fd,
    credentials: 'include',
  })
  if (res.status === 401) {
    window.location.assign('/app/login')
    throw new Error('Unauthorized')
  }
  const data = (await res.json()) as Record<string, unknown>
  if (!res.ok) {
    const msg =
      (typeof data.detail === 'string' && data.detail) ||
      (typeof data.error === 'string' && data.error) ||
      (data.pipeline_error != null && String(data.pipeline_error)) ||
      res.statusText
    throw new Error(msg)
  }
  return data
}

export function Costs() {
  const [rows, setRows] = useState<Record<string, unknown>[] | null>(null)
  const [err, setErr] = useState<string | null>(null)
  const [uploading, setUploading] = useState(false)
  const [uploadMsg, setUploadMsg] = useState<string | null>(null)

  const load = () => {
    setErr(null)
    apiGet<RowsResponse>('/costs')
      .then((d) => setRows(d.rows))
      .catch((e: Error) => setErr(e.message))
  }

  useEffect(() => {
    load()
  }, [])

  const onFile = async (f: File | null) => {
    if (!f) return
    setUploadMsg(null)
    setUploading(true)
    try {
      const r = await postImportBill(f)
      const parts = [
        typeof r.rows === 'number' ? `Hotovo. Zapísaných produktov: ${r.rows}` : '',
        r.tab ? `Záložka: ${String(r.tab)}` : '',
        r.spreadsheet ? `Tabuľka: ${String(r.spreadsheet)}` : '',
      ].filter(Boolean)
      if (r.pipeline_ran) {
        parts.push(
          r.pipeline_ok
            ? 'Pipeline: OK'
            : `Pipeline: chyba — ${String(r.pipeline_error ?? r.pipeline_exit_code ?? '')}`,
        )
      }
      setUploadMsg(parts.join('\n'))
      load()
    } catch (e) {
      setUploadMsg(e instanceof Error ? e.message : 'Chyba uploadu')
    } finally {
      setUploading(false)
    }
  }

  if (err && rows === null) return <p className="text-red-400">{err}</p>
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
        <h1 className="text-3xl font-bold tracking-tight">Costs</h1>
        <p className="mt-2 text-[var(--color-muted)]">
          BillDetail import a chýbajúce supplier náklady.
        </p>
      </header>

      <div className="grid gap-6 lg:grid-cols-2">
        <section className="rounded-2xl border border-[var(--color-border)] bg-[var(--color-surface)]/90 p-6">
          <h2 className="text-lg font-semibold">BillDetail Import</h2>
          <p className="mt-2 text-sm leading-relaxed text-[var(--color-muted)]">
            Vyber <strong className="text-[var(--color-text)]">.xls</strong> alebo{' '}
            <strong className="text-[var(--color-text)]">.xlsx</strong>. Import zapíše supplier costs;
            pipeline sa môže spustiť automaticky podľa env.
          </p>
          <label className="mt-6 flex cursor-pointer flex-col gap-3">
            <span className="sr-only">Súbor</span>
            <div className="flex flex-wrap items-center gap-3">
              <span className="inline-flex items-center gap-2 rounded-xl border border-[var(--color-border)] bg-white/[0.04] px-4 py-2.5 text-sm font-medium hover:bg-white/[0.08]">
                {uploading ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <Upload className="h-4 w-4" />
                )}
                Vybrať súbor
              </span>
              <input
                type="file"
                accept=".xls,.xlsx,application/vnd.ms-excel"
                className="sr-only"
                disabled={uploading}
                onChange={(e) => {
                  const file = e.target.files?.[0] ?? null
                  void onFile(file)
                  e.target.value = ''
                }}
              />
            </div>
          </label>
          {uploadMsg ? (
            <pre className="mt-4 whitespace-pre-wrap rounded-xl bg-white/[0.04] p-4 font-mono text-xs text-[var(--color-muted)]">
              {uploadMsg}
            </pre>
          ) : null}
        </section>

        <section className="rounded-2xl border border-[var(--color-border)] bg-[var(--color-surface)]/90 p-6">
          <h2 className="text-lg font-semibold">Missing Supplier Costs</h2>
          <p className="mt-1 text-sm text-[var(--color-muted)]">
            Agregát z tabuľky MISSING_SUPPLIER_COSTS (alebo podľa config).
          </p>
          <div className="mt-5">
            <DataTable rows={rows} empty="Žiadne chýbajúce supplier costs." />
          </div>
        </section>
      </div>
    </div>
  )
}
