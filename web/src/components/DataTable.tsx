function fmtCell(v: unknown): string {
  if (v === null || v === undefined) return '—'
  const s = String(v).trim()
  return s || '—'
}

export function DataTable({
  rows,
  empty,
  monoCols,
}: {
  rows: Record<string, unknown>[]
  empty: string
  monoCols?: Set<string>
}) {
  if (!rows.length) {
    return (
      <div className="rounded-2xl border border-dashed border-[var(--color-border-strong)] bg-white/[0.02] px-5 py-10 text-center text-sm text-[var(--color-muted)]">
        {empty}
      </div>
    )
  }
  const cols = Object.keys(rows[0])
  return (
    <div className="max-w-full overflow-x-auto rounded-2xl border border-[var(--color-border)] bg-[#06080c]/80">
      <table className="w-full min-w-[720px] border-collapse text-left text-sm">
        <thead>
          <tr className="border-b border-[var(--color-border)] bg-[#0d121a]/95 backdrop-blur">
            {cols.map((c) => (
              <th key={c} className="sticky top-0 px-4 py-3 font-semibold text-[var(--color-muted)]">
                {c}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr
              key={i}
              className="border-b border-white/[0.04] transition-colors hover:bg-white/[0.03]"
            >
              {cols.map((c) => {
                const raw = row[c]
                const mono = monoCols?.has(c)
                return (
                  <td
                    key={c}
                    className={`px-4 py-2.5 text-[var(--color-text)] ${mono ? 'font-mono text-[0.85rem]' : ''}`}
                  >
                    {fmtCell(raw)}
                  </td>
                )
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
