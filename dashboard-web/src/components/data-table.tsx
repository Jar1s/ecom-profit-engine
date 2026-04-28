function fmtCell(v: unknown): string {
  if (v === null || v === undefined) return "—";
  const s = String(v).trim();
  return s || "—";
}

export function DataTable({
  rows,
  empty,
  monoCols,
}: {
  rows: Record<string, unknown>[];
  empty: string;
  monoCols?: Set<string>;
}) {
  if (!rows.length) {
    return (
      <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-5 py-10 text-center text-sm text-slate-600">
        {empty}
      </div>
    );
  }
  const cols = Object.keys(rows[0]);
  return (
    <div className="max-w-full overflow-x-auto rounded-xl border border-slate-200 bg-white">
      <table className="w-full min-w-[720px] border-collapse text-left text-sm">
        <thead>
          <tr className="border-b border-slate-200 bg-slate-50/90">
            {cols.map((c) => (
              <th key={c} className="sticky top-0 px-4 py-3 text-xs font-semibold uppercase tracking-wide text-slate-500">
                {c}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} className="border-b border-slate-100 transition-colors hover:bg-slate-50/80">
              {cols.map((c) => {
                const raw = row[c];
                const mono = monoCols?.has(c);
                return (
                  <td
                    key={c}
                    className={`px-4 py-2.5 text-slate-800 ${mono ? "font-mono text-[0.8rem]" : ""}`}
                  >
                    {fmtCell(raw)}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
