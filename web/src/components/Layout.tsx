import { NavLink, Outlet } from 'react-router-dom'
import { LayoutDashboard } from 'lucide-react'

const nav = [
  { to: '/', label: 'Dashboard', end: true },
  { to: '/orders', label: 'Orders' },
  { to: '/daily', label: 'Daily' },
  { to: '/marketing', label: 'Marketing' },
  { to: '/accounting', label: 'Accounting' },
  { to: '/costs', label: 'Costs' },
  { to: '/jobs', label: 'Jobs' },
]

function navClass({ isActive }: { isActive: boolean }) {
  return [
    'rounded-full px-4 py-2 text-sm font-medium transition-colors',
    isActive
      ? 'bg-[var(--color-accent-soft)] text-white shadow-[inset_0_1px_0_rgba(255,255,255,0.06)] ring-1 ring-[var(--color-border-strong)]'
      : 'text-[var(--color-muted)] hover:bg-white/[0.04] hover:text-[var(--color-text)]',
  ].join(' ')
}

export function Layout() {
  return (
    <div className="mx-auto max-w-[1380px] px-5 pb-16 pt-7 sm:px-6">
      <header className="mb-8 flex flex-col gap-6 lg:flex-row lg:items-end lg:justify-between">
        <div className="max-w-2xl space-y-3">
          <div className="flex items-center gap-2 text-3xl font-bold tracking-tight">
            <LayoutDashboard className="h-8 w-8 text-[var(--color-accent)] opacity-90" aria-hidden />
            <span>
              Ecom <span className="text-[var(--color-accent)]">Profit</span>
            </span>
          </div>
          <p className="text-[0.95rem] leading-relaxed text-[var(--color-muted)]">
            Interný operating dashboard pre Shopify, Meta, supplier costs a tracking. Google Sheets ostáva
            zdroj pravdy — operatíva je tu prehľadnejšia.
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          <span className="rounded-full border border-[var(--color-border)] bg-white/[0.03] px-3 py-1.5 text-xs text-[var(--color-muted)]">
            /app internal
          </span>
          <span className="rounded-full border border-[var(--color-border)] bg-white/[0.03] px-3 py-1.5 text-xs text-[var(--color-muted)]">
            Google Sheets backed
          </span>
        </div>
      </header>

      <nav
        className="mb-10 flex flex-wrap gap-2 rounded-2xl border border-[var(--color-border)] bg-[#080c14]/70 p-3 backdrop-blur-md"
        aria-label="Hlavná navigácia"
      >
        {nav.map((item) => (
          <NavLink key={item.to} to={item.to} end={item.end} className={navClass}>
            {item.label}
          </NavLink>
        ))}
        <a
          href="/app/logout"
          className="rounded-full px-4 py-2 text-sm font-medium text-[var(--color-muted)] hover:bg-white/[0.04] hover:text-[var(--color-text)]"
        >
          Odhlásiť
        </a>
      </nav>

      <main>
        <Outlet />
      </main>
    </div>
  )
}
