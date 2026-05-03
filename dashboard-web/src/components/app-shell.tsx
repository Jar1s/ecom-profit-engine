"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { LayoutDashboard, LogOut } from "lucide-react";
import { cn } from "@/lib/utils";

const nav = [
  { href: "/", label: "Dashboard", end: true },
  { href: "/orders", label: "Orders" },
  { href: "/daily", label: "Daily" },
  { href: "/marketing", label: "Marketing" },
  { href: "/costs", label: "Costs" },
  { href: "/jobs", label: "Jobs" },
];

export function AppShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();

  return (
    <div className="flex min-h-screen bg-slate-50 text-slate-900">
      <aside className="hidden w-56 shrink-0 border-r border-slate-200 bg-white lg:flex lg:flex-col">
        <div className="flex items-center gap-2 border-b border-slate-100 px-5 py-5">
          <LayoutDashboard className="h-7 w-7 text-slate-800" aria-hidden />
          <div className="leading-tight">
            <p className="text-sm font-bold tracking-tight">Ecom Profit</p>
            <p className="text-[11px] font-medium text-slate-500">Operating</p>
          </div>
        </div>
        <nav className="flex flex-1 flex-col gap-0.5 p-3" aria-label="Hlavná navigácia">
          {nav.map((item) => {
            const active = item.end ? pathname === "/" || pathname === "" : pathname === item.href;
            return (
              <Link
                key={item.href}
                href={item.href}
                className={cn(
                  "rounded-lg px-3 py-2 text-sm font-medium transition",
                  active ? "bg-slate-900 text-white shadow-sm" : "text-slate-600 hover:bg-slate-100 hover:text-slate-900",
                )}
              >
                {item.label}
              </Link>
            );
          })}
        </nav>
        <div className="border-t border-slate-100 p-3">
          <a
            href="/app/logout"
            className="flex items-center gap-2 rounded-lg px-3 py-2 text-sm font-medium text-slate-600 hover:bg-slate-100 hover:text-slate-900"
          >
            <LogOut className="h-4 w-4" aria-hidden />
            Odhlásiť
          </a>
        </div>
      </aside>

      <div className="flex min-w-0 flex-1 flex-col">
        <header className="border-b border-slate-200 bg-white/90 px-4 py-3 backdrop-blur lg:hidden">
          <div className="flex items-center justify-between gap-3">
            <div className="flex items-center gap-2">
              <LayoutDashboard className="h-6 w-6 text-slate-800" aria-hidden />
              <span className="text-sm font-bold">Ecom Profit</span>
            </div>
            <a href="/app/logout" className="text-xs font-semibold text-slate-600">
              Odhlásiť
            </a>
          </div>
          <nav className="mt-3 flex flex-wrap gap-1" aria-label="Hlavná navigácia">
            {nav.map((item) => {
              const active = item.end ? pathname === "/" || pathname === "" : pathname === item.href;
              return (
                <Link
                  key={item.href}
                  href={item.href}
                  className={cn(
                    "rounded-full px-3 py-1 text-xs font-semibold",
                    active ? "bg-slate-900 text-white" : "bg-slate-100 text-slate-700",
                  )}
                >
                  {item.label}
                </Link>
              );
            })}
          </nav>
        </header>

        <main className="mx-auto w-full max-w-[1400px] flex-1 px-4 py-8 sm:px-6 lg:px-10">{children}</main>
      </div>
    </div>
  );
}
