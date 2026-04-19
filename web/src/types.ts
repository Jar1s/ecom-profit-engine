export type UiStatus = {
  sheet_url: string | null
  supplier_tab: string
  sheet_title_hint: string | null
  cron_secret_set: boolean
}

export type KpiCard = { label: string; value: string; meta: string }

export type RunRow = { job: string; last_run: string; purpose: string }

export type RunResponse = { runs: RunRow[] }

export type DashboardResponse = {
  status: UiStatus
  cards: KpiCard[]
  runs: RunRow[]
  recent_orders: Record<string, unknown>[]
  recent_daily: Record<string, unknown>[]
}

export type RowsResponse = { rows: Record<string, unknown>[] }

export type MarketingResponse = {
  meta_daily: Record<string, unknown>[]
  campaigns: Record<string, unknown>[]
}

export type PipelineRunResult = {
  ok: boolean
  exitCode?: number
  mode: string
  message?: string
  error?: string
}
