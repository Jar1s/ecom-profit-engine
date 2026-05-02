import type { ExecutiveThreshold } from "@/types";

/** Must match keys in `api/dashboard.py` `_EXEC_THRESH_DEFAULTS`. */
export const DEFAULT_EXECUTIVE_THRESHOLDS: Record<string, ExecutiveThreshold> = {
  "ROAS 30d": { mode: "higher_better", green: 2.0, amber: 1.4 },
  "Gross Margin": { mode: "higher_better", green: 35, amber: 25 },
  "Ad Share": { mode: "lower_better", green: 20, amber: 30 },
  "Delivery Rate": { mode: "higher_better", green: 85, amber: 70 },
  Undelivered: { mode: "lower_better", green: 15, amber: 30 },
  "Undelivered Now": { mode: "lower_better", green: 15, amber: 30 },
  "Undelivered 30d": { mode: "lower_better", green: 60, amber: 120 },
  "Avg Transit Days": { mode: "lower_better", green: 5, amber: 8 },
  "Profit After Ads": { mode: "higher_better", green: 0, amber: -500 },
  "Payment net 30d": { mode: "higher_better", green: 0, amber: -500 },
};

function isThreshold(v: unknown): v is ExecutiveThreshold {
  if (!v || typeof v !== "object") return false;
  const o = v as Record<string, unknown>;
  if (o.mode !== "higher_better" && o.mode !== "lower_better") return false;
  if (typeof o.green !== "number" || typeof o.amber !== "number") return false;
  return Number.isFinite(o.green) && Number.isFinite(o.amber);
}

/** Merge API payload (from FastAPI env) over local defaults. */
export function mergeExecutiveThresholds(
  api: Record<string, ExecutiveThreshold> | undefined,
): Record<string, ExecutiveThreshold> {
  const out: Record<string, ExecutiveThreshold> = { ...DEFAULT_EXECUTIVE_THRESHOLDS };
  if (!api) return out;
  for (const [key, val] of Object.entries(api)) {
    if (!(key in DEFAULT_EXECUTIVE_THRESHOLDS)) continue;
    if (!isThreshold(val)) continue;
    out[key] = val;
  }
  return out;
}
