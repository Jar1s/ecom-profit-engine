const API = "/api/app";

async function parseJson(res: Response): Promise<unknown> {
  const text = await res.text();
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return { raw: text };
  }
}

export async function apiGet<T>(path: string): Promise<T> {
  const res = await fetch(`${API}${path}`, { credentials: "include" });
  if (res.status === 401) {
    window.location.assign("/app/login");
    throw new Error("Unauthorized");
  }
  if (!res.ok) {
    const body = await parseJson(res);
    const detail =
      typeof body === "object" && body !== null && "detail" in body
        ? String((body as { detail: unknown }).detail)
        : res.statusText;
    throw new Error(detail || `HTTP ${res.status}`);
  }
  return (await res.json()) as T;
}

export async function apiPostJson<T>(path: string): Promise<T> {
  const res = await fetch(`${API}${path}`, {
    method: "POST",
    credentials: "include",
    headers: { Accept: "application/json" },
  });
  if (res.status === 401) {
    window.location.assign("/app/login");
    throw new Error("Unauthorized");
  }
  const data = (await parseJson(res)) as Record<string, unknown> | T | null;
  if (!res.ok) {
    const o = data && typeof data === "object" ? (data as Record<string, unknown>) : null;
    const msg =
      (o && typeof o.message === "string" && o.message) ||
      (o && typeof o.error === "string" && o.error) ||
      (o && typeof o.detail === "string" && o.detail) ||
      res.statusText;
    throw new Error(msg || `HTTP ${res.status}`);
  }
  return data as T;
}
