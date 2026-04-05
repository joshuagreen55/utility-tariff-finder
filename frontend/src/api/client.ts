const BASE_URL = "/api";

export interface AuthMeResponse {
  auth_enabled: boolean;
  authenticated: boolean;
  email?: string;
  name?: string;
}

function headersForPath(path: string, extra?: HeadersInit): HeadersInit {
  const h: Record<string, string> = { "Content-Type": "application/json" };
  if (path.includes("/admin/")) {
    const key = import.meta.env.VITE_ADMIN_API_KEY || localStorage.getItem("admin_api_key");
    if (key) h["X-Admin-Key"] = key;
  }
  if (!extra) return h;
  const merged = new Headers(h);
  new Headers(extra).forEach((v, k) => merged.set(k, v));
  return merged;
}

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const resp = await fetch(`${BASE_URL}${path}`, {
    ...options,
    headers: headersForPath(path, options?.headers),
    credentials: "include",
  });
  if (!resp.ok) {
    throw new Error(`API error ${resp.status}: ${resp.statusText}`);
  }
  return resp.json();
}

export interface Utility {
  id: number;
  name: string;
  country: string;
  state_province: string;
  utility_type: string;
  tariff_count: number;
}

export interface UtilityDetail extends Utility {
  eia_id: number | null;
  website_url: string | null;
  is_active: boolean;
  created_at: string;
  updated_at: string;
}

export interface RateComponent {
  id: number;
  component_type: string;
  unit: string;
  rate_value: number;
  tier_min_kwh: number | null;
  tier_max_kwh: number | null;
  tier_label: string | null;
  period_index: number | null;
  period_label: string | null;
  season: string | null;
  adjustment: number | null;
}

export interface Tariff {
  id: number;
  utility_id: number;
  name: string;
  code: string | null;
  customer_class: string;
  rate_type: string;
  is_default: boolean;
  effective_date: string | null;
  end_date: string | null;
  approved: boolean;
  last_verified_at: string | null;
  data_freshness: "current" | "aging" | "stale";
}

export interface TariffDetail extends Tariff {
  description: string | null;
  source_url: string | null;
  rate_components: RateComponent[];
  energy_schedule_weekday: number[][] | null;
  energy_schedule_weekend: number[][] | null;
  created_at: string;
  updated_at: string;
}

export interface TariffBrowseItem {
  id: number;
  utility_id: number;
  utility_name: string;
  country: string;
  state_province: string;
  name: string;
  code: string | null;
  customer_class: string;
  rate_type: string;
  is_default: boolean;
  effective_date: string | null;
  component_count: number;
  data_freshness: "current" | "aging" | "stale";
}

export interface TariffBrowseResponse {
  items: TariffBrowseItem[];
  total: number;
  limit: number;
  offset: number;
}

export interface UtilityMatch {
  id: number;
  name: string;
  country: string;
  state_province: string;
  utility_type: string;
  match_method: string;
  residential_tariff_count: number;
  commercial_tariff_count: number;
}

export interface LookupResponse {
  geocoded: {
    latitude: number;
    longitude: number;
    formatted_address: string | null;
  } | null;
  utilities: UtilityMatch[];
}

export interface MonitoringSource {
  id: number;
  utility_id: number;
  utility_name: string;
  url: string;
  check_frequency_days: number;
  last_checked_at: string | null;
  last_changed_at: string | null;
  status: string;
}

export interface MonitoringLog {
  id: number;
  source_id: number;
  checked_at: string;
  content_hash: string;
  changed: boolean;
  diff_summary: string | null;
  review_status: string;
}

export interface RefreshRun {
  id: number;
  refresh_type: string;
  started_at: string | null;
  finished_at: string | null;
  duration_minutes: number | null;
  utilities_targeted: number;
  utilities_processed: number;
  tariffs_added: number;
  tariffs_updated: number;
  tariffs_stale: number;
  errors: number;
  summary_json: Record<string, unknown> | null;
  error_details?: string | null;
}

export interface RefreshRunsResponse {
  total: number;
  limit: number;
  offset: number;
  runs: RefreshRun[];
}

export const api = {
  authMe: () => request<AuthMeResponse>("/auth/me"),

  logout: () =>
    fetch(`${BASE_URL}/auth/logout`, { method: "POST", credentials: "include" }).then(async (resp) => {
      if (!resp.ok) throw new Error(`Logout failed ${resp.status}`);
      return resp.json() as Promise<{ ok: boolean }>;
    }),

  lookup: (address: string) =>
    request<LookupResponse>(`/lookup?address=${encodeURIComponent(address)}`),

  listUtilities: (params?: {
    country?: string;
    state_province?: string;
    search?: string;
    limit?: number;
    offset?: number;
  }) => {
    const qs = new URLSearchParams();
    if (params?.country) qs.set("country", params.country);
    if (params?.state_province) qs.set("state_province", params.state_province);
    if (params?.search) qs.set("search", params.search);
    if (params?.limit) qs.set("limit", String(params.limit));
    if (params?.offset) qs.set("offset", String(params.offset));
    return request<Utility[]>(`/utilities?${qs}`);
  },

  getUtility: (id: number) => request<UtilityDetail>(`/utilities/${id}`),

  listTariffs: (utilityId: number, customerClass?: string) => {
    const qs = customerClass ? `?customer_class=${customerClass}` : "";
    return request<Tariff[]>(`/utilities/${utilityId}/tariffs${qs}`);
  },

  getTariff: (id: number) => request<TariffDetail>(`/tariffs/${id}`),

  tariffFilters: () =>
    request<Record<string, { states: Record<string, { id: number; name: string }[]> }>>("/tariffs/filters"),

  deleteTariff: (id: number) =>
    request<{ ok: boolean; deleted_tariff_id: number }>(`/tariffs/${id}`, {
      method: "DELETE",
    }),

  browseTariffs: (params?: {
    country?: string;
    state_province?: string;
    utility_search?: string;
    utility_id?: number;
    customer_class?: string;
    rate_type?: string;
    limit?: number;
    offset?: number;
  }) => {
    const qs = new URLSearchParams();
    if (params?.country) qs.set("country", params.country);
    if (params?.state_province) qs.set("state_province", params.state_province);
    if (params?.utility_id) qs.set("utility_id", String(params.utility_id));
    else if (params?.utility_search) qs.set("utility_search", params.utility_search);
    if (params?.customer_class) qs.set("customer_class", params.customer_class);
    if (params?.rate_type) qs.set("rate_type", params.rate_type);
    if (params?.limit) qs.set("limit", String(params.limit));
    if (params?.offset) qs.set("offset", String(params.offset));
    return request<TariffBrowseResponse>(`/tariffs/browse?${qs}`);
  },

  listMonitoringSources: (status?: string) => {
    const qs = status ? `?status=${status}` : "";
    return request<MonitoringSource[]>(`/admin/monitoring/sources${qs}`);
  },

  listMonitoringLogs: (params?: {
    source_id?: number;
    changed_only?: boolean;
    review_status?: string;
  }) => {
    const qs = new URLSearchParams();
    if (params?.source_id) qs.set("source_id", String(params.source_id));
    if (params?.changed_only) qs.set("changed_only", "true");
    if (params?.review_status) qs.set("review_status", params.review_status);
    return request<MonitoringLog[]>(`/admin/monitoring/logs?${qs}`);
  },

  updateMonitoringLog: (logId: number, reviewStatus: string) =>
    request<MonitoringLog>(`/admin/monitoring/logs/${logId}`, {
      method: "PATCH",
      body: JSON.stringify({ review_status: reviewStatus }),
    }),

  monitoringStats: () =>
    request<{
      total_sources: number;
      checked: number;
      unchecked: number;
      changed: number;
      errors: number;
      pending_reviews: number;
    }>("/admin/monitoring/stats"),

  triggerCheckSingle: (sourceId: number) =>
    request<{ status: string; error?: string; content_hash?: string }>(
      `/admin/monitoring/sources/${sourceId}/check`,
      { method: "POST" }
    ),

  triggerCheckAll: (stateProvince?: string) => {
    const qs = stateProvince ? `?state_province=${stateProvince}` : "";
    return request<{ message: string; count: number }>(
      `/admin/monitoring/check-all${qs}`,
      { method: "POST" }
    );
  },

  listRefreshRuns: (limit = 20, offset = 0) =>
    request<RefreshRunsResponse>(`/admin/monitoring/refresh-runs?limit=${limit}&offset=${offset}`),

  getRefreshRun: (runId: number) =>
    request<RefreshRun>(`/admin/monitoring/refresh-runs/${runId}`),
};
