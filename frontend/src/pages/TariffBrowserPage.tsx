import { useEffect, useState, useCallback, useMemo } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { api, type TariffBrowseItem, type TariffBrowseResponse } from "../api/client";

const RATE_TYPES = [
  { value: "", label: "All Rate Types" },
  { value: "flat", label: "Flat" },
  { value: "tou", label: "Time-of-Use" },
  { value: "tiered", label: "Tiered" },
  { value: "demand", label: "Demand" },
  { value: "seasonal", label: "Seasonal" },
  { value: "tou_tiered", label: "TOU + Tiered" },
  { value: "seasonal_tou", label: "Seasonal TOU" },
  { value: "seasonal_tiered", label: "Seasonal Tiered" },
  { value: "demand_tou", label: "Demand TOU" },
  { value: "complex", label: "Complex" },
];

const CUSTOMER_CLASSES = [
  { value: "", label: "All Classes" },
  { value: "residential", label: "Residential" },
  { value: "commercial", label: "Commercial" },
  { value: "industrial", label: "Industrial" },
  { value: "lighting", label: "Lighting" },
];

const COUNTRY_LABELS: Record<string, string> = {
  US: "United States",
  CA: "Canada",
};

const PAGE_SIZE = 50;

type FilterData = Record<string, { states: Record<string, { id: number; name: string }[]> }>;

function formatRateType(rt: string): string {
  return rt.replace(/_/g, " ").replace(/\btou\b/gi, "TOU");
}

export default function TariffBrowserPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [data, setData] = useState<TariffBrowseResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [filterData, setFilterData] = useState<FilterData | null>(null);
  const [deleting, setDeleting] = useState<Set<number>>(new Set());

  const country = searchParams.get("country") || "";
  const stateProvince = searchParams.get("state") || "";
  const utilityId = searchParams.get("utility") || "";
  const customerClass = searchParams.get("class") || "";
  const rateType = searchParams.get("type") || "";
  const page = parseInt(searchParams.get("page") || "1", 10);

  useEffect(() => {
    api.tariffFilters().then(setFilterData).catch(() => {});
  }, []);

  const countryOptions = useMemo(() => {
    if (!filterData) return [];
    return Object.keys(filterData).sort((a, b) => {
      if (a === "CA") return -1;
      if (b === "CA") return 1;
      return a.localeCompare(b);
    });
  }, [filterData]);

  const stateOptions = useMemo(() => {
    if (!filterData || !country) return [];
    const states = filterData[country]?.states;
    if (!states) return [];
    return Object.keys(states).filter(s => s !== "Unknown").sort();
  }, [filterData, country]);

  const utilityOptions = useMemo(() => {
    if (!filterData || !country) return [];
    const states = filterData[country]?.states;
    if (!states) return [];
    const statesMap = stateProvince ? { [stateProvince]: states[stateProvince] || [] } : states;
    const all: { id: number; name: string }[] = [];
    for (const arr of Object.values(statesMap)) {
      if (arr) all.push(...arr);
    }
    all.sort((a, b) => a.name.localeCompare(b.name));
    return all;
  }, [filterData, country, stateProvince]);

  const setFilter = useCallback(
    (key: string, value: string, alsoReset?: string[]) => {
      setSearchParams((prev) => {
        const next = new URLSearchParams(prev);
        if (value) next.set(key, value); else next.delete(key);
        next.delete("page");
        if (alsoReset) alsoReset.forEach((k) => next.delete(k));
        return next;
      });
    },
    [setSearchParams]
  );

  const setPage = useCallback(
    (p: number) => {
      setSearchParams((prev) => {
        const next = new URLSearchParams(prev);
        if (p > 1) next.set("page", String(p)); else next.delete("page");
        return next;
      });
    },
    [setSearchParams]
  );

  const handleDelete = useCallback(async (tariff: TariffBrowseItem) => {
    if (!window.confirm(`Delete "${tariff.name}" from ${tariff.utility_name}?`)) return;
    setDeleting((prev) => new Set(prev).add(tariff.id));
    try {
      await api.deleteTariff(tariff.id);
      setData((prev) => {
        if (!prev) return prev;
        return {
          ...prev,
          items: prev.items.filter((t) => t.id !== tariff.id),
          total: prev.total - 1,
        };
      });
    } catch (err) {
      alert(`Failed to delete: ${err instanceof Error ? err.message : err}`);
    } finally {
      setDeleting((prev) => {
        const next = new Set(prev);
        next.delete(tariff.id);
        return next;
      });
    }
  }, []);

  useEffect(() => {
    let cancelled = false;
    const fetchData = async () => {
      setLoading(true);
      try {
        const resp = await api.browseTariffs({
          country: country || undefined,
          state_province: stateProvince || undefined,
          utility_id: utilityId ? parseInt(utilityId, 10) : undefined,
          customer_class: customerClass || undefined,
          rate_type: rateType || undefined,
          limit: PAGE_SIZE,
          offset: (page - 1) * PAGE_SIZE,
        });
        if (!cancelled) setData(resp);
      } catch {
        if (!cancelled) setData(null);
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    fetchData();
    return () => { cancelled = true; };
  }, [country, stateProvince, utilityId, customerClass, rateType, page]);

  const totalPages = data ? Math.ceil(data.total / PAGE_SIZE) : 0;

  return (
    <div>
      <div className="card">
        <h2>Tariff Browser</h2>
        <p style={{ color: "var(--color-text-secondary)", fontSize: 14, marginBottom: 16 }}>
          Browse and filter all tariffs across every utility in the database.
        </p>

        <div className="filters">
          <select value={country} onChange={(e) => setFilter("country", e.target.value, ["state", "utility"])}>
            <option value="">All Countries</option>
            {countryOptions.map((c) => (
              <option key={c} value={c}>{COUNTRY_LABELS[c] || c}</option>
            ))}
          </select>

          <select
            value={stateProvince}
            onChange={(e) => setFilter("state", e.target.value, ["utility"])}
            disabled={!country}
          >
            <option value="">All States / Provinces</option>
            {stateOptions.map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>

          <select
            value={utilityId}
            onChange={(e) => setFilter("utility", e.target.value)}
            disabled={!country}
          >
            <option value="">All Utilities</option>
            {utilityOptions.map((u) => (
              <option key={u.id} value={String(u.id)}>{u.name}</option>
            ))}
          </select>

          <select value={customerClass} onChange={(e) => setFilter("class", e.target.value)}>
            {CUSTOMER_CLASSES.map((c) => (
              <option key={c.value} value={c.value}>{c.label}</option>
            ))}
          </select>

          <select value={rateType} onChange={(e) => setFilter("type", e.target.value)}>
            {RATE_TYPES.map((r) => (
              <option key={r.value} value={r.value}>{r.label}</option>
            ))}
          </select>
        </div>

        {data && !loading && (
          <div style={{ fontSize: 13, color: "var(--color-text-secondary)", marginBottom: 4 }}>
            {data.total.toLocaleString()} tariff{data.total !== 1 ? "s" : ""} found
          </div>
        )}
      </div>

      <div className="card">
        {loading ? (
          <div className="loading">Loading tariffs...</div>
        ) : !data || data.items.length === 0 ? (
          <div className="empty-state">
            <p>No tariffs found. Try adjusting your filters.</p>
          </div>
        ) : (
          <>
            <div className="table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>Utility</th>
                    <th>Location</th>
                    <th>Tariff Name</th>
                    <th>Class</th>
                    <th>Rate Type</th>
                    <th>Components</th>
                    <th>Freshness</th>
                    <th></th>
                  </tr>
                </thead>
                <tbody>
                  {data.items.map((t: TariffBrowseItem) => (
                    <tr key={t.id}>
                      <td>
                        <Link to={`/utilities/${t.utility_id}`} className="table-link">
                          {t.utility_name}
                        </Link>
                      </td>
                      <td style={{ whiteSpace: "nowrap" }}>
                        {t.state_province}, {t.country}
                      </td>
                      <td>
                        <Link to={`/tariffs/${t.id}`} className="table-link">
                          {t.name}
                        </Link>
                        {t.code && (
                          <span style={{ fontSize: 12, color: "var(--color-text-secondary)", marginLeft: 6 }}>
                            {t.code}
                          </span>
                        )}
                      </td>
                      <td>
                        <span className={`badge badge-${t.customer_class}`}>
                          {t.customer_class}
                        </span>
                      </td>
                      <td>
                        <span className={`badge badge-${t.rate_type.split("_")[0]}`}>
                          {formatRateType(t.rate_type)}
                        </span>
                      </td>
                      <td style={{ textAlign: "center" }}>{t.component_count}</td>
                      <td>
                        <span className={`badge badge-${t.data_freshness}`}>
                          {t.data_freshness}
                        </span>
                      </td>
                      <td>
                        <button
                          className="btn-delete"
                          disabled={deleting.has(t.id)}
                          onClick={() => handleDelete(t)}
                          title={`Delete ${t.name}`}
                        >
                          {deleting.has(t.id) ? "…" : "✕"}
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {totalPages > 1 && (
              <div className="pagination">
                <button
                  className="btn btn-sm"
                  disabled={page <= 1}
                  onClick={() => setPage(page - 1)}
                >
                  Previous
                </button>
                <span className="pagination-info">
                  Page {page} of {totalPages.toLocaleString()}
                </span>
                <button
                  className="btn btn-sm"
                  disabled={page >= totalPages}
                  onClick={() => setPage(page + 1)}
                >
                  Next
                </button>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
