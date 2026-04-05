import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import { api, type TariffDetail } from "../api/client";

export default function TariffDetailPage() {
  const { id } = useParams<{ id: string }>();
  const [tariff, setTariff] = useState<TariffDetail | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!id) return;
    api.getTariff(Number(id)).then(setTariff).catch(() => setTariff(null)).finally(() => setLoading(false));
  }, [id]);

  if (loading) return <div className="loading">Loading...</div>;
  if (!tariff) return <div className="card empty-state"><p>Tariff not found.</p></div>;

  const energyComponents = tariff.rate_components.filter((c) => c.component_type === "energy");
  const demandComponents = tariff.rate_components.filter((c) => c.component_type === "demand");
  const fixedComponents = tariff.rate_components.filter(
    (c) => c.component_type === "fixed" || c.component_type === "minimum"
  );

  const hasSeason = (comps: typeof tariff.rate_components) =>
    comps.some((c) => c.season && c.season.trim() !== "");

  const groupBySeason = (comps: typeof tariff.rate_components) => {
    const groups: Record<string, typeof comps> = {};
    for (const c of comps) {
      const key = c.season?.trim() || "All Seasons";
      if (!groups[key]) groups[key] = [];
      groups[key].push(c);
    }
    return groups;
  };

  const renderComponentTable = (comps: typeof tariff.rate_components, isEnergy: boolean) => {
    const seasonal = hasSeason(comps);
    const groups = seasonal ? groupBySeason(comps) : { "": comps };

    return Object.entries(groups).map(([season, items]) => (
      <div key={season} style={seasonal ? { marginBottom: 20 } : undefined}>
        {seasonal && (
          <h3 style={{ fontSize: 15, fontWeight: 600, margin: "12px 0 6px", color: "var(--color-text-secondary, #555)" }}>
            {season}
          </h3>
        )}
        <table>
          <thead>
            <tr>
              <th>Period</th>
              <th>Tier</th>
              <th>{isEnergy ? "Rate ($/kWh)" : "Rate"}</th>
              {isEnergy && <th>Adjustment</th>}
              <th>Unit</th>
            </tr>
          </thead>
          <tbody>
            {items.map((c) => (
              <tr key={c.id}>
                <td>{c.period_label || (c.period_index !== null ? `Period ${c.period_index}` : "All")}</td>
                <td>
                  {c.tier_min_kwh !== null || c.tier_max_kwh !== null
                    ? `${c.tier_min_kwh ?? 0} - ${c.tier_max_kwh ?? "+"} kWh`
                    : "All usage"}
                </td>
                <td>${isEnergy ? c.rate_value.toFixed(5) : c.rate_value.toFixed(2)}</td>
                {isEnergy && <td>{c.adjustment ? `$${c.adjustment.toFixed(5)}` : ""}</td>}
                <td>{c.unit}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    ));
  };

  return (
    <div>
      <div className="card">
        <div style={{ marginBottom: 12 }}>
          <Link to={`/utilities/${tariff.utility_id}`} className="source-link" style={{ fontSize: 13 }}>
            &larr; Back to utility
          </Link>
        </div>
        <h2>{tariff.name}</h2>
        <div style={{ display: "flex", gap: 8, marginBottom: 16, flexWrap: "wrap" }}>
          <span className={`badge badge-${tariff.customer_class}`}>{tariff.customer_class}</span>
          <span
            className={`badge badge-${tariff.rate_type.includes("tou") ? "tou" : tariff.rate_type}`}
          >
            {tariff.rate_type.replace(/_/g, " ").toUpperCase()}
          </span>
          {tariff.is_default && <span className="badge badge-unchanged">DEFAULT</span>}
          <span className={`badge badge-${tariff.data_freshness}`}>
            {tariff.data_freshness === "current"
              ? "Current data"
              : tariff.data_freshness === "aging"
              ? "Aging data"
              : "Stale data"}
          </span>
        </div>
        <dl className="detail-grid">
          {tariff.code && (
            <>
              <dt>Code</dt>
              <dd>{tariff.code}</dd>
            </>
          )}
          <dt>Effective Date</dt>
          <dd>{tariff.effective_date || "N/A"}</dd>
          <dt>End Date</dt>
          <dd>{tariff.end_date || "N/A (current)"}</dd>
          {tariff.description && (
            <>
              <dt>Description</dt>
              <dd>{tariff.description}</dd>
            </>
          )}
          <dt>Source</dt>
          <dd>
            {tariff.source_url ? (
              <a href={tariff.source_url} target="_blank" rel="noreferrer" className="source-link">
                {tariff.source_url}
              </a>
            ) : (
              "N/A"
            )}
          </dd>
          <dt>Last Verified</dt>
          <dd>
            {tariff.last_verified_at ? (
              <span className="verified-badge">
                {new Date(tariff.last_verified_at).toLocaleString()}
              </span>
            ) : (
              <span className="stale-badge">Not yet verified</span>
            )}
          </dd>
          {tariff.data_freshness === "stale" && (
            <>
              <dt>Data Warning</dt>
              <dd style={{ color: "var(--color-danger)", fontSize: 13 }}>
                This rate data is over 5 years old and may not reflect current pricing.
                Check the utility's website for the latest rates.
              </dd>
            </>
          )}
          {tariff.data_freshness === "aging" && (
            <>
              <dt>Data Warning</dt>
              <dd style={{ color: "var(--color-warning)", fontSize: 13 }}>
                This rate data is 2-5 years old. Rates may have changed since then.
              </dd>
            </>
          )}
        </dl>
      </div>

      {fixedComponents.length > 0 && (
        <div className="card">
          <h2>Fixed Charges</h2>
          <table>
            <thead>
              <tr>
                <th>Type</th>
                {fixedComponents.some((c) => c.tier_label) && <th>Applies To</th>}
                {fixedComponents.some((c) => c.season) && <th>Season</th>}
                <th>Rate</th>
                <th>Unit</th>
              </tr>
            </thead>
            <tbody>
              {fixedComponents.map((c) => (
                <tr key={c.id}>
                  <td>{c.component_type}</td>
                  {fixedComponents.some((fc) => fc.tier_label) && (
                    <td>{c.tier_label || "All"}</td>
                  )}
                  {fixedComponents.some((fc) => fc.season) && (
                    <td>{c.season || "All"}</td>
                  )}
                  <td>${c.rate_value.toFixed(2)}</td>
                  <td>{c.unit}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {energyComponents.length > 0 && (
        <div className="card">
          <h2>Energy Rates</h2>
          {renderComponentTable(energyComponents, true)}
        </div>
      )}

      {demandComponents.length > 0 && (
        <div className="card">
          <h2>Demand Charges</h2>
          {renderComponentTable(demandComponents, false)}
        </div>
      )}
    </div>
  );
}
