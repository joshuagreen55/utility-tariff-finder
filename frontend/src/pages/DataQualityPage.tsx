import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { api, type DataQualityOverview } from "../api/client";

// Domains the pipeline already hard-blocks. Anything in this list that
// shows up in the top-domains panel is historical contamination that the
// next purge_aggregator_contamination run will sweep up.
const KNOWN_AGGREGATOR_HINTS = [
  "energybot",
  "comparepower",
  "energyrate",
  "electricityrate",
  "ohenergyratings",
  "texaselectricityratings",
  "vaultelectricity",
  "maenergyratings",
  "energypal",
  "chooseenergy",
  "wattbuy",
  "saveonenergy",
  "findenergy",
  "energysage",
  "powertochoose",
  "njenergyratings",
  "openei",
  "ecowatch",
  "qmerit",
  "uselectricgrid",
  "getcurrents",
  "xoomenergy",
  "energysavings",
  "utilitiesformyhome",
];

function isAggregator(domain: string): boolean {
  const d = domain.toLowerCase();
  return KNOWN_AGGREGATOR_HINTS.some((k) => d.includes(k));
}

export default function DataQualityPage() {
  const [data, setData] = useState<DataQualityOverview | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>("");

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const overview = await api.dataQualityOverview();
        if (!cancelled) {
          setData(overview);
          setError("");
        }
      } catch (e) {
        if (!cancelled) {
          setError(e instanceof Error ? e.message : "Failed to load data quality overview");
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  if (loading) {
    return (
      <div className="card">
        <div className="loading">Loading data quality overview…</div>
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="card">
        <p style={{ color: "var(--color-danger, #dc2626)" }}>
          {error || "No data available"}
        </p>
      </div>
    );
  }

  const { headline, top_source_domains, utility_outliers, freshness, confidence } = data;

  const totalForFresh =
    freshness.current + freshness.aging + freshness.stale + freshness.never_verified;
  const totalForConf =
    confidence.high + confidence.medium + confidence.low + confidence.unscored;

  const pctOf = (n: number, total: number) =>
    total ? Math.round((n / total) * 1000) / 10 : 0;

  return (
    <div>
      <div className="card">
        <h2>Data Quality</h2>
        <p style={{ color: "var(--color-text-secondary)", fontSize: 14, marginBottom: 16 }}>
          Health check of the tariff dataset. Flags suspected aggregator
          contamination, utility outliers (mis-attribution risk), staleness,
          and confidence distribution.
        </p>

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))",
            gap: 12,
            marginBottom: 20,
          }}
        >
          <div className="stat-card">
            <div className="stat-value">{headline.total_tariffs.toLocaleString()}</div>
            <div className="stat-label">Tariffs</div>
          </div>
          <div className="stat-card">
            <div className="stat-value">{headline.utilities_with_tariffs.toLocaleString()}</div>
            <div className="stat-label">Utilities with tariffs</div>
          </div>
          <div className="stat-card">
            <div className="stat-value">{headline.coverage_pct.toFixed(1)}%</div>
            <div className="stat-label">Active-utility coverage</div>
          </div>
          <div className="stat-card">
            <div
              className="stat-value"
              style={{
                color:
                  headline.tariffs_no_source > 0
                    ? "var(--color-danger, #dc2626)"
                    : "var(--color-success, #16a34a)",
              }}
            >
              {headline.tariffs_no_source.toLocaleString()}
            </div>
            <div className="stat-label">Tariffs missing source URL</div>
          </div>
          <div className="stat-card">
            <div
              className="stat-value"
              style={{
                color:
                  headline.tariffs_no_confidence > 0
                    ? "var(--color-warning, #d97706)"
                    : "var(--color-success, #16a34a)",
              }}
            >
              {headline.tariffs_no_confidence.toLocaleString()}
            </div>
            <div className="stat-label">Tariffs missing confidence</div>
          </div>
        </div>
      </div>

      <div className="card">
        <h3>Freshness (last_verified_at)</h3>
        <p style={{ color: "var(--color-text-secondary)", fontSize: 13, marginBottom: 12 }}>
          When was each tariff last re-verified by the pipeline? Stale means
          we haven't touched it in over 90 days.
        </p>
        <table>
          <thead>
            <tr>
              <th>Bucket</th>
              <th style={{ textAlign: "right" }}>Tariffs</th>
              <th style={{ textAlign: "right" }}>Share</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>Current (&lt;30 days)</td>
              <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                {freshness.current.toLocaleString()}
              </td>
              <td style={{ textAlign: "right" }}>{pctOf(freshness.current, totalForFresh)}%</td>
            </tr>
            <tr>
              <td>Aging (30–90 days)</td>
              <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                {freshness.aging.toLocaleString()}
              </td>
              <td style={{ textAlign: "right" }}>{pctOf(freshness.aging, totalForFresh)}%</td>
            </tr>
            <tr>
              <td style={{ color: "var(--color-warning, #d97706)" }}>Stale (&gt;90 days)</td>
              <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                {freshness.stale.toLocaleString()}
              </td>
              <td style={{ textAlign: "right" }}>{pctOf(freshness.stale, totalForFresh)}%</td>
            </tr>
            <tr>
              <td style={{ color: "var(--color-text-secondary)" }}>Never verified</td>
              <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                {freshness.never_verified.toLocaleString()}
              </td>
              <td style={{ textAlign: "right" }}>
                {pctOf(freshness.never_verified, totalForFresh)}%
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <div className="card">
        <h3>Top source domains</h3>
        <p style={{ color: "var(--color-text-secondary)", fontSize: 13, marginBottom: 12 }}>
          Where our tariff data comes from. Rows highlighted in red are
          aggregator domains that are already in the pipeline blocklist —
          if you see one here, run the contamination cleanup script. New,
          unfamiliar domains are worth investigating.
        </p>
        <table>
          <thead>
            <tr>
              <th>Domain</th>
              <th style={{ textAlign: "right" }}>Tariffs</th>
              <th style={{ textAlign: "right" }}>Distinct utilities</th>
            </tr>
          </thead>
          <tbody>
            {top_source_domains.map((d) => {
              const flagged = isAggregator(d.domain);
              return (
                <tr key={d.domain} style={flagged ? { background: "#fef2f2" } : undefined}>
                  <td>
                    {d.domain}
                    {flagged && (
                      <span
                        style={{
                          marginLeft: 8,
                          fontSize: 11,
                          color: "var(--color-danger, #dc2626)",
                          fontWeight: 600,
                        }}
                      >
                        AGGREGATOR
                      </span>
                    )}
                  </td>
                  <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                    {d.tariff_count.toLocaleString()}
                  </td>
                  <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                    {d.utility_count.toLocaleString()}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <div className="card">
        <h3>Utility outliers (≥25 tariffs)</h3>
        <p style={{ color: "var(--color-text-secondary)", fontSize: 13, marginBottom: 12 }}>
          Utilities with abnormally high tariff counts — usually a signal
          that the LLM grabbed rates from neighboring utilities on a
          comparison page. Click to inspect.
        </p>
        {utility_outliers.length === 0 ? (
          <div className="empty-state">
            <p>No outliers ≥25 tariffs. Healthy distribution.</p>
          </div>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Utility</th>
                <th>Region</th>
                <th style={{ textAlign: "right" }}>Tariff count</th>
              </tr>
            </thead>
            <tbody>
              {utility_outliers.map((u) => (
                <tr key={u.id}>
                  <td>
                    <Link to={`/utilities/${u.id}`}>{u.name}</Link>
                  </td>
                  <td>
                    {u.country ? `${u.country} ` : ""}
                    {u.state_province || ""}
                  </td>
                  <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                    {u.tariff_count.toLocaleString()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      <div className="card">
        <h3>Confidence distribution</h3>
        <table>
          <thead>
            <tr>
              <th>Bucket</th>
              <th style={{ textAlign: "right" }}>Tariffs</th>
              <th style={{ textAlign: "right" }}>Share</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>High (≥0.85)</td>
              <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                {confidence.high.toLocaleString()}
              </td>
              <td style={{ textAlign: "right" }}>{pctOf(confidence.high, totalForConf)}%</td>
            </tr>
            <tr>
              <td>Medium (0.5–0.85)</td>
              <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                {confidence.medium.toLocaleString()}
              </td>
              <td style={{ textAlign: "right" }}>{pctOf(confidence.medium, totalForConf)}%</td>
            </tr>
            <tr>
              <td style={{ color: "var(--color-warning, #d97706)" }}>Low (&lt;0.5)</td>
              <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                {confidence.low.toLocaleString()}
              </td>
              <td style={{ textAlign: "right" }}>{pctOf(confidence.low, totalForConf)}%</td>
            </tr>
            <tr>
              <td style={{ color: "var(--color-text-secondary)" }}>Unscored</td>
              <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                {confidence.unscored.toLocaleString()}
              </td>
              <td style={{ textAlign: "right" }}>{pctOf(confidence.unscored, totalForConf)}%</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  );
}
