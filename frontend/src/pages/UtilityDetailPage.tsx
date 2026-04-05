import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import { api, type UtilityDetail, type Tariff } from "../api/client";

export default function UtilityDetailPage() {
  const { id } = useParams<{ id: string }>();
  const [utility, setUtility] = useState<UtilityDetail | null>(null);
  const [tariffs, setTariffs] = useState<Tariff[]>([]);
  const [loading, setLoading] = useState(true);
  const [customerClass, setCustomerClass] = useState("");

  useEffect(() => {
    if (!id) return;
    const load = async () => {
      setLoading(true);
      try {
        const [u, t] = await Promise.all([
          api.getUtility(Number(id)),
          api.listTariffs(Number(id), customerClass || undefined),
        ]);
        setUtility(u);
        setTariffs(t);
      } catch {
        setUtility(null);
        setTariffs([]);
      } finally {
        setLoading(false);
      }
    };
    load();
  }, [id, customerClass]);

  if (loading) return <div className="loading">Loading...</div>;
  if (!utility) return <div className="card empty-state"><p>Utility not found.</p></div>;

  return (
    <div>
      <div className="card">
        <h2>{utility.name}</h2>
        <dl className="detail-grid">
          <dt>EIA ID</dt>
          <dd>{utility.eia_id || "N/A"}</dd>
          <dt>Country</dt>
          <dd>{utility.country}</dd>
          <dt>State/Province</dt>
          <dd>{utility.state_province}</dd>
          <dt>Type</dt>
          <dd>{utility.utility_type.replace(/_/g, " ")}</dd>
          <dt>Website</dt>
          <dd>
            {utility.website_url ? (
              <a href={utility.website_url} target="_blank" rel="noreferrer" className="source-link">
                {utility.website_url}
              </a>
            ) : (
              "N/A"
            )}
          </dd>
        </dl>
      </div>

      <div className="card">
        <h2>Rate Tariffs ({tariffs.length})</h2>
        <div className="tab-bar">
          <button className={!customerClass ? "active" : ""} onClick={() => setCustomerClass("")}>
            All
          </button>
          <button
            className={customerClass === "residential" ? "active" : ""}
            onClick={() => setCustomerClass("residential")}
          >
            Residential
          </button>
          <button
            className={customerClass === "commercial" ? "active" : ""}
            onClick={() => setCustomerClass("commercial")}
          >
            Commercial
          </button>
          <button
            className={customerClass === "industrial" ? "active" : ""}
            onClick={() => setCustomerClass("industrial")}
          >
            Industrial
          </button>
        </div>

        {tariffs.length === 0 ? (
          <div className="empty-state">
            <p>No tariffs found for this utility{customerClass ? ` (${customerClass})` : ""}.</p>
          </div>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>Customer Class</th>
                <th>Rate Type</th>
                <th>Default</th>
                <th>Effective</th>
                <th>Data Freshness</th>
              </tr>
            </thead>
            <tbody>
              {tariffs.map((t) => (
                <tr key={t.id}>
                  <td>
                    <Link to={`/tariffs/${t.id}`} className="table-link">
                      {t.name}
                    </Link>
                  </td>
                  <td>
                    <span className={`badge badge-${t.customer_class}`}>{t.customer_class}</span>
                  </td>
                  <td>
                    <span className={`badge badge-${t.rate_type.includes("tou") ? "tou" : t.rate_type}`}>
                      {t.rate_type.replace(/_/g, " ").toUpperCase()}
                    </span>
                  </td>
                  <td>{t.is_default ? "Yes" : ""}</td>
                  <td>{t.effective_date || "N/A"}</td>
                  <td>
                    <span className={`badge badge-${t.data_freshness}`}>
                      {t.data_freshness === "current"
                        ? "Current"
                        : t.data_freshness === "aging"
                        ? "Aging"
                        : "Stale"}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
