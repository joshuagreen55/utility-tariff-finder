import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { api, type Utility } from "../api/client";

export default function UtilitiesPage() {
  const [utilities, setUtilities] = useState<Utility[]>([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [country, setCountry] = useState("");
  const [stateFilter, setStateFilter] = useState("");

  const fetchUtilities = async () => {
    setLoading(true);
    try {
      const data = await api.listUtilities({
        search: search || undefined,
        country: country || undefined,
        state_province: stateFilter || undefined,
        limit: 100,
      });
      setUtilities(data);
    } catch {
      setUtilities([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchUtilities();
  }, [country, stateFilter]);

  return (
    <div>
      <div className="card">
        <h2>Utility Browser</h2>
        <div className="search-box">
          <input
            type="text"
            placeholder="Search utilities by name..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && fetchUtilities()}
          />
          <button className="btn btn-primary" onClick={fetchUtilities}>
            Search
          </button>
        </div>
        <div className="filters">
          <select value={country} onChange={(e) => setCountry(e.target.value)}>
            <option value="">All Countries</option>
            <option value="US">United States</option>
            <option value="CA">Canada</option>
          </select>
          <input
            type="text"
            placeholder="State/Province..."
            value={stateFilter}
            onChange={(e) => setStateFilter(e.target.value)}
            style={{
              padding: "8px 12px",
              border: "1px solid var(--color-border)",
              borderRadius: "var(--radius)",
              fontSize: 14,
              width: 160,
            }}
          />
        </div>
      </div>

      <div className="card">
        {loading ? (
          <div className="loading">Loading utilities...</div>
        ) : utilities.length === 0 ? (
          <div className="empty-state">
            <p>No utilities found. Try adjusting your search filters.</p>
          </div>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>Country</th>
                <th>State/Province</th>
                <th>Type</th>
                <th>Tariffs</th>
              </tr>
            </thead>
            <tbody>
              {utilities.map((u) => (
                <tr key={u.id}>
                  <td>
                    <Link to={`/utilities/${u.id}`} className="table-link">
                      {u.name}
                    </Link>
                  </td>
                  <td>{u.country}</td>
                  <td>{u.state_province}</td>
                  <td>{u.utility_type.replace(/_/g, " ")}</td>
                  <td>{u.tariff_count}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
