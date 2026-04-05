import { useEffect, useState, useCallback } from "react";
import { api, type MonitoringSource, type MonitoringLog, type RefreshRun } from "../api/client";

interface MonitoringStats {
  total_sources: number;
  checked: number;
  unchecked: number;
  changed: number;
  errors: number;
  pending_reviews: number;
}

export default function MonitoringPage() {
  const [sources, setSources] = useState<MonitoringSource[]>([]);
  const [logs, setLogs] = useState<MonitoringLog[]>([]);
  const [refreshRuns, setRefreshRuns] = useState<RefreshRun[]>([]);
  const [selectedRun, setSelectedRun] = useState<RefreshRun | null>(null);
  const [stats, setStats] = useState<MonitoringStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [tab, setTab] = useState<"sources" | "changes" | "refreshes">("refreshes");
  const [statusFilter, setStatusFilter] = useState("");
  const [checking, setChecking] = useState(false);
  const [checkingSingle, setCheckingSingle] = useState<number | null>(null);
  const [batchMessage, setBatchMessage] = useState("");

  const loadStats = useCallback(async () => {
    try {
      const data = await api.monitoringStats();
      setStats(data);
    } catch {
      // ignore
    }
  }, []);

  useEffect(() => {
    loadStats();
  }, [loadStats]);

  useEffect(() => {
    const load = async () => {
      setLoading(true);
      try {
        if (tab === "sources") {
          const data = await api.listMonitoringSources(statusFilter || undefined);
          setSources(data);
        } else if (tab === "changes") {
          const data = await api.listMonitoringLogs({ changed_only: true, review_status: "pending" });
          setLogs(data);
        } else if (tab === "refreshes") {
          const data = await api.listRefreshRuns(20);
          setRefreshRuns(data.runs);
        }
      } catch {
        setSources([]);
        setLogs([]);
        setRefreshRuns([]);
      } finally {
        setLoading(false);
      }
    };
    load();
  }, [tab, statusFilter]);

  const handleReview = async (logId: number, status: string) => {
    try {
      await api.updateMonitoringLog(logId, status);
      setLogs((prev) => prev.filter((l) => l.id !== logId));
      loadStats();
    } catch {
      // ignore
    }
  };

  const handleCheckAll = async (stateProvince?: string) => {
    setChecking(true);
    setBatchMessage("");
    try {
      const data = await api.triggerCheckAll(stateProvince);
      setBatchMessage(`Queued ${data.count} sources for checking. Refresh in a few minutes to see results.`);
      setTimeout(() => {
        loadStats();
      }, 5000);
    } catch {
      setBatchMessage("Failed to trigger checks.");
    } finally {
      setChecking(false);
    }
  };

  const handleCheckSingle = async (sourceId: number) => {
    setCheckingSingle(sourceId);
    try {
      const data = await api.triggerCheckSingle(sourceId);
      setSources((prev) =>
        prev.map((s) =>
          s.id === sourceId
            ? {
                ...s,
                status: data.status === "error" ? "error" : data.status === "changed" ? "changed" : "unchanged",
                last_checked_at: new Date().toISOString(),
                last_changed_at: data.status === "changed" ? new Date().toISOString() : s.last_changed_at,
              }
            : s
        )
      );
      loadStats();
    } catch {
      // ignore
    } finally {
      setCheckingSingle(null);
    }
  };

  return (
    <div>
      <div className="card">
        <h2>Monitoring Dashboard</h2>
        <p style={{ color: "var(--color-text-secondary)", fontSize: 14, marginBottom: 16 }}>
          Automated checks of utility tariff source URLs. Detect when utilities update their rate schedules.
        </p>

        {stats && (
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))", gap: 12, marginBottom: 20 }}>
            <div className="stat-card">
              <div className="stat-value">{stats.total_sources.toLocaleString()}</div>
              <div className="stat-label">Total Sources</div>
            </div>
            <div className="stat-card">
              <div className="stat-value">{stats.checked.toLocaleString()}</div>
              <div className="stat-label">Checked</div>
            </div>
            <div className="stat-card">
              <div className="stat-value" style={{ color: "var(--color-warning)" }}>{stats.unchecked.toLocaleString()}</div>
              <div className="stat-label">Unchecked</div>
            </div>
            <div className="stat-card">
              <div className="stat-value" style={{ color: "var(--color-danger)" }}>{stats.changed.toLocaleString()}</div>
              <div className="stat-label">Changed</div>
            </div>
            <div className="stat-card">
              <div className="stat-value" style={{ color: "var(--color-danger)" }}>{stats.errors.toLocaleString()}</div>
              <div className="stat-label">Errors</div>
            </div>
            <div className="stat-card">
              <div className="stat-value" style={{ color: "var(--color-primary)" }}>{stats.pending_reviews.toLocaleString()}</div>
              <div className="stat-label">Pending Reviews</div>
            </div>
          </div>
        )}

        <div style={{ display: "flex", gap: 8, marginBottom: 16, flexWrap: "wrap" }}>
          <button className="btn btn-primary" onClick={() => handleCheckAll()} disabled={checking}>
            {checking ? "Queuing..." : "Check All (50 oldest)"}
          </button>
          <button className="btn" style={{ background: "#eff6ff", color: "var(--color-primary)" }} onClick={() => handleCheckAll("BC")} disabled={checking}>
            Check BC
          </button>
          <button className="btn" style={{ background: "#eff6ff", color: "var(--color-primary)" }} onClick={() => handleCheckAll("QC")} disabled={checking}>
            Check QC
          </button>
          <button className="btn" style={{ background: "#eff6ff", color: "var(--color-primary)" }} onClick={() => handleCheckAll("WA")} disabled={checking}>
            Check WA
          </button>
          <button className="btn" style={{ background: "#eff6ff", color: "var(--color-primary)" }} onClick={() => handleCheckAll("CA")} disabled={checking}>
            Check CA
          </button>
          <button className="btn" style={{ background: "#eff6ff", color: "var(--color-primary)" }} onClick={() => handleCheckAll("NY")} disabled={checking}>
            Check NY
          </button>
        </div>
        {batchMessage && (
          <p style={{ fontSize: 13, color: "var(--color-accent)", marginBottom: 12 }}>{batchMessage}</p>
        )}

        <div className="tab-bar">
          <button className={tab === "refreshes" ? "active" : ""} onClick={() => setTab("refreshes")}>
            Refresh History
          </button>
          <button className={tab === "sources" ? "active" : ""} onClick={() => setTab("sources")}>
            All Sources
          </button>
          <button className={tab === "changes" ? "active" : ""} onClick={() => setTab("changes")}>
            Pending Changes
          </button>
        </div>
      </div>

      <div className="card">
        {loading ? (
          <div className="loading">Loading...</div>
        ) : tab === "refreshes" ? (
          refreshRuns.length === 0 ? (
            <div className="empty-state">
              <p>No refresh runs yet. The first monthly refresh will run on the 1st of the month.</p>
            </div>
          ) : (
            <>
              {selectedRun && (
                <div style={{ marginBottom: 20, padding: 16, background: "var(--color-bg-secondary, #f8fafc)", borderRadius: 8, border: "1px solid var(--color-border, #e2e8f0)" }}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
                    <h3 style={{ margin: 0 }}>
                      Run #{selectedRun.id} — {selectedRun.refresh_type}
                    </h3>
                    <button className="btn btn-sm" onClick={() => setSelectedRun(null)} style={{ background: "#f1f5f9" }}>
                      Close
                    </button>
                  </div>
                  {selectedRun.summary_json && (
                    <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))", gap: 8, marginBottom: 12 }}>
                      {Object.entries(selectedRun.summary_json).map(([k, v]) => (
                        <div key={k} style={{ fontSize: 13 }}>
                          <span style={{ color: "var(--color-text-secondary, #64748b)" }}>{k.replace(/_/g, " ")}:</span>{" "}
                          <strong>{Array.isArray(v) ? (v as string[]).join(", ") : String(v)}</strong>
                        </div>
                      ))}
                    </div>
                  )}
                  {selectedRun.error_details && (
                    <details>
                      <summary style={{ cursor: "pointer", fontSize: 13, color: "var(--color-danger, #dc2626)" }}>
                        Error details ({selectedRun.errors} errors)
                      </summary>
                      <pre style={{ fontSize: 12, maxHeight: 200, overflow: "auto", marginTop: 8, padding: 8, background: "#fff", borderRadius: 4 }}>
                        {selectedRun.error_details}
                      </pre>
                    </details>
                  )}
                </div>
              )}
              <table>
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Type</th>
                    <th>Started</th>
                    <th>Duration</th>
                    <th>Targeted</th>
                    <th>Processed</th>
                    <th>Added</th>
                    <th>Updated</th>
                    <th>Stale</th>
                    <th>Errors</th>
                    <th></th>
                  </tr>
                </thead>
                <tbody>
                  {refreshRuns.map((run) => {
                    const isRunning = !run.finished_at;
                    return (
                      <tr key={run.id} style={isRunning ? { background: "var(--color-bg-info, #eff6ff)" } : undefined}>
                        <td>#{run.id}</td>
                        <td>
                          <span className={`badge badge-${run.refresh_type === "monthly" ? "unchanged" : run.refresh_type === "quarterly" ? "changed" : "pending"}`}>
                            {run.refresh_type}
                          </span>
                        </td>
                        <td>{run.started_at ? new Date(run.started_at).toLocaleString() : "—"}</td>
                        <td>
                          {isRunning ? (
                            <span style={{ color: "var(--color-primary, #2563eb)", fontWeight: 500 }}>Running...</span>
                          ) : run.duration_minutes != null ? (
                            `${run.duration_minutes} min`
                          ) : "—"}
                        </td>
                        <td>{run.utilities_targeted}</td>
                        <td>{run.utilities_processed}</td>
                        <td style={{ color: run.tariffs_added > 0 ? "var(--color-success, #16a34a)" : undefined, fontWeight: run.tariffs_added > 0 ? 600 : undefined }}>
                          {run.tariffs_added > 0 ? `+${run.tariffs_added}` : "0"}
                        </td>
                        <td>{run.tariffs_updated}</td>
                        <td style={{ color: run.tariffs_stale > 0 ? "var(--color-warning, #d97706)" : undefined }}>
                          {run.tariffs_stale}
                        </td>
                        <td style={{ color: run.errors > 0 ? "var(--color-danger, #dc2626)" : undefined, fontWeight: run.errors > 0 ? 600 : undefined }}>
                          {run.errors}
                        </td>
                        <td>
                          <button
                            className="btn btn-sm"
                            style={{ background: "#eff6ff", color: "var(--color-primary)" }}
                            onClick={async () => {
                              try {
                                const detail = await api.getRefreshRun(run.id);
                                setSelectedRun(detail);
                              } catch { /* ignore */ }
                            }}
                          >
                            Details
                          </button>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </>
          )
        ) : tab === "changes" ? (
          logs.length === 0 ? (
            <div className="empty-state">
              <p>No pending changes to review. Run some checks first.</p>
            </div>
          ) : (
            <table>
              <thead>
                <tr>
                  <th>Detected</th>
                  <th>Source</th>
                  <th>Diff Summary</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {logs.map((log) => (
                  <tr key={log.id}>
                    <td>{new Date(log.checked_at).toLocaleString()}</td>
                    <td>Source #{log.source_id}</td>
                    <td style={{ maxWidth: 400, overflow: "hidden", textOverflow: "ellipsis" }}>
                      {log.diff_summary || "Content changed"}
                    </td>
                    <td>
                      <button
                        className="btn btn-sm btn-primary"
                        onClick={() => handleReview(log.id, "reviewed")}
                        style={{ marginRight: 8 }}
                      >
                        Mark Reviewed
                      </button>
                      <button
                        className="btn btn-sm"
                        onClick={() => handleReview(log.id, "dismissed")}
                        style={{ background: "#f1f5f9" }}
                      >
                        Dismiss
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )
        ) : (
          <>
            <div className="filters">
              <select value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)}>
                <option value="">All Statuses</option>
                <option value="changed">Changed</option>
                <option value="unchanged">Unchanged</option>
                <option value="error">Error</option>
                <option value="pending">Pending</option>
              </select>
            </div>
            {sources.length === 0 ? (
              <div className="empty-state">
                <p>No monitoring sources found{statusFilter ? ` with status "${statusFilter}"` : ""}.</p>
              </div>
            ) : (
              <table>
                <thead>
                  <tr>
                    <th>Utility</th>
                    <th>URL</th>
                    <th>Status</th>
                    <th>Last Checked</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {sources.map((s) => (
                    <tr key={s.id}>
                      <td>{s.utility_name}</td>
                      <td>
                        <a href={s.url} target="_blank" rel="noreferrer" className="source-link">
                          {s.url.length > 55 ? s.url.slice(0, 55) + "..." : s.url}
                        </a>
                      </td>
                      <td>
                        <span className={`badge badge-${s.status}`}>{s.status}</span>
                      </td>
                      <td>{s.last_checked_at ? new Date(s.last_checked_at).toLocaleDateString() : "Never"}</td>
                      <td>
                        <button
                          className="btn btn-sm"
                          style={{ background: "#eff6ff", color: "var(--color-primary)" }}
                          onClick={() => handleCheckSingle(s.id)}
                          disabled={checkingSingle === s.id}
                        >
                          {checkingSingle === s.id ? "Checking..." : "Check Now"}
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </>
        )}
      </div>
    </div>
  );
}
