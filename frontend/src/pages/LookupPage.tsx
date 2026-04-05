import { useState, useEffect, useCallback } from "react";
import { api, type Tariff, type TariffDetail, type RateComponent, type UtilityMatch } from "../api/client";
import AddressAutocomplete, { type PlaceResult } from "../components/AddressAutocomplete";

export default function LookupPage() {
  const [homeName, setHomeName] = useState("");
  const [address, setAddress] = useState("");
  const [placeResult, setPlaceResult] = useState<PlaceResult | null>(null);

  const [lookupLoading, setLookupLoading] = useState(false);
  const [utilities, setUtilities] = useState<UtilityMatch[]>([]);
  const [selectedUtilityId, setSelectedUtilityId] = useState<number | null>(null);

  const [tariffs, setTariffs] = useState<Tariff[]>([]);
  const [tariffsLoading, setTariffsLoading] = useState(false);

  const [selectedTariffId, setSelectedTariffId] = useState<number | null>(null);
  const [tariffDetail, setTariffDetail] = useState<TariffDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);

  const [error, setError] = useState("");

  const handlePlaceSelected = useCallback(async (result: PlaceResult) => {
    setPlaceResult(result);
    setAddress(result.formattedAddress);
    setSelectedUtilityId(null);
    setTariffs([]);
    setSelectedTariffId(null);
    setTariffDetail(null);
    setError("");
    setLookupLoading(true);
    setUtilities([]);

    try {
      const data = await api.lookup(result.formattedAddress);
      const filtered = data.utilities.filter((u) => u.residential_tariff_count > 0);
      setUtilities(filtered);
      if (filtered.length === 1) {
        setSelectedUtilityId(filtered[0].id);
      }
    } catch (e: any) {
      setError(e.message || "Could not find utilities for this address");
    } finally {
      setLookupLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!selectedUtilityId) {
      setTariffs([]);
      setSelectedTariffId(null);
      setTariffDetail(null);
      return;
    }
    setTariffsLoading(true);
    setSelectedTariffId(null);
    setTariffDetail(null);
    api
      .listTariffs(selectedUtilityId, "residential")
      .then(setTariffs)
      .catch(() => setTariffs([]))
      .finally(() => setTariffsLoading(false));
  }, [selectedUtilityId]);

  useEffect(() => {
    if (!selectedTariffId) {
      setTariffDetail(null);
      return;
    }
    setDetailLoading(true);
    api
      .getTariff(selectedTariffId)
      .then(setTariffDetail)
      .catch(() => setTariffDetail(null))
      .finally(() => setDetailLoading(false));
  }, [selectedTariffId]);

  const selectedUtility = utilities.find((u) => u.id === selectedUtilityId);

  const formatRateType = (rt: string) =>
    rt
      .replace(/_/g, " ")
      .replace(/\btou\b/gi, "TOU")
      .replace(/\b\w/g, (c) => c.toUpperCase());

  return (
    <div className="mysa-container">
      <h1 className="mysa-heading">Set up your home</h1>

      {/* Home Name */}
      <div className="mysa-field-wrap">
        <div className="mysa-field">
          <span className="mysa-field-label">Home Name</span>
          <input
            type="text"
            className="mysa-field-input"
            value={homeName}
            onChange={(e) => setHomeName(e.target.value)}
            placeholder="My Home"
          />
        </div>
        <p className="mysa-hint">e.g. 6 Clarke Ave, Cabin</p>
      </div>

      {/* Address */}
      <div className="mysa-field-wrap">
        <div className="mysa-field mysa-field-address">
          <div className="mysa-search-icon">
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#5f6775" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
              <circle cx="11" cy="11" r="8" />
              <line x1="21" y1="21" x2="16.65" y2="16.65" />
            </svg>
          </div>
          <div className="mysa-field-inner">
            <span className="mysa-field-label">Address</span>
            <AddressAutocomplete
              value={address}
              onChange={setAddress}
              onPlaceSelected={handlePlaceSelected}
              placeholder="Start typing your address..."
              className="mysa-field-input"
            />
          </div>
        </div>
        {lookupLoading && <p className="mysa-hint">Finding utilities at this address...</p>}
      </div>

      {error && <p className="mysa-error">{error}</p>}

      {/* Utility selection */}
      {utilities.length > 0 && (
        <div className="mysa-field-wrap mysa-appear">
          <div className="mysa-field">
            <span className="mysa-field-label">Your Utility</span>
            {utilities.length === 1 ? (
              <div className="mysa-field-value">{utilities[0].name}</div>
            ) : (
              <select
                className="mysa-field-select"
                value={selectedUtilityId ?? ""}
                onChange={(e) =>
                  setSelectedUtilityId(e.target.value ? Number(e.target.value) : null)
                }
              >
                <option value="">Select your utility</option>
                {utilities.map((u) => (
                  <option key={u.id} value={u.id}>
                    {u.name}
                  </option>
                ))}
              </select>
            )}
          </div>
          {utilities.length === 1 && <p className="mysa-hint">Auto-detected for your area</p>}
          {utilities.length > 1 && !selectedUtilityId && (
            <p className="mysa-hint">
              {utilities.length} utilities serve {placeResult?.stateProvince || "your area"}
            </p>
          )}
        </div>
      )}

      {/* Rate plan selection */}
      {selectedUtilityId && tariffs.length > 0 && (
        <div className="mysa-field-wrap mysa-appear">
          <div className="mysa-field">
            <span className="mysa-field-label">Rate Plan</span>
            <select
              className="mysa-field-select"
              value={selectedTariffId ?? ""}
              onChange={(e) =>
                setSelectedTariffId(e.target.value ? Number(e.target.value) : null)
              }
            >
              <option value="">Select your rate plan</option>
              {tariffs.map((t) => (
                <option key={t.id} value={t.id}>
                  {t.name} ({formatRateType(t.rate_type)})
                  {t.is_default ? " — Default" : ""}
                </option>
              ))}
            </select>
          </div>
          <p className="mysa-hint">
            {tariffs.length} residential rate plan{tariffs.length !== 1 ? "s" : ""} available
          </p>
        </div>
      )}

      {tariffsLoading && (
        <div className="mysa-field-wrap">
          <p className="mysa-hint">Loading available rate plans...</p>
        </div>
      )}

      {selectedUtilityId && !tariffsLoading && tariffs.length === 0 && (
        <div className="mysa-field-wrap">
          <p className="mysa-hint">No residential rate plans found for this utility yet.</p>
        </div>
      )}

      {/* Inline tariff detail */}
      {detailLoading && (
        <div className="mysa-field-wrap">
          <p className="mysa-hint">Loading rate details...</p>
        </div>
      )}

      {tariffDetail && <TariffInline detail={tariffDetail} formatRateType={formatRateType} />}

      {/* Done button */}
      {tariffDetail && (
        <div className="mysa-done-wrap mysa-appear">
          <button className="mysa-done-btn" onClick={() => {}}>
            Done
          </button>
        </div>
      )}
    </div>
  );
}

/* ── Inline tariff detail component ── */

function TariffInline({
  detail,
  formatRateType,
}: {
  detail: TariffDetail;
  formatRateType: (rt: string) => string;
}) {
  const fixedComponents = detail.rate_components.filter(
    (c) => c.component_type === "fixed" || c.component_type === "minimum"
  );
  const energyComponents = detail.rate_components.filter(
    (c) => c.component_type === "energy"
  );
  const demandComponents = detail.rate_components.filter(
    (c) => c.component_type === "demand"
  );

  const hasSeason = (comps: RateComponent[]) =>
    comps.some((c) => c.season && c.season.trim() !== "");

  const groupBySeason = (comps: RateComponent[]) => {
    const groups: Record<string, RateComponent[]> = {};
    for (const c of comps) {
      const key = c.season?.trim() || "All Seasons";
      if (!groups[key]) groups[key] = [];
      groups[key].push(c);
    }
    return groups;
  };

  return (
    <div className="mysa-detail mysa-appear">
      {/* Header */}
      <div className="mysa-detail-header">
        <div className="mysa-detail-name">{detail.name}</div>
        <div className="mysa-detail-badges">
          <span className={`badge badge-${detail.rate_type.includes("tou") ? "tou" : detail.rate_type}`}>
            {formatRateType(detail.rate_type)}
          </span>
          <span className={`badge badge-${detail.data_freshness}`}>
            {detail.data_freshness === "current" ? "Current" : detail.data_freshness === "aging" ? "Aging" : "Stale"}
          </span>
        </div>
        {detail.description && (
          <p className="mysa-detail-desc">{detail.description}</p>
        )}
        {detail.effective_date && (
          <p className="mysa-detail-meta">Effective {detail.effective_date}</p>
        )}
      </div>

      {/* Fixed Charges */}
      {fixedComponents.length > 0 && (
        <div className="mysa-detail-section">
          <h3 className="mysa-detail-section-title">Fixed Charges</h3>
          <div className="mysa-rate-list">
            {fixedComponents.map((c) => (
              <div key={c.id} className="mysa-rate-row">
                <span className="mysa-rate-label">
                  {c.tier_label || c.component_type}
                </span>
                <span className="mysa-rate-value">
                  ${c.rate_value.toFixed(2)}
                  <span className="mysa-rate-unit">/{c.unit.replace("$/", "")}</span>
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Energy Rates */}
      {energyComponents.length > 0 && (
        <div className="mysa-detail-section">
          <h3 className="mysa-detail-section-title">Energy Rates</h3>
          {hasSeason(energyComponents) ? (
            Object.entries(groupBySeason(energyComponents)).map(([season, comps]) => (
              <div key={season} className="mysa-season-group">
                <div className="mysa-season-label">{season}</div>
                <div className="mysa-rate-list">
                  {comps.map((c) => (
                    <div key={c.id} className="mysa-rate-row">
                      <span className="mysa-rate-label">
                        {c.period_label || (c.tier_min_kwh !== null || c.tier_max_kwh !== null
                          ? `${c.tier_min_kwh ?? 0} – ${c.tier_max_kwh ?? "+"} kWh`
                          : "All usage")}
                      </span>
                      <span className="mysa-rate-value">
                        ${c.rate_value.toFixed(5)}
                        <span className="mysa-rate-unit">/kWh</span>
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            ))
          ) : (
            <div className="mysa-rate-list">
              {energyComponents.map((c) => (
                <div key={c.id} className="mysa-rate-row">
                  <span className="mysa-rate-label">
                    {c.period_label ||
                      (c.tier_min_kwh !== null || c.tier_max_kwh !== null
                        ? `${c.tier_min_kwh ?? 0} – ${c.tier_max_kwh ?? "+"} kWh`
                        : "All usage")}
                  </span>
                  <span className="mysa-rate-value">
                    ${c.rate_value.toFixed(5)}
                    <span className="mysa-rate-unit">/kWh</span>
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Demand Charges */}
      {demandComponents.length > 0 && (
        <div className="mysa-detail-section">
          <h3 className="mysa-detail-section-title">Demand Charges</h3>
          <div className="mysa-rate-list">
            {demandComponents.map((c) => (
              <div key={c.id} className="mysa-rate-row">
                <span className="mysa-rate-label">
                  {c.period_label || "All"}
                </span>
                <span className="mysa-rate-value">
                  ${c.rate_value.toFixed(2)}
                  <span className="mysa-rate-unit">/{c.unit.replace("$/", "")}</span>
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {detail.source_url && (
        <div className="mysa-detail-source">
          <a href={detail.source_url} target="_blank" rel="noreferrer">
            View source document
          </a>
        </div>
      )}
    </div>
  );
}
