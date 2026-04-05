# Centralized Electricity Rate Regulators

Instead of scraping each utility's website individually, many jurisdictions have
a single regulator or dominant utility that sets rates for the entire province/state.
Scraping the centralized source is faster, more reliable, and avoids bot-blocking.

---

## Canada — Province-by-Province

### Tier 1: Centralized Rate Source Available (scrape regulator directly)

| Province | Source | URL | Notes |
|----------|--------|-----|-------|
| **Ontario** | Ontario Energy Board (OEB) | [oeb.ca/...historical-electricity-rates](https://www.oeb.ca/consumer-information-and-protection/electricity-rates/historical-electricity-rates) | OEB sets RPP rates (TOU, Tiered, ULO) for ALL ~60 LDCs. Rates identical across utilities. Updated Nov 1 each year. **Implemented in `scrape_oeb_rates.py`**. |
| **Quebec** | Hydro-Québec (single utility) | [hydroquebec.com/residential/rates](https://www.hydroquebec.com/residential/customer-space/rates/) | Hydro-Québec is the sole provider. Régie de l'énergie approves rates. Rate D (residential), Rate G (commercial). |
| **Manitoba** | Manitoba Hydro (single utility) | [hydro.mb.ca/accounts-and-billing/rates](https://www.hydro.mb.ca/accounts_and_billing/rates/) | Crown corporation, sole provider. Simple tiered structure. |
| **Saskatchewan** | SaskPower (dominant utility) | [saskpower.com/rates-and-billing](https://www.saskpower.com/Rates-and-Billing) | Crown corporation. Covers entire province except Swift Current and most of Saskatoon (municipal). |
| **New Brunswick** | NB Power (dominant utility) | [nbpower.com/en/products-services/residential](https://www.nbpower.com/en/products-services/residential) | Crown corporation. Covers all except Saint John, Edmundston, Perth-Andover (municipal). |
| **Prince Edward Island** | Maritime Electric (single utility) | [maritimeelectric.com/rates](https://www.maritimeelectric.com/about-us/regulatory/rates-and-general-rules-and-regulations/) | Regulated by IRAC. Only utility except Summerside Electric (exempt). |

### Tier 2: Dominant Utility + Regulator (scrape utility, verify with regulator)

| Province | Main Utility | Regulator | Notes |
|----------|-------------|-----------|-------|
| **British Columbia** | BC Hydro (~95% of province) | BCUC | BC Hydro and FortisBC have DIFFERENT rates (unlike Ontario). Must scrape each. BCUC approves but doesn't set uniform rates. |
| **Alberta** | ATCO/ENMAX/EPCOR/Direct Energy | AUC | Deregulated market. AUC sets Rate of Last Resort (ROLR) — currently ~12¢/kWh fixed 2025-2026. Individual retailers may offer competitive plans. For regulated rate, AUC is the source. |
| **Nova Scotia** | Nova Scotia Power (~95%) | NSUARB | NS Power is the dominant utility. Rates set via cost-of-service model, approved by NSUARB. A few small municipal utilities exist. |
| **Newfoundland & Labrador** | Newfoundland Power + NL Hydro | PUB NL | Two main utilities with different service areas. PUB regulates both. Must scrape each separately. |

### Tier 3: Territories (small, manual curation practical)

| Territory | Utility | Notes |
|-----------|---------|-------|
| **Yukon** | ATCO Electric Yukon / Yukon Energy | Small number of rate classes. Manual curation recommended. |
| **Northwest Territories** | NTPC (NWT Power Corporation) | Very few customers, simple rate structure. |
| **Nunavut** | Qulliq Energy | Remote communities, unique rate structures per community. |

---

## United States — Key Findings

Unlike Canada, the US does NOT have centralized state-wide rate setting. Key differences:

1. **State PUCs regulate IOUs individually** — Each state Public Utility Commission (PUC)
   reviews and approves rate cases for each investor-owned utility separately. There is no
   single page with "all rates for all utilities in Texas."

2. **Rate diversity** — Even within a state, rates vary dramatically between utilities.
   A utility in rural Texas charges different rates than one in Houston.

3. **Three utility types with different regulation:**
   - **Investor-Owned Utilities (IOUs):** Regulated by state PUC. Rates public but filed individually.
   - **Municipal Utilities:** Self-regulated by city council. Rates on city/utility website.
   - **Cooperatives (Co-ops):** Governed by member board. Rates on co-op website.

4. **Some helpful aggregation sources:**
   - **EIA-861** data: Annual utility data including customer counts, revenue, sales — but NOT
     individual tariff rate structures.
   - **OpenEI URDB:** Crowd-sourced rate database (our existing seed data), but often outdated.
   - **State PUC filing systems:** Some states have rate case databases (e.g., CA CPUC,
     TX PUC, NY PSC) but they contain legal filings, not structured rate data.

### Recommended US Approach

For the US, continue with the **per-utility scraping pipeline** (Brave Search → Crawl → LLM Extract).
The browser interaction agent helps with the ~20% of sites that block bots.

Priority states for quality assurance (based on population and market size):
1. California, Texas, New York, Florida, Illinois
2. Pennsylvania, Ohio, Georgia, Michigan, North Carolina
3. New Jersey, Virginia, Washington, Massachusetts, Arizona

---

## Implementation Strategy

### Phase 1 (Done): Ontario via OEB
- `backend/scripts/scrape_oeb_rates.py` fetches OEB page, parses TOU/Tiered/ULO rates
- Applies identical rates to all ~15 Ontario utilities in one run
- Runs monthly (or on Nov 1 for the annual rate change)

### Phase 2 (Next): Other Canadian Single-Utility Provinces
- Quebec (Hydro-Québec), Manitoba (Manitoba Hydro), Saskatchewan (SaskPower),
  New Brunswick (NB Power), PEI (Maritime Electric)
- Each needs a dedicated scraper similar to `scrape_oeb_rates.py`
- Much simpler since there's only one utility per province

### Phase 3: Canadian Multi-Utility Provinces
- BC (BC Hydro + FortisBC), Alberta (4 distributors), Nova Scotia (NS Power),
  Newfoundland (NF Power + NL Hydro)
- Use per-utility pipeline with browser agent for blocked sites

### Phase 4: US Utilities
- Continue per-utility pipeline
- Browser agent handles interactive/blocked sites
- Prioritize top 15 states by market size
