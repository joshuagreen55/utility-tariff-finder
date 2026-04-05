# Tariff URL Remediation Skill

You are a data-quality agent for the **Utility Tariff Finder** database. Your job is to fix broken monitoring source URLs — each source tracks a US or Canadian electric utility's tariff/rate page.

## When to Use This Skill

Use when asked to remediate tariff URLs, fix monitoring errors, or run the weekly tariff URL maintenance task.

## Context

The system monitors ~8,000 URLs that point to electric utility tariff pages. A deterministic remediation script has already tried simple fixes (HTTP/HTTPS swap, `www` toggle, common paths). You handle the **remaining errors** that need intelligent web research.

Each monitoring source has:
- `id` — unique source ID
- `utility_name` — human name of the utility (e.g. "Pacific Gas & Electric")
- `url` — the broken URL we need to replace
- `utility_id` — FK to the utility record
- `status` — `"error"` for sources that need fixing

The utility also has `state_province`, `country` (US or CA), and `website_url` stored in the database.

## Environment Setup

Helper scripts live alongside this skill. Before using them, source the environment:

```bash
source /home/josh/.config/utility-tariff.env
```

This sets `UTILITY_TARIFF_API_BASE` (http://127.0.0.1:8000) and `UTILITY_TARIFF_ADMIN_KEY`.

## Step-by-Step Workflow

### 1. Fetch Errored Sources

Run the helper script to get a batch of broken sources:

```bash
/home/josh/.openclaw/skills/tariff-remediation/fetch-errors.sh [LIMIT]
```

Default limit is 25. Output is JSON — each object has `id`, `utility_name`, `url`, `utility_id`, `status`.

**Note:** This script automatically filters out sources you've already attempted (by reading the audit log). You will only see fresh, unattempted sources.

### 2. Research Replacement URLs

> **HARD RULE: You MUST call `web_search` for EVERY source. No exceptions.**
>
> - Do NOT guess URLs by appending "/rates", "/tariffs", or any path.
> - Do NOT construct URLs by combining a domain with a common path.
> - Do NOT reuse a URL from a previous source (even if the utility name is similar).
> - The ONLY acceptable way to get a replacement URL is from `web_search` results.
> - If you skip `web_search` for even one source, the entire session is a failure.

For each errored source:

1. **Identify the utility** from `utility_name`. Note the state/province from context.
2. **Call `web_search`** with a specific query. Good queries:
   - `"<utility_name>" electric tariff rates`
   - `"<utility_name>" residential electric rates <state>`
   - `"<utility_name>" tariff schedule`
   - If the first search returns no useful results, try a **different** query.
3. **Pick a URL from the search results** — prefer URLs on the utility's own domain or a state regulator site. Avoid third-party aggregators, news articles, or unrelated PDFs.
4. **Verify the page loads** and contains tariff-relevant content (rate tables, tariff schedules, rate case documents, PDF tariff sheets).
5. If `web_search` returns no relevant results after 2 attempts, **skip** the source. Do NOT fabricate a URL.
6. **Pace yourself**: each source should take 30-60 seconds of real research. If you're going faster, you're not doing real searches.

### 3. Patch and Verify

When you find a working replacement URL, use the helper script:

```bash
/home/josh/.openclaw/skills/tariff-remediation/patch-source.sh <SOURCE_ID> "<NEW_URL>"
```

This PATCHes the source URL in the database, then re-checks it. It prints:
- `PATCHED` if the URL was saved successfully
- The re-check result (`unchanged`/`changed` = success, `error` = the new URL also fails)

### 4. Skip When Uncertain

If you cannot find a reliable replacement:
- **Skip the source** — do not guess.
- Log it as skipped so it can be retried later.
- Common skip reasons: utility no longer exists, utility merged with another, tariff page is behind a login wall, no web presence at all.

## Guardrails

- **ALWAYS use `web_search`** — never guess URLs by appending common paths. Every fix must come from a real search result.
- **Only change monitoring source URLs** (data). Never modify Python source files, configs, or database schema.
- **Prefer the utility's own domain** or official regulator sites (e.g. state PUC). Avoid random third-party PDFs.
- **No login-walled pages** — the monitoring system cannot authenticate.
- **HTTP or HTTPS only** — URLs must start with `http://` or `https://`.
- **One source at a time** — search, patch, verify, then move to the next.
- **Audit everything** — every action (fix or skip) gets logged with the search query used.

## Logging

> **CRITICAL: You MUST log EVERY source — both fixed AND skipped — IMMEDIATELY after processing it.**
> Do NOT batch the logging at the end. Log each source right after you process it.
> If you skip logging a source, it will be re-fetched and re-attempted, wasting time.

After EACH source (fixed or skipped), run this command:

```bash
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) | source_id=<ID> | action=<fixed|skipped> | old_url=<OLD> | new_url=<NEW_OR_NONE> | reason=<BRIEF>" \
  >> /home/josh/utility-tariff-finder/logs/agent-audit.log
```

Do this IMMEDIATELY after each source, not in a batch at the end.

## Dead Utility Cleanup

Use when asked to clean up dead utilities or after the remediation pass.

### 1. Fetch Dead Utilities

Run the helper script to list utilities where ALL monitoring sources are in error status:

```bash
/home/josh/.openclaw/skills/tariff-remediation/fetch-dead-utilities.sh [LIMIT]
```

Returns JSON with `utility_id`, `name`, `state_province`, `website_url`, `source_count`, `error_count`.

### 2. Investigate Each Utility

For each dead utility, use `web_search` to determine if it still exists:

- Search `"<utility_name>" electric utility <state>`
- Check if it was **merged** with another utility (common — e.g., "Westar Energy" became "Evergy")
- Check if it was **absorbed** by a larger utility
- Check if it still exists but simply has no web presence

### 3. Deactivate or Skip

- If the utility **no longer exists** (merged, dissolved, absorbed), deactivate it:
  ```bash
  /home/josh/.openclaw/skills/tariff-remediation/deactivate-utility.sh <UTILITY_ID> "<REASON>"
  ```
  Good reasons: "Merged with [name] in [year]", "Dissolved", "Absorbed by [name]"

- If the utility **still exists** but has no findable tariff page, **skip** it — leave it active so it can be retried later.

### 4. Log Everything

```bash
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) | utility_id=<ID> | action=<deactivated|skipped> | name=<NAME> | reason=<BRIEF>" \
  >> /home/josh/utility-tariff-finder/logs/agent-audit.log
```

## Important: Use the Helper Scripts

**Do NOT call the API directly.** Always use the helper scripts in this skill directory. They handle authentication, pagination, and deduplication automatically. Calling the API directly will bypass deduplication and cause you to re-process already-attempted sources.

## Example Session

```
> Fetching 20 errored sources...

Source 4521: "Acme Electric Cooperative" — old URL: https://acme-electric.com/rates
  Searching: "Acme Electric Cooperative" electric rates Oklahoma
  Found: https://www.acmeelectric.coop/residential-rates
  Patching source 4521... PATCHED ✓
  Re-check: unchanged (page loads, content hashed)

Source 4522: "Defunct Power Co" — old URL: https://defunctpower.com/tariffs
  Searching: "Defunct Power Co" electric tariff
  No results — utility appears to have been absorbed by StatePower Inc.
  Skipping source 4522.

Fixed: 15 / 20 | Skipped: 5 / 20
```

### Dead Utility Cleanup Example

```
> Fetching dead utilities (all sources errored)...

Utility 892: "Old Valley Electric" (WV) — 3 sources, all errors
  Searching: "Old Valley Electric" electric utility West Virginia
  Found: merged with Appalachian Power in 2019
  Deactivating utility 892... DEACTIVATED ✓

Utility 1456: "Smalltown Municipal Power" (KS) — 1 source, all errors
  Searching: "Smalltown Municipal Power" electric utility Kansas
  Found: still exists, serves 2,000 customers, but no website
  Skipping — utility is real but has no web presence.

Deactivated: 8 / 15 | Skipped: 7 / 15
```
