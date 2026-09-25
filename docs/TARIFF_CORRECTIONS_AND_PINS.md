# Manual corrections, document pins and automated re-verification

How a human-approved rate fix gets into UTF, why the monthly refresh cannot
undo it, and how a real utility rate change is still picked up **without a
human in the loop**. Code: `app/api/routes/corrections.py`,
`app/services/{corrections,pins,pin_verification}.py`,
`app/tasks/verification.py`.

## 1. Who does what

| Step | Owner |
|---|---|
| Customer reports a wrong rate; CS verifies; second check / approval | **Mysa** admin portal (not UTF) |
| Apply the approved correction (soft-supersede, pin, audit) | UTF `POST /api/tariff-corrections` |
| Keep the fix safe from scraper refreshes | UTF pins (§3) |
| Notice the utility published new rates and adopt them | UTF automated verification (§4) |

## 2. `POST /api/tariff-corrections`

**Auth:** header `X-Corrections-Key: <TARIFF_CORRECTIONS_API_KEY>` (or
`Authorization: Bearer`). This key is separate from `ADMIN_API_KEY` (which
the Flux read proxy holds) and from Google sessions; neither is accepted
here. With the env var unset the endpoint returns 503. Keys are compared in
constant time. Give the key only to the Mysa cloud service that pushes
approved corrections.

```json
{
  "idempotency_key": "mysa-cs-6f1c2e0a-...",
  "ticket_id": "CS-12345",
  "approved_by": "approver@getmysa.com",
  "approved_at": "2026-10-02T14:00:00Z",
  "requested_by": "agent@getmysa.com",
  "target": {"utility_id": 123, "mode": "replace", "expected_live_tariff_id": 456},
  "tariff": {"name": "Residential Service", "code": "R-1", "customer_class": "residential",
             "rate_type": "seasonal_tou", "effective_date": "2025-11-01"},
  "components": [
    {"component_type": "energy", "unit": "$/kWh", "rate_value": "0.203000",
     "period_label": "On-Peak", "period_start_time": "07:00", "period_end_time": "11:00",
     "day_type": "weekday", "season": "Winter",
     "season_start_month": 11, "season_start_day": 1, "season_end_month": 4, "season_end_day": 30},
    {"component_type": "fixed", "unit": "$/month", "rate_value": "33.41"}
  ],
  "evidence": {"source_url": "https://utility.example/rates.pdf",
               "source_document_sha256": "<64 hex, optional>",
               "page_ref": "p. 12", "quote": "On-peak 20.3¢/kWh"},
  "pin": {"scope": "document", "cause": "extraction_error"}
}
```

- `mode`: `replace` supersedes `expected_live_tariff_id` with a new row
  (`supersede_reason='manual'`). `create` adds a new product with no
  predecessor. `retire` soft-retires `expected_live_tariff_id`
  (`manual_retire`, no successor; omit `tariff` / `components`).
- `rate_value` should be a decimal string. Units must be dollars (`$/kWh`,
  `$/day`, `$/month`, …); cents units are rejected rather than converted.
- `pin.cause`: `extraction_error` (the document was right, our extraction
  was wrong) or `source_error` (the document itself was wrong or stale).
  Only affects the effective-date rule in §4.

**Responses**

| Status | Body |
|---|---|
| 201 | `{new_tariff_id, superseded_tariff_id, change_event_id, pin_id, computable, computable_reasons, computable_warnings, warnings, replayed: false}` |
| 200 | Same body with `replayed: true`. The same `idempotency_key` was already applied with an identical payload. |
| 409 `not_live` | The target was superseded after Mysa approved (e.g. a refresh or another correction). Includes `current_live_tariff_id` and `last_change_event_id`; re-approve against the current row. |
| 409 `live_tariff_exists` | `create` would duplicate a live product; use `replace`. |
| 409 `idempotency_key_reused` | Same key, different payload. |
| 409 `refresh_in_progress` | A refresh holds the utility's lock; retry later. |
| 404 / 422 / 401 / 503 | Unknown utility or tariff / invalid payload (negative energy, cents units, mode rules) / bad key / endpoint disabled. |

**Write semantics (one transaction):** new live row (`approved=true`,
`confidence_factors.origin='manual'` with ticket, approver and evidence) →
predecessor soft-superseded, its components untouched → document pin on
`evidence.source_url` (a monitoring source is created for it if missing) →
one `tariff_change_events` row (`actor_type='manual_api'`, `actor_id` =
approver, ticket, idempotency key, evidence hash, full request). To revert,
send a new correction; history is never edited in place.

`DELETE /api/tariffs/{id}` (admin key or session) no longer hard-deletes:
it soft-retires with `manual_retire` and logs the caller.

## 3. Pins: what refreshes do to a pinned row

A manual row is **protected** (`is_protected`) and **pinned** (`tariff_pins`):

- `store_tariffs`: an extract that differs from the pinned row is **held**.
  The live row is untouched and the proposal is logged as a `hold` change
  event. If the extract came from the *same* document, that is extraction-bug
  evidence and nothing else happens. If it came from a *different* document
  (e.g. a newer rate book), a verification is opened (`trigger=new_document`).
- OEB feed, reconciliation, dup cleanup, vintage collapse: never overwrite or
  retire the row (they log `hold` events).
- `_touch_tariff_verified` (unchanged-fingerprint shortcut) does not refresh
  a pinned row's `last_verified_at`. Only a verification of its own document
  does.
- Weekly monitoring of the pinned URL: the first check records the baseline
  hash. A later **CHANGED** opens a verification (`trigger=source_changed`).
  Monitoring now keeps the previous normalized text, so `diff_summary` shows
  real added/removed terms.
- Periodic safety net: pins with no verification in 365 days get a
  `periodic` verification. This covers a static wrong PDF that never changes.

## 4. Automated verification (no human queue)

`process_pin_verifications` (Celery; not scheduled, see **Enabling** below)
decides each `proposed` verification
with gates, cheapest first. The first failure **holds**: the pinned row
stays live, the pin moves to state `held` with `hold_reason`, and the next
signal retries.

| # | Gate | Hold reason |
|---|---|---|
| 0 | Claim verifier **and** arbiter configured | `verifier_unavailable` |
| 1 | ≤ `PIN_VERIFY_DAILY_MAX` (default 20) decisions per day | `budget_exhausted` |
| 2 | Fetch the new document | `fetch_failed` |
| 3 | Prompt-injection screen | `injection_suspected` |
| 4 | Proposal from the scraper or a fresh single-document extraction | `no_proposal` |
| 5 | Same class, computable, effective date newer (≥ for `source_error` pins), rates actually differ | `customer_class_changed`, `not_computable`, `effective_date_not_newer`, `no_rate_change` |
| 6 | Every rate / clock / season / effective-date claim verified against the text (≥0.90; ≥0.95 for derived all-in values) | `claims_contradicted`, `claims_unsupported` |
| 7 | Arbiter (Opus-tier typed verdict) accepts | `arbiter_rejected` |
| 3/6/7 | A verifier or arbiter call raised (Mercury / Anthropic down, bad response) | `verifier_error` |

**Accept:** insert the verified row (`origin='agent_verified'`, approved)
and soft-supersede the pinned row (`agent_verify_accept`). The pin moves to
the new row with the new document hash, and a change event carries every
gate verdict.

**Adapters** (`app/services/pin_adapters.py`). The defaults are
`PIN_VERIFIER=none` / `PIN_ARBITER=none`: every proposal holds at gate 0
without fetching or calling an LLM. Gate 0 needs **both** real adapters, so
Jev alone never accepts a pinned row.

| Env | Values | Adapter |
|---|---|---|
| `PIN_VERIFIER` | `none` (default), `jev` | `JevVerifier`: Mercury `jev_screen` (gate 3) + `jev_verify` (gate 6) |
| `PIN_ARBITER` | `none` (default), `opus` | `OpusArbiter`: one Anthropic call, forced `record_verdict` tool (typed accept/reject) |

Required when enabled (values live in the VM env / `.env`, never the repo):

- `jev`: `MERCURY_URL` (Mercury's MCP HTTP endpoint) and `MERCURY_API_TOKEN`
  (bearer token of a Mercury actor scoped for `jev.verify` / `jev.screen`).
  Optional: `MERCURY_TIMEOUT_SEC` (120), `JEV_CHUNK_CHARS` (40000),
  `JEV_MAX_CHUNKS` (5).
- `opus`: `ANTHROPIC_API_KEY`. Model: `AUDITOR_MODEL`, else `OPUS_MODEL`
  (default `claude-opus-5`), the same knobs as `opus_audit.py`.

A value that is unknown, or whose credentials are missing, logs a warning
and falls back to the Null adapter (holds).

How the adapters decide:

- **Screen.** The document is split into `JEV_CHUNK_CHARS` chunks and every
  chunk must come back `pass`; `review` / `block` / `skip` hold as
  `injection_suspected`. A document longer than `JEV_MAX_CHUNKS` chunks is
  not screened and holds, so no unscreened text reaches the extractor,
  Jev or Opus. The screen result is stored in `gate_results.screen`.
- **Claims.** Atomic sentences, one per rate, clock window, season and
  effective date, verified against the chunked document (`auto_accept`
  0.90). Verdicts are `verified` / `contradicted` / `unsupported` plus
  confidence. A Jev verdict flagged `review` never counts as verified, and a
  missing or malformed row is `unsupported`.
- **All-in values.** Claims use the book's verbatim numbers. When a proposal
  carries the `included_in_energy` riders of an all-in ENERGY row, the claims
  are the base (all-in minus riders) and each rider, so the all-in is their
  sum by construction. Without riders it stays one `derived_rate` claim at
  the ≥0.95 floor.
- **Arbiter.** Sees the current row, the proposal and the (same capped)
  document, treats the document as untrusted data, and rejects when in
  doubt. A reply without a typed verdict is a reject.

Cost: Opus usage is priced as `opus` under phase `pin_arbiter`; Jev tokens
are recorded as `jev` (priced 0 unless `LLM_PRICING_JSON` sets `jev`). Each
task run appends a `pin_verifications` entry to the LLM cost ledger and
returns `llm_cost_usd` and `jev_gateway_usd` (Mercury's `cost.usd`, which
reported $0.00 in the spike: treat that as unverified billing).

**Enabling.** Set the env, restart `celery-worker`, and run
`process_pin_verifications` by hand on a few proposals first. The
`daily-pin-verifications` beat entry in `celery_app.py` stays **commented
out**: ops schedules it deliberately after checking Mercury billing and Opus
spend per decision (`PIN_VERIFY_DAILY_MAX` caps decisions per day). The
§7.3 offline spike (9 gold tariffs, 100% perturbation catch, 0% false
contradictions on verbatim book numbers) is the basis for the Jev gate.

## 5. Observability

- `tariff_change_events`: every insert / supersede / retire / hold with actor.
- `tariff_pins`: `state`, `hold_reason`, `consecutive_holds`, `last_checked_at`.
- `tariff_verifications`: `status`, `trigger`, `hold_reason`, `gate_results`.
