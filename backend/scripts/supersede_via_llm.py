"""LLM-driven 1:N absorption of stranded OpenEI tariffs ("Track B").

For each utility that has BOTH fresh tariffs (`last_verified_at IS NOT
NULL`) and stranded OpenEI seeds (`openei_id IS NOT NULL AND
last_verified_at IS NULL`), ask Claude Haiku to decide which fresh
tariff (if any) is the current published equivalent of each stranded
seed. When the LLM returns a confident match, mark the stranded row
with `superseded_by_tariff_id` pointing at the fresh row.

This is the companion to the deterministic matcher in
`tariff_pipeline.tariffs_likely_same`. The deterministic pass handles
1:1 cases (same name, shared rate code). This LLM pass handles the
1:N case that URDB structurally produces: one logical tariff exploded
across zones / voltages / regions in OpenEI but consolidated into a
single marketing row in the utility's published rate page.

Designed for offline / one-shot runs, not as part of the live
extraction pipeline. Cost is dominated by Anthropic Haiku tokens; a
full DB sweep is ~$3-5 at current pricing.

Usage:
  python -m scripts.supersede_via_llm                       # dry-run all
  python -m scripts.supersede_via_llm --utility 246         # dry-run one
  python -m scripts.supersede_via_llm --apply               # live, all
  python -m scripts.supersede_via_llm --apply --confidence medium
"""
import argparse
import os
import sys
import time
from collections import defaultdict

import anthropic
from sqlalchemy import create_engine, text, update as sa_update
from sqlalchemy.orm import Session

from app.models.tariff import Tariff
from app.services import anthropic_compat

HAIKU_MODEL = os.environ.get("HAIKU_MODEL", "claude-haiku-4-5-20251001")

from scripts import llm_cost  # noqa: E402

CONF_RANK = {"high": 3, "medium": 2, "low": 1}


PAIRING_TOOL = {
    "name": "report_pairings",
    "description": (
        "Report which OpenEI URDB seed tariffs are subsumed by which fresh "
        "marketing tariff for a single utility. For each OpenEI seed, return "
        "either the fresh_tariff_id that publishes the same product today, "
        "or null if no fresh row covers it."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "pairings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "stranded_id": {"type": "integer"},
                        "fresh_id":    {"type": ["integer", "null"]},
                        "confidence":  {"type": "string", "enum": ["high", "medium", "low"]},
                        "reason":      {"type": "string"}
                    },
                    "required": ["stranded_id", "fresh_id", "confidence", "reason"]
                }
            }
        },
        "required": ["pairings"]
    }
}


SYSTEM_PROMPT = """You are reconciling a US/Canadian utility's tariff catalog.

Two lists of tariffs exist for ONE utility:

  FRESH   = recently extracted from the utility's official rate pages or PDFs
            (marketing-style names, broad categories, typically <20 items).

  OPENEI  = older URDB seed rows (URDB explodes one logical tariff into many
            per-zone, per-voltage, per-region permutations; technical names
            like 'SC-1 Residential Rate III [NYC Zone J]').

For each OPENEI row, decide which FRESH row (if any) is its current
published equivalent. Strict rules:

  1. **Same customer class only.** Residential <-> Residential. Never
     pair a Residential OPENEI to a Commercial FRESH.
  2. **Same rate-code family.** SC-1 OPENEI pairs to SC 1 FRESH, not
     SC 2. 'Rate D' pairs to 'Rate D', not 'Rate DP'. Pay attention to
     letters and numbers.
  3. **Optional vs Standard.** 'Voluntary TOD' or 'Optional Demand' is
     a different product from the standard rate of the same class —
     they should match different FRESH rows. Only pair if the FRESH
     row explicitly covers that variant.
  4. **Zone/region permutations roll up.** Multiple OPENEI rows like
     '[NYC]', '[Westchester]', 'Zone H', 'Zone J' for the same logical
     tariff all pair to the SAME single FRESH row when one exists.
  5. **Tier permutations roll up.** Same with 'Tier 1', 'Tier 2',
     'Summer', 'Winter' — these are all parts of one tariff.
  6. **When in doubt, return null.** It is much better to leave a row
     unmatched than to merge two different products. The supersede is
     irreversible (the API will hide the OPENEI row).
  7. Use 'high' confidence when names share rate-code letters/numbers
     OR are near-identical text. Use 'medium' when family clearly
     matches but variant is uncertain. Use 'low' rarely; prefer null."""


def _build_user_prompt(utility_name: str, state: str,
                       fresh: list[dict], stranded: list[dict]) -> str:
    parts = [f"Utility: {utility_name} ({state})\n"]
    parts.append(f"FRESH ({len(fresh)} tariffs):")
    for t in fresh:
        parts.append(f"  id={t['id']} class={t['customer_class'][:3]} rate_type={t['rate_type']}")
        parts.append(f"    name: {t['name']}")
    parts.append(f"\nOPENEI STRANDED ({len(stranded)} tariffs, need to be paired or left null):")
    for t in stranded:
        parts.append(f"  id={t['id']} class={t['customer_class'][:3]} rate_type={t['rate_type']}")
        parts.append(f"    name: {t['name']}")
    parts.append("\nReturn a pairing for every OPENEI id above.")
    return "\n".join(parts)


# Max stranded seeds per LLM call. URDB can explode one utility into
# hundreds of permutation rows; packing them all into one call overflows
# the 8192-token tool output and silently truncates pairings mid-array.
# Each call still sees the FULL fresh catalog (typically <20 rows).
STRANDED_BATCH = 80


def _pair_one_utility(client: anthropic.Anthropic, utility_name: str, state: str,
                      fresh: list[dict], stranded: list[dict]) -> list[dict]:
    all_pairings: list[dict] = []
    for i in range(0, len(stranded), STRANDED_BATCH):
        chunk = stranded[i:i + STRANDED_BATCH]
        user = _build_user_prompt(utility_name, state, fresh, chunk)
        resp = anthropic_compat.create(
            client.messages,
            model=HAIKU_MODEL,
            max_tokens=8192,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user}],
            tools=[PAIRING_TOOL],
            tool_choice={"type": "tool", "name": "report_pairings"},
        )
        with llm_cost.phase("trackb"):
            llm_cost.record_anthropic(HAIKU_MODEL, getattr(resp, "usage", None))
        for block in resp.content:
            if block.type == "tool_use" and block.name == "report_pairings":
                all_pairings.extend(block.input.get("pairings", []))
                break
        if len(stranded) > STRANDED_BATCH:
            time.sleep(0.5)
    return all_pairings


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true",
                        help="actually write supersede pointers (default dry-run)")
    parser.add_argument("--utility", type=int, help="process a single utility_id only")
    parser.add_argument("--confidence", choices=["high", "medium", "low"], default="high",
                        help="minimum LLM confidence to apply (default: high)")
    parser.add_argument("--limit", type=int, default=0, help="stop after N utilities")
    args = parser.parse_args()

    min_conf = CONF_RANK[args.confidence]

    url = os.environ.get("SYNC_DATABASE_URL") or os.environ["DATABASE_URL"].replace("+asyncpg", "")
    from app.db.session import normalize_sync_url
    engine = create_engine(normalize_sync_url(url))
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    print(f"\nLLM-supersede {'LIVE' if args.apply else 'DRY-RUN'}  "
          f"model={HAIKU_MODEL}  min-conf={args.confidence}")
    print("=" * 70)
    t_start = time.time()

    with Session(engine) as session:
        if args.utility:
            util_rows = session.execute(text("""
                SELECT u.id, u.name, u.state_province
                FROM utilities u
                WHERE u.id = :uid
            """), {"uid": args.utility}).all()
        else:
            util_rows = session.execute(text("""
                SELECT u.id, u.name, u.state_province
                FROM utilities u
                JOIN tariffs t ON t.utility_id = u.id
                GROUP BY u.id, u.name, u.state_province
                HAVING COUNT(*) FILTER (WHERE t.last_verified_at IS NOT NULL
                                        AND t.superseded_by_tariff_id IS NULL
                                        AND t.supersede_reason IS NULL) > 0
                   AND COUNT(*) FILTER (WHERE t.last_verified_at IS NULL
                                        AND t.openei_id IS NOT NULL
                                        AND t.superseded_by_tariff_id IS NULL
                                        AND t.supersede_reason IS NULL) > 0
                ORDER BY COUNT(*) FILTER (WHERE t.last_verified_at IS NULL
                                          AND t.openei_id IS NOT NULL
                                          AND t.superseded_by_tariff_id IS NULL
                                          AND t.supersede_reason IS NULL) DESC
            """)).all()

        if args.limit:
            util_rows = util_rows[: args.limit]

        print(f"Processing {len(util_rows)} utilities\n")

        grand_total_proposed = 0
        grand_total_applied = 0
        grand_conf = defaultdict(int)
        failures = []

        for i, u in enumerate(util_rows, start=1):
            fresh_q = session.execute(text("""
                SELECT id, name, customer_class, rate_type FROM tariffs
                WHERE utility_id = :uid AND last_verified_at IS NOT NULL
                  AND superseded_by_tariff_id IS NULL
                  AND supersede_reason IS NULL
                ORDER BY customer_class, name
            """), {"uid": u.id}).all()
            stranded_q = session.execute(text("""
                SELECT id, name, customer_class, rate_type FROM tariffs
                WHERE utility_id = :uid AND last_verified_at IS NULL
                  AND openei_id IS NOT NULL
                  AND superseded_by_tariff_id IS NULL
                  AND supersede_reason IS NULL
                ORDER BY customer_class, name
            """), {"uid": u.id}).all()

            if not fresh_q or not stranded_q:
                continue

            fresh    = [dict(r._mapping) for r in fresh_q]
            stranded = [dict(r._mapping) for r in stranded_q]

            try:
                pairings = _pair_one_utility(client, u.name, u.state_province, fresh, stranded)
            except Exception as e:
                failures.append((u.id, u.name, str(e)[:100]))
                print(f"  [{i}/{len(util_rows)}] FAIL util_id={u.id} {u.name[:35]}: {e}")
                continue

            non_null = [p for p in pairings if p.get("fresh_id") is not None]
            applied = 0
            fresh_ids = {f["id"] for f in fresh}
            stranded_ids = {s["id"] for s in stranded}
            fresh_class = {f["id"]: f["customer_class"] for f in fresh}
            stranded_class = {s["id"]: s["customer_class"] for s in stranded}
            for p in non_null:
                conf = p.get("confidence", "low")
                grand_conf[conf] += 1
                if CONF_RANK.get(conf, 0) < min_conf:
                    continue
                if p["fresh_id"] not in fresh_ids or p["stranded_id"] not in stranded_ids:
                    continue
                # Hard guard: never pair across customer classes, even if
                # the LLM proposes it (prompt rule 1 is advisory; this is
                # enforcement — the supersede hides the row from the API).
                if fresh_class.get(p["fresh_id"]) != stranded_class.get(p["stranded_id"]):
                    print(f"        SKIP cross-class pairing: stranded "
                          f"{p['stranded_id']} -> fresh {p['fresh_id']}")
                    continue
                applied += 1
                if args.apply:
                    session.execute(
                        sa_update(Tariff)
                        .where(Tariff.id == p["stranded_id"])
                        .values(superseded_by_tariff_id=p["fresh_id"],
                                supersede_reason="llm_absorb")
                    )

            grand_total_proposed += len(non_null)
            grand_total_applied  += applied
            if args.apply:
                session.commit()

            print(f"  [{i:>4}/{len(util_rows)}] util_id={u.id:<5} {u.state_province} "
                  f"{u.name[:32]:32s}  fresh={len(fresh):>3}  stranded={len(stranded):>3}  "
                  f"proposed={len(non_null):>3}  applied={applied:>3}")

    elapsed = time.time() - t_start
    print()
    print("=" * 70)
    print(f"SUMMARY ({'APPLIED' if args.apply else 'DRY-RUN'}, min-conf={args.confidence})")
    print("=" * 70)
    print(f"  Utilities processed:      {len(util_rows)}")
    print(f"  Non-null proposals:       {grand_total_proposed}  "
          f"(high={grand_conf['high']}, medium={grand_conf['medium']}, low={grand_conf['low']})")
    print(f"  Total applied:            {grand_total_applied}")
    print(f"  Failures:                 {len(failures)}")
    print(f"  Elapsed:                  {elapsed:.0f}s  "
          f"({elapsed / max(1, len(util_rows)):.2f}s/utility)")
    cost = llm_cost.summary()
    print(f"  LLM cost:                 ${cost['total_usd']:.4f}")
    llm_cost.append_ledger("trackb", cost)


if __name__ == "__main__":
    sys.exit(main())
