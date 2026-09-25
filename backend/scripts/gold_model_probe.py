"""Live model A/B on the TOU / seasonal gold set (dry-run: no DB reads or writes).

Fetches each gold utility's source document(s), runs Phase 3 extraction with
whatever models the env selects (``GEMINI_MODEL`` / ``HAIKU_MODEL`` /
``OPUS_MODEL``) and Phase 4 validation, then scores the result against the
gold with the benchmark's strict comparison, the failure taxonomy and the
computable contract, and prices the calls with ``llm_cost``. Every config
reads the same documents, so two runs are a fair before/after.

    cd backend   # on the VM: inside the api container
    python -m scripts.gold_model_probe --no-cache --output /tmp/gold_base.json
    HAIKU_MODEL=claude-sonnet-5 OPUS_MODEL=claude-opus-5-5 \\
        python -m scripts.gold_model_probe --no-cache --output /tmp/gold_cand.json
    python -m scripts.gold_model_probe --compare /tmp/gold_base.json /tmp/gold_cand.json

``--add-url "San Diego Gas & Electric=https://www.sdge.com/total-electric-rates"``
adds a document (e.g. the page that states TOU periods next to a price
table). Utilities without a ``rate_url`` and no ``--add-url`` are reported as
skipped, not scored. Spends real LLM money (one Phase 3 pass per document).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from scripts import llm_cost
from scripts.benchmark import (
    TOU_SEASONAL_GOLD_PATH,
    Tolerance,
    _compare_components,
    _find_best_match,
    failure_taxonomy,
    top_failure,
)

log = logging.getLogger("gold_model_probe")


def _fetch(url: str):
    from scripts import tariff_pipeline as tp

    if url.lower().split("?")[0].endswith(".pdf"):
        pdf_bytes = tp._download_pdf(url)
        text = tp.fetch_pdf_text(url) if pdf_bytes else ""
        if not text:
            return None
        return tp.RatePage(
            url=url, title=url.rsplit("/", 1)[-1], page_type="pdf", content=text,
            content_hash=hashlib.sha256(text.encode()).hexdigest(), pdf_bytes=pdf_bytes,
        )
    return tp._fetch_and_parse(url)


def _as_db_tariff(et):
    from scripts import tariff_pipeline as tp

    return SimpleNamespace(
        name=et.name,
        customer_class=SimpleNamespace(value=et.customer_class),
        rate_type=et.rate_type,
        rate_components=tp._build_rate_components(et),
        extraction_tier=getattr(et, "extraction_tier", ""),
    )


def probe_utility(utility: dict, urls: list[str], tol: Tolerance) -> dict:
    from app.services.computable import evaluate_computable
    from scripts import tariff_pipeline as tp

    llm_cost.reset()
    tp._reset_opus_budget()
    out = {"utility": utility["name"], "state": utility["state"], "urls": urls, "tariffs": [], "errors": []}
    pages = []
    for url in urls:
        try:
            page = _fetch(url)
        except Exception as e:  # noqa: BLE001
            page = None
            out["errors"].append(f"fetch {url}: {e}")
        if page is None:
            out["errors"].append(f"fetch {url}: no content")
        else:
            pages.append(page)

    extracted = []
    if pages:
        try:
            extracted = tp.phase3_extract_tariffs(pages, utility["name"], state=utility["state"])
        except Exception as e:  # noqa: BLE001
            out["errors"].append(f"phase3: {e}")
    _report, valid = tp.phase4_validate(extracted, utility["name"], utility["state"])
    for et in valid:
        et.components = tp.dedupe_rate_components(et.components)
    candidates = [_as_db_tariff(et) for et in valid]
    out["extracted"] = [c.name for c in candidates]

    used = set()
    for gold in utility["tariffs"]:
        best = _find_best_match(gold, [c for c in candidates if id(c) not in used])
        row = {"gold": gold["name"], "code": gold.get("code"), "expect_computable": gold.get("expect_computable")}
        if best is None:
            row.update(matched=False, top_failure="product_match", computable=False,
                       rate_errors=len(gold["components"]), structure_errors=0)
        else:
            used.add(id(best))
            _p, _r, errors = _compare_components(gold["components"], best.rate_components, tol)
            verdict = evaluate_computable(best.rate_type, best.rate_components, name=best.name)
            modes = failure_taxonomy(gold["components"], best.rate_components, errors, tol)
            row.update(
                matched=True, extracted_name=best.name, tier=best.extraction_tier,
                computable=verdict.computable, reasons=list(verdict.reasons),
                rate_errors=len(errors),
                structure_errors=sum(1 for e in errors if e.get("issue") == "missing_structure"),
                failure_modes=modes, top_failure=top_failure(modes, matched=True),
            )
        out["tariffs"].append(row)
    out["cost"] = llm_cost.summary()
    return out


def summarize(utilities: list[dict]) -> dict:
    rows = [t for u in utilities for t in u["tariffs"]]
    cost = llm_cost.merge_summaries([u.get("cost") or {} for u in utilities])
    agree = sum(1 for t in rows if t["expect_computable"] is None or t["computable"] == t["expect_computable"])
    return {
        "gold_tariffs": len(rows),
        "matched": sum(1 for t in rows if t["matched"]),
        "computable_agree": agree,
        "rate_errors": sum(t["rate_errors"] for t in rows),
        "structure_errors": sum(t["structure_errors"] for t in rows),
        "total_usd": cost.get("total_usd", 0.0),
        "by_model_usd": cost.get("by_model", {}),
        "tier_acceptance": cost.get("tier_acceptance", {}),
    }


def compare(a_path: str, b_path: str) -> int:
    a, b = (json.loads(Path(p).read_text()) for p in (a_path, b_path))
    print(f"A: {a['models']}\nB: {b['models']}\n")
    print(f"{'metric':22s} {'A':>12s} {'B':>12s}")
    for key in ("gold_tariffs", "matched", "computable_agree", "rate_errors", "structure_errors", "total_usd"):
        print(f"{key:22s} {a['summary'][key]!s:>12s} {b['summary'][key]!s:>12s}")
    print("\nper gold tariff (top failure / computable):")
    b_rows = {(u["utility"], t["gold"]): t for u in b["utilities"] for t in u["tariffs"]}
    for u in a["utilities"]:
        for t in u["tariffs"]:
            o = b_rows.get((u["utility"], t["gold"]), {})
            print(f"  {u['utility'][:20]:20s} {str(t['code'])[:12]:12s} "
                  f"A {t['top_failure']:17s} {str(t['computable']):5s} | "
                  f"B {o.get('top_failure', '-'):17s} {str(o.get('computable', '-')):5s}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--fixtures", default=str(TOU_SEASONAL_GOLD_PATH))
    parser.add_argument("--add-url", action="append", default=[], metavar="NAME=URL")
    parser.add_argument("--only", action="append", default=[], metavar="NAME")
    parser.add_argument("--no-cache", action="store_true", help="fresh LLM cache dir (measure real calls)")
    parser.add_argument("--output")
    parser.add_argument("--compare", nargs=2, metavar=("A_JSON", "B_JSON"))
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if args.compare:
        return compare(*args.compare)

    from scripts import tariff_pipeline as tp

    data = json.loads(Path(args.fixtures).read_text())
    tol = Tolerance.from_meta(data.get("_meta") or {})
    extra: dict[str, list[str]] = {}
    for spec in args.add_url:
        name, _, url = spec.partition("=")
        extra.setdefault(name.strip(), []).append(url.strip())

    models = {"gemini": tp.GEMINI_MODEL, "haiku": tp.HAIKU_MODEL, "opus": tp.OPUS_MODEL,
              "opus_max_per_utility": tp.OPUS_MAX_PER_UTILITY}
    with tempfile.TemporaryDirectory() as tmp, \
            mock.patch.object(tp, "LLM_CACHE_DIR", tmp if args.no_cache else tp.LLM_CACHE_DIR):
        results, skipped = [], []
        for u in data["utilities"]:
            if args.only and u["name"] not in args.only:
                continue
            urls = ([u["rate_url"]] if u.get("rate_url") else []) + extra.get(u["name"], [])
            if not urls:
                skipped.append(u["name"])
                continue
            log.info(f"== {u['name']} ({len(urls)} document(s))")
            results.append(probe_utility(u, urls, tol))

    report = {"models": models, "skipped": skipped, "summary": summarize(results), "utilities": results}
    llm_cost.append_ledger("gold_model_probe", llm_cost.merge_summaries([r["cost"] for r in results]))
    s = report["summary"]
    print(f"\nmodels: {models}\nskipped (no document): {skipped}")
    print(f"matched {s['matched']}/{s['gold_tariffs']}  computable agreement {s['computable_agree']}/"
          f"{s['gold_tariffs']}  rate errors {s['rate_errors']}  structure misses "
          f"{s['structure_errors']}  spend ${s['total_usd']:.4f} {s['by_model_usd']}")
    for r in results:
        for t in r["tariffs"]:
            print(f"  {r['utility'][:22]:22s} {str(t['code'])[:12]:12s} {t['top_failure']:17s} "
                  f"computable={t['computable']}")
    if args.output:
        Path(args.output).write_text(json.dumps(report, indent=2, default=str))
        print(f"\nwritten {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
