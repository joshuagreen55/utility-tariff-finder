"""Audit + backfill blank ``utilities.website_url``, then reclassify tariff sources.

Issue #30. ``classify_source`` can only call a tariff ``official`` when it
has an official host to compare ``source_url`` against. Most active
utilities have no ``website_url`` (and no configured rate URLs), so their
tariffs sit at ``unknown`` / ``no_official_host`` even when they cite the
utility's own site. After #29 that was ~1,321 of 1,425 active utilities and
~5,383 live verified residential tariffs; run the audit for current numbers.

Inference, per active utility with a blank (NULL / whitespace) website:

1. Take the ``source_url`` of every live, verified residential tariff.
2. Drop hosts that cannot be the utility's own site: the aggregator
   blocklist (``THIRD_PARTY_DOMAINS``), generic file hosts
   (``GENERIC_HOSTS``) and regulator publishers (``REGULATOR_PUBLISHERS``,
   e.g. ``oeb.ca`` publishes every Ontario LDC's commodity price).
3. Group the rest by registrable domain (``www.`` / subdomains collapse);
   the most-cited domain is the candidate. The stored value is the bare or
   ``www.`` origin as the tariffs cite it, else ``https://<domain>``.
4. ``accept`` only when the candidate has at least ``--min-tariffs``
   tariffs (default 2) and at least ``--min-share`` (default 0.6) of the
   eligible ones, with no tie. Otherwise ``review``, with a reason:
   ``thin_evidence``, ``no_majority``, ``tie``,
   ``jurisdiction_government_domain`` (a state / province / federal-wide
   domain such as ``nh.gov``, ``texas.gov``, ``gov.bc.ca``, ``state.nh.us``,
   ``ferc.gov`` — never a utility's own site, usually a PSC docket copy),
   ``government_host`` (any other government candidate, e.g. ``seattle.gov``,
   for a utility that is not municipal / political subdivision / state /
   federal), ``shared_domain`` (another active utility's website or
   candidate is on the same domain — often a parent company, sometimes a
   PSC; ``--allow-shared`` accepts non-government shared domains).

Only ``accept`` rows are written. ``review`` rows are for a human: export
them with ``--export-csv``, fill ``website_url`` on the rows you trust and
feed the file back with ``--csv`` (curated rows override inference for
their utility; ``--csv-only`` applies nothing else). Curated URLs must
parse and must not be on the blocklist, a generic host or a regulator
publisher; any bad row aborts before a write.

Writes (``--apply`` only):

- ``utilities.website_url`` (+ ``updated_at``), only where it is still
  blank at write time. ``--force`` also considers utilities that already
  have a website on a different domain than the evidence, and replaces a
  value only if it is unchanged since the audit read it.
- then ``reclassify_tariffs`` for exactly those utilities, which writes
  ``tariffs.source_type`` / ``source_type_reason`` and nothing else. No rate
  components, rate values, ``last_verified_at``, supersede columns or
  change events are touched.
- one JSON line per utility (old → new website, evidence) appended to
  ``$APP_LOG_DIR/website_url_backfill.jsonl`` (``--log-path``). To undo,
  set those rows back to the logged ``old_website_url`` and run
  ``python -m scripts.backfill_source_type --utility-id ... --apply``.

Batches of ``--batch-size`` utilities commit separately (website + reclassify
in one transaction each) so row locks stay short; re-running is safe.

``website_url`` also steers the extraction pipeline (Phase 1 domain-scoped
search and direct rate-page probes, Phase 5 homepage navigation), which is
another reason inference is conservative. Workers read it per task, so no
worker restart is needed. Production ``--apply``: between freshness batches
or after the campaign (AGENTS.md §6).

Usage (inside the api container):
  python -m scripts.backfill_website_url                        # audit (dry run)
  python -m scripts.backfill_website_url --samples 50 --export-csv /app/logs/website_candidates.csv
  python -m scripts.backfill_website_url --csv reviewed.csv     # dry run incl. curated rows
  python -m scripts.backfill_website_url --apply                # write accepted + curated rows
  python -m scripts.backfill_website_url --csv reviewed.csv --csv-only --apply
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import urlparse

from app.services.source_type import (
    UtilitySourceContext,
    classify_source,
    context_from_utility,
    is_generic_host,
    is_government_host,
    is_regulator_publisher_host,
    is_third_party_host,
    normalize_host,
    registrable_domain,
)

ACCEPT = "accept"
REVIEW = "review"
NONE = "none"

DEFAULT_MIN_TARIFFS = 2
DEFAULT_MIN_SHARE = 0.6
DEFAULT_BATCH_SIZE = 200
LOG_FILENAME = "website_url_backfill.jsonl"

# Utility types whose own site is often a .gov (city / district / state /
# federal power agency). utility_type is stored by enum name.
PUBLIC_UTILITY_TYPES = frozenset({"MUNICIPAL", "POLITICAL_SUBDIVISION", "FEDERAL", "STATE"})

# Registrable domains shared by a whole state / province / federal government
# (``puc.nh.gov`` → ``nh.gov``, ``psc.texas.gov`` → ``texas.gov``). As a
# utility's website they would make every agency page, PSC dockets included,
# look official for it, so they are never accepted automatically.
_STATE_GOV_LABELS = frozenset(
    "al ak az ar ca co ct de dc fl ga hi id il in ia ks ky la me md ma mi mn ms mo mt ne nv nh nj "
    "nm ny nc nd oh ok or pa ri sc sd tn tx ut vt va wa wv wi wy pr gu vi "
    "alabama alaska arizona arkansas california colorado connecticut delaware florida myflorida "
    "georgia hawaii idaho illinois indiana iowa kansas kentucky louisiana maine maryland mass "
    "massachusetts michigan minnesota mississippi missouri montana nebraska nevada newhampshire "
    "newjersey newmexico newyork northcarolina northdakota ohio oklahoma oregon pennsylvania "
    "rhodeisland southcarolina southdakota tennessee texas utah vermont virginia washington "
    "westvirginia wisconsin wyoming".split()
)
_FEDERAL_GOV_DOMAINS = frozenset({"ferc.gov", "energy.gov", "usa.gov", "regulations.gov", "doe.gov"})

EXPORT_COLUMNS = (
    "utility_id", "website_url", "candidate_url", "status", "reasons", "name",
    "state_province", "current_website_url", "domain", "support", "eligible", "share",
    "shared_with",
)


@dataclass(frozen=True)
class UtilityEvidence:
    utility_id: int
    name: str = ""
    utility_type: str | None = None
    website_url: str | None = None
    source_urls: tuple[str, ...] = ()


@dataclass
class Inference:
    utility_id: int
    status: str = NONE
    candidate_url: str | None = None
    domain: str | None = None
    support: int = 0
    eligible: int = 0
    share: float = 0.0
    government: bool = False
    reasons: list[str] = field(default_factory=list)
    excluded: Counter = field(default_factory=Counter)
    shared_with: tuple[int, ...] = ()


@dataclass(frozen=True)
class CuratedRow:
    utility_id: int
    website_url: str
    note: str | None = None


@dataclass(frozen=True)
class PlannedUpdate:
    utility_id: int
    old_website_url: str | None
    new_website_url: str
    origin: str  # "inferred" | "csv"
    detail: Mapping[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Inference rules (pure)
# ---------------------------------------------------------------------------

def is_blank(website_url: str | None) -> bool:
    return not (website_url or "").strip()


def host_exclusion(url: str | None) -> str | None:
    """Why a source host cannot be the utility's own website (None = eligible)."""
    host = normalize_host(url)
    if not host:
        return "no_url"
    if is_third_party_host(host):
        return "third_party"
    if is_generic_host(host):
        return "generic_host"
    if is_regulator_publisher_host(host):
        return "regulator_publisher"
    return None


def is_jurisdiction_government_domain(domain: str) -> bool:
    """State / province / federal-wide government domain (not a city's or agency's own)."""
    labels = (domain or "").lower().split(".")
    if domain in _FEDERAL_GOV_DOMAINS:
        return True
    if len(labels) == 2 and labels[1] in ("gov", "mil") and labels[0] in _STATE_GOV_LABELS:
        return True
    if labels[-2:] == ["gc", "ca"] or (len(labels) == 3 and labels[0] == "gov" and labels[2] == "ca"):
        return True
    return labels[-1] == "us" and "state" in labels


def _origin(url: str) -> tuple[str, str] | None:
    raw = url.strip()
    if "://" not in raw:
        raw = f"https://{raw}"
    try:
        p = urlparse(raw)
        host = (p.hostname or "").lower().strip(".")
    except ValueError:
        return None
    scheme = p.scheme.lower() if p.scheme.lower() in ("http", "https") else "https"
    return scheme, host


def website_for_domain(domain: str, urls: Iterable[str]) -> str:
    """Origin to store for ``domain``: its bare / ``www.`` host as cited, else ``https://domain``."""
    seen: Counter = Counter()
    for url in urls:
        o = _origin(url or "")
        if o and o[1] in (domain, f"www.{domain}"):
            seen[f"{o[0]}://{o[1]}"] += 1
    if not seen:
        return f"https://{domain}"
    return sorted(seen, key=lambda o: (-seen[o], not o.startswith("https://"), o))[0]


def infer_website(
    ev: UtilityEvidence,
    *,
    min_tariffs: int = DEFAULT_MIN_TARIFFS,
    min_share: float = DEFAULT_MIN_SHARE,
) -> Inference:
    inf = Inference(ev.utility_id)
    if not ev.source_urls:
        inf.reasons.append("no_verified_residential")
        return inf
    by_domain: dict[str, list[str]] = defaultdict(list)
    for url in ev.source_urls:
        why = host_exclusion(url)
        if why:
            inf.excluded[why] += 1
        else:
            by_domain[registrable_domain(normalize_host(url))].append(url)
    if not by_domain:
        inf.reasons.append("no_eligible_host")
        return inf

    ranked = sorted(by_domain.items(), key=lambda kv: (-len(kv[1]), kv[0]))
    domain, urls = ranked[0]
    inf.domain = domain
    inf.support = len(urls)
    inf.eligible = sum(len(v) for v in by_domain.values())
    inf.share = inf.support / inf.eligible
    inf.candidate_url = website_for_domain(domain, urls)
    inf.government = is_government_host(urls[0])

    current = normalize_host(ev.website_url)
    if current and registrable_domain(current) == domain:
        inf.reasons.append("matches_existing")
        return inf

    if len(ranked) > 1 and len(ranked[1][1]) == inf.support:
        inf.reasons.append("tie")
    elif inf.share < min_share:
        inf.reasons.append("no_majority")
    if inf.support < min_tariffs:
        inf.reasons.append("thin_evidence")
    if inf.government and is_jurisdiction_government_domain(domain):
        inf.reasons.append("jurisdiction_government_domain")
    elif inf.government and (ev.utility_type or "").upper() not in PUBLIC_UTILITY_TYPES:
        inf.reasons.append("government_host")
    inf.status = REVIEW if inf.reasons else ACCEPT
    return inf


def mark_shared_domains(
    inferences: Iterable[Inference],
    existing_websites: Mapping[int, str | None],
    *,
    allow_shared: bool = False,
) -> None:
    """Flag candidates whose domain another active utility already uses or is inferred to use.

    ``existing_websites`` is every active utility's current ``website_url``.
    Shared government domains always stay in review.
    """
    inferences = list(inferences)
    users: dict[str, set[int]] = defaultdict(set)
    for uid, site in existing_websites.items():
        host = normalize_host(site)
        if host:
            users[registrable_domain(host)].add(uid)
    for inf in inferences:
        if inf.status in (ACCEPT, REVIEW) and inf.domain:
            users[inf.domain].add(inf.utility_id)
    for inf in inferences:
        if inf.status not in (ACCEPT, REVIEW) or not inf.domain:
            continue
        others = tuple(sorted(users[inf.domain] - {inf.utility_id}))
        if not others:
            continue
        inf.shared_with = others
        if inf.government:
            inf.reasons.append("shared_government_host")
        elif not allow_shared:
            inf.reasons.append("shared_domain")
        else:
            continue
        inf.status = REVIEW


# ---------------------------------------------------------------------------
# Curated CSV + plan (pure)
# ---------------------------------------------------------------------------

def normalize_website(url: str) -> str:
    url = url.strip()
    return url if "://" in url else f"https://{url}"


def curated_url_problem(url: str) -> str | None:
    why = host_exclusion(url)
    return {
        "no_url": "is not a URL",
        "third_party": "is on the third-party blocklist",
        "generic_host": "is a generic file host",
        "regulator_publisher": "is a regulator publisher host",
    }.get(why) if why else None


def load_curated_csv(path: str) -> tuple[dict[int, CuratedRow], list[str]]:
    """``utility_id,website_url[,note]`` (extra columns ignored; blank website rows skipped)."""
    rows: dict[int, CuratedRow] = {}
    errors: list[str] = []
    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        cols = set(reader.fieldnames or ())
        if not {"utility_id", "website_url"} <= cols:
            return {}, [f"{path}: needs utility_id and website_url columns"]
        for lineno, row in enumerate(reader, start=2):
            raw_id = (row.get("utility_id") or "").strip()
            url = (row.get("website_url") or "").strip()
            if not url:
                continue
            try:
                uid = int(raw_id)
            except ValueError:
                errors.append(f"line {lineno}: bad utility_id {raw_id!r}")
                continue
            problem = curated_url_problem(url)
            if problem:
                errors.append(f"line {lineno}: {url!r} {problem}")
                continue
            website = normalize_website(url)
            if uid in rows and rows[uid].website_url != website:
                errors.append(f"line {lineno}: utility {uid} listed twice with different websites")
                continue
            rows[uid] = CuratedRow(uid, website, (row.get("note") or "").strip() or None)
    return rows, errors


def build_plan(
    inferences: Iterable[Inference],
    current_websites: Mapping[int, str | None],
    curated: Mapping[int, CuratedRow] | None = None,
    *,
    force: bool = False,
    csv_only: bool = False,
) -> tuple[list[PlannedUpdate], list[tuple[int, str]]]:
    """Updates to write, and ``(utility_id, reason)`` for curated / accepted rows left alone."""
    curated = curated or {}
    plan: list[PlannedUpdate] = []
    skipped: list[tuple[int, str]] = []

    def add(uid: int, new: str, origin: str, detail: Mapping[str, Any]) -> None:
        if uid not in current_websites:
            skipped.append((uid, "unknown_or_inactive_utility"))
            return
        old = current_websites[uid]
        if not force and not is_blank(old):
            skipped.append((uid, "has_website"))
            return
        if not is_blank(old) and old.strip() == new:
            skipped.append((uid, "unchanged"))
            return
        plan.append(PlannedUpdate(uid, old, new, origin, detail))

    for uid in sorted(curated):
        row = curated[uid]
        add(uid, row.website_url, "csv", {"note": row.note} if row.note else {})
    if not csv_only:
        for inf in inferences:
            if inf.status == ACCEPT and inf.utility_id not in curated and inf.candidate_url:
                add(inf.utility_id, inf.candidate_url, "inferred", {
                    "domain": inf.domain, "support": inf.support,
                    "eligible": inf.eligible, "share": round(inf.share, 3),
                })
    plan.sort(key=lambda p: p.utility_id)
    return plan, skipped


def project_source_types(
    tariffs: Iterable[tuple[int, str | None, str | None, str | None]],
    contexts: Mapping[int, UtilitySourceContext],
    new_websites: Mapping[int, str],
) -> Counter:
    """``(old_type, old_reason, new_type, new_reason) → n`` for tariffs of utilities in ``new_websites``."""
    out: Counter = Counter()
    for uid, url, old_type, old_reason in tariffs:
        if uid not in new_websites:
            continue
        ctx = replace(contexts.get(uid) or UtilitySourceContext(), website_url=new_websites[uid])
        res = classify_source(url, ctx)
        out[(old_type, old_reason, res.source_type, res.reason)] += 1
    return out


def export_rows(
    inferences: Iterable[Inference], utilities: Mapping[int, Mapping[str, Any]]
) -> list[dict[str, Any]]:
    rows = []
    for inf in inferences:
        if inf.status not in (ACCEPT, REVIEW):
            continue
        u = utilities.get(inf.utility_id, {})
        rows.append({
            "utility_id": inf.utility_id,
            "website_url": inf.candidate_url if inf.status == ACCEPT else "",
            "candidate_url": inf.candidate_url,
            "status": inf.status,
            "reasons": ";".join(inf.reasons),
            "name": u.get("name", ""),
            "state_province": u.get("state_province", ""),
            "current_website_url": u.get("website_url") or "",
            "domain": inf.domain,
            "support": inf.support,
            "eligible": inf.eligible,
            "share": round(inf.share, 3),
            "shared_with": ";".join(str(i) for i in inf.shared_with),
        })
    return rows


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

_UTILITIES_SQL = (
    "SELECT id, name, utility_type::text AS utility_type, website_url, tariff_page_urls, "
    "rate_page_url_override, country::text AS country, state_province "
    "FROM utilities WHERE is_active"
)
_TARIFFS_SQL = (
    "SELECT utility_id, source_url, source_type, source_type_reason FROM tariffs "
    "WHERE utility_id = ANY(:uids) "
    "AND lower(customer_class::text) = 'residential' "
    "AND last_verified_at IS NOT NULL "
    "AND superseded_by_tariff_id IS NULL AND supersede_reason IS NULL"
)


@dataclass
class AuditReport:
    active_utilities: int
    in_scope: list[int]
    utilities: dict[int, dict[str, Any]]
    tariffs: list[tuple[int, str | None, str | None, str | None]]
    inferences: list[Inference]
    plan: list[PlannedUpdate]
    skipped: list[tuple[int, str]]
    projection: Counter
    force: bool = False


def audit(
    conn,
    *,
    utility_ids: Sequence[int] | None = None,
    force: bool = False,
    min_tariffs: int = DEFAULT_MIN_TARIFFS,
    min_share: float = DEFAULT_MIN_SHARE,
    allow_shared: bool = False,
    curated: Mapping[int, CuratedRow] | None = None,
    csv_only: bool = False,
) -> AuditReport:
    """Read-only: evidence, inferences, the update plan and its projected classification."""
    from sqlalchemy import text

    utilities = {r["id"]: dict(r) for r in conn.execute(text(_UTILITIES_SQL)).mappings()}
    wanted = set(utility_ids) if utility_ids else None
    in_scope = sorted(
        uid for uid, u in utilities.items()
        if (wanted is None or uid in wanted) and (force or is_blank(u["website_url"]))
    )
    tariffs = [
        tuple(r) for r in conn.execute(text(_TARIFFS_SQL), {"uids": in_scope})
    ] if in_scope else []
    urls_by_uid: dict[int, list[str]] = defaultdict(list)
    for uid, url, _, _ in tariffs:
        urls_by_uid[uid].append(url or "")

    inferences = []
    for uid in in_scope:
        u = utilities[uid]
        ev = UtilityEvidence(
            uid, u["name"], u["utility_type"], u["website_url"], tuple(urls_by_uid.get(uid, ())),
        )
        inferences.append(infer_website(ev, min_tariffs=min_tariffs, min_share=min_share))
    mark_shared_domains(
        inferences, {uid: u["website_url"] for uid, u in utilities.items()}, allow_shared=allow_shared,
    )
    plan, skipped = build_plan(
        inferences, {uid: u["website_url"] for uid, u in utilities.items()},
        curated, force=force, csv_only=csv_only,
    )

    plan_ids = [p.utility_id for p in plan]
    extra = sorted(set(plan_ids) - set(in_scope))
    if extra:
        tariffs += [tuple(r) for r in conn.execute(text(_TARIFFS_SQL), {"uids": extra})]
    contexts = {uid: context_from_utility(utilities[uid]) for uid in plan_ids}
    projection = project_source_types(tariffs, contexts, {p.utility_id: p.new_website_url for p in plan})
    return AuditReport(
        active_utilities=len(utilities), in_scope=in_scope, utilities=utilities, tariffs=tariffs,
        inferences=inferences, plan=plan, skipped=skipped, projection=projection, force=force,
    )


def default_log_path() -> str:
    return os.path.join(os.environ.get("APP_LOG_DIR", "/app/logs"), LOG_FILENAME)


def apply_plan(
    engine,
    plan: Sequence[PlannedUpdate],
    *,
    force: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    log_path: str | None = None,
) -> dict:
    """Write ``website_url`` for ``plan`` and reclassify those utilities' tariffs.

    The UPDATE re-checks the row at write time: blank (default) or unchanged
    since the audit (``force``). Rows that moved in between are reported in
    ``raced`` and not written.
    """
    from sqlalchemy import text

    from app.services.source_type import reclassify_tariffs

    log_path = log_path or default_log_path()
    os.makedirs(os.path.dirname(os.path.abspath(log_path)), exist_ok=True)
    guard = (
        "website_url IS NOT DISTINCT FROM CAST(:old AS text)" if force
        else "coalesce(btrim(website_url), '') = ''"
    )
    stmt = text(
        f"UPDATE utilities SET website_url = :new, updated_at = now() WHERE id = :id AND {guard}"
    )
    updated: list[int] = []
    raced: list[int] = []
    counts: Counter = Counter()
    changed: Counter = Counter()
    step = max(1, batch_size)
    with open(log_path, "a", encoding="utf-8") as log:
        for i in range(0, len(plan), step):
            batch = plan[i:i + step]
            done: list[PlannedUpdate] = []
            with engine.begin() as conn:
                for p in batch:
                    res = conn.execute(stmt, {"id": p.utility_id, "new": p.new_website_url, "old": p.old_website_url})
                    if res.rowcount:
                        done.append(p)
                    else:
                        raced.append(p.utility_id)
                if done:
                    rc = reclassify_tariffs(conn, utility_ids=[p.utility_id for p in done])
                    counts.update(rc["counts"])
                    changed.update(rc["changed"])
            ts = datetime.now(timezone.utc).isoformat()
            for p in done:
                log.write(json.dumps({
                    "ts": ts, "utility_id": p.utility_id, "old_website_url": p.old_website_url,
                    "new_website_url": p.new_website_url, "origin": p.origin, "force": force,
                    **dict(p.detail),
                }) + "\n")
            log.flush()
            updated += [p.utility_id for p in done]
    return {
        "updated": updated, "raced": raced, "log_path": log_path,
        "reclassify": {"counts": dict(counts), "changed": dict(changed)},
    }


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def _pct(n: int, d: int) -> str:
    return f"{100 * n / d:.1f}%" if d else "-"


def print_report(r: AuditReport, *, samples: int = 20, apply: bool = False) -> None:
    by_status = Counter(i.status for i in r.inferences)
    reasons: Counter = Counter(reason for i in r.inferences for reason in i.reasons)
    excluded: Counter = Counter()
    for i in r.inferences:
        excluded.update(i.excluded)
    in_scope = set(r.in_scope)
    scope_tariffs = [t for t in r.tariffs if t[0] in in_scope]
    labels = Counter((t[2], t[3]) for t in scope_tariffs)

    print(f"{'APPLY' if apply else 'DRY RUN'}: utilities.website_url backfill")
    scope = "active utilities (--force: all)" if r.force else "active utilities with blank website_url"
    print(f"  active utilities:          {r.active_utilities:>7,}")
    print(f"  in scope ({scope}): {len(in_scope):,}")
    print(f"  live verified residential tariffs in scope: {len(scope_tariffs):,}")
    for (st, reason), n in sorted(labels.items(), key=lambda kv: -kv[1]):
        print(f"    {st or '-':<12} {reason or '-':<22} {n:>8,}")
    print("  inference:")
    for status in (ACCEPT, REVIEW, NONE):
        print(f"    {status:<8} {by_status.get(status, 0):>7,}")
    if reasons:
        print("  reasons (review / none):")
        for reason, n in reasons.most_common():
            print(f"    {reason:<32} {n:>7,}")
    if excluded:
        print("  source hosts excluded from inference (tariff count):")
        for why, n in excluded.most_common():
            print(f"    {why:<32} {n:>7,}")

    origins = Counter(p.origin for p in r.plan)
    print(f"  planned website_url writes: {len(r.plan):,} "
          f"({origins.get('inferred', 0):,} inferred, {origins.get('csv', 0):,} curated CSV)")
    if r.skipped:
        print("  not written:")
        for why, n in Counter(w for _, w in r.skipped).most_common():
            print(f"    {why:<32} {n:>7,}")

    total = sum(r.projection.values())
    moved: Counter = Counter()
    new_labels: Counter = Counter()
    for (ot, _, nt, nr), n in r.projection.items():
        new_labels[(nt, nr)] += n
        if ot != nt:
            moved[(ot, nt)] += n
    print(f"  projected: live verified residential tariffs of planned utilities: {total:,}")
    for (st, reason), n in sorted(new_labels.items(), key=lambda kv: -kv[1]):
        print(f"    → {st:<12} {reason:<22} {n:>8,}  ({_pct(n, total)})")
    for (ot, nt), n in sorted(moved.items(), key=lambda kv: -kv[1]):
        print(f"    {ot or '-':<12} → {nt:<12} {n:>8,}")

    def show(status: str) -> None:
        rows = [i for i in r.inferences if i.status == status][:samples]
        if not rows:
            return
        print(f"  sample {status} ({len(rows)} of {by_status.get(status, 0):,}):")
        for i in rows:
            u = r.utilities.get(i.utility_id, {})
            tail = f"  [{', '.join(i.reasons)}]" if i.reasons else ""
            shared = f" shared_with={list(i.shared_with)[:5]}" if i.shared_with else ""
            print(
                f"    {i.utility_id:>6} {str(u.get('name', ''))[:40]:<40} {u.get('state_province', ''):<3}"
                f" {i.candidate_url or '-':<40} {i.support}/{i.eligible} ({i.share:.0%}){shared}{tail}"
            )

    show(ACCEPT)
    show(REVIEW)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> dict:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="Write website_url + reclassify (default: dry run)")
    parser.add_argument("--force", action="store_true",
                        help="Also replace a non-blank website_url (inferred domain differs, or curated CSV)")
    parser.add_argument("--utility-id", type=int, action="append", help="Limit to these utilities (repeatable)")
    parser.add_argument("--min-tariffs", type=int, default=DEFAULT_MIN_TARIFFS,
                        help=f"Tariffs the candidate domain needs to be accepted (default {DEFAULT_MIN_TARIFFS})")
    parser.add_argument("--min-share", type=float, default=DEFAULT_MIN_SHARE,
                        help=f"Share of eligible tariffs the candidate needs (default {DEFAULT_MIN_SHARE})")
    parser.add_argument("--allow-shared", action="store_true",
                        help="Accept candidates on a domain other utilities use (non-government only)")
    parser.add_argument("--csv", help="Curated utility_id,website_url[,note] CSV (overrides inference)")
    parser.add_argument("--csv-only", action="store_true", help="Write only the curated CSV rows")
    parser.add_argument("--export-csv", help="Write accept + review candidates here for human review")
    parser.add_argument("--samples", type=int, default=20, help="Sample rows to print per status")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                        help=f"Utilities per commit on --apply (default {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--log-path", help=f"JSONL audit log (default $APP_LOG_DIR/{LOG_FILENAME})")
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])
    if args.csv_only and not args.csv:
        parser.error("--csv-only needs --csv")

    curated: dict[int, CuratedRow] = {}
    if args.csv:
        curated, errors = load_curated_csv(args.csv)
        if errors:
            print("Curated CSV rejected (nothing written):", file=sys.stderr)
            for e in errors:
                print(f"  {e}", file=sys.stderr)
            raise SystemExit(2)

    from app.db.session import get_sync_engine

    engine = get_sync_engine()
    with engine.connect() as conn:
        report = audit(
            conn, utility_ids=args.utility_id, force=args.force, min_tariffs=args.min_tariffs,
            min_share=args.min_share, allow_shared=args.allow_shared, curated=curated,
            csv_only=args.csv_only,
        )
    print_report(report, samples=args.samples, apply=args.apply)

    if args.export_csv:
        rows = export_rows(report.inferences, report.utilities)
        with open(args.export_csv, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=EXPORT_COLUMNS)
            w.writeheader()
            w.writerows(rows)
        print(f"  exported {len(rows):,} candidates → {args.export_csv}")

    result: dict = {"report": report, "applied": None}
    if not args.apply:
        print("  dry run: nothing written (pass --apply to write website_url + reclassify)")
        return result
    if not report.plan:
        print("  nothing to write")
        return result

    applied = apply_plan(
        engine, report.plan, force=args.force, batch_size=args.batch_size, log_path=args.log_path,
    )
    result["applied"] = applied
    print(f"  wrote website_url for {len(applied['updated']):,} utilities (log: {applied['log_path']})")
    if applied["raced"]:
        print(f"  skipped {len(applied['raced']):,} whose website_url changed since the audit: "
              f"{applied['raced'][:20]}")
    rc = applied["reclassify"]
    print("  reclassified tariffs of those utilities (all rows, live + superseded):")
    for (st, reason), n in sorted(rc["counts"].items(), key=lambda kv: (kv[0][0], -kv[1])):
        print(f"    {st:<12} {reason:<22} {n:>8,}")
    for (old, new), n in sorted(rc["changed"].items(), key=lambda kv: -kv[1]):
        print(f"    {old or '-':<12} → {new:<12} {n:>8,}")
    return result


if __name__ == "__main__":
    main()
