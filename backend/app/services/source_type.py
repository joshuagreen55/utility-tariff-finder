"""Classify where a tariff's rates came from: official, third_party, unknown.

``tariffs.source_type`` answers "did this rate come from the utility's own
site / documents, or from somebody else's copy of it?". Fresh rates copied
from a rate blog should not look as healthy as the utility's own rate book,
and refreshes should go to the official document when one is known.

Rules (``classify_source``), first match wins:

1. no parseable ``source_url`` host                  → unknown     ``no_url``
2. host on ``THIRD_PARTY_DOMAINS`` (aggregators,
   rate blogs, directories, social, news)            → third_party ``aggregator_blocklist``
3. same registrable domain as the utility's
   ``website_url``                                   → official    ``domain_match``
4. same registrable domain as one of the utility's
   configured rate URLs (``rate_page_url_override`` /
   ``tariff_page_urls``) — the official PDF host or
   same-org subdomain already used for that utility  → official    ``configured_host``
5. exact URL (or a path under it) configured for the
   utility on a generic file host (CDN, Squarespace,
   Google Drive, ...)                                → official    ``configured_url``
6. host is the rate *publisher* for the utility's
   jurisdiction (``REGULATOR_PUBLISHERS``)           → official    ``regulator_publisher``
7. generic file host that is not configured          → unknown     ``generic_host``
8. government host (.gov, gc.ca, gov.<prov>.ca,
   state.<xx>.us) that is not the utility's own      → unknown     ``government_host``
9. host differs from every known official host       → third_party ``domain_mismatch``
10. utility has no official host to compare against  → unknown     ``no_official_host``

Regulators on ``REGULATOR_PUBLISHERS`` set the rate itself, so their page
*is* the official publication:

- Ontario — Ontario Energy Board (``oeb.ca``) sets RPP commodity prices
  (TOU / tiered / ULO) for every LDC; ``scrape_oeb_rates.py`` stores them.
- Alberta — Alberta Utilities Commission (``auc.ab.ca``) sets the Rate of
  Last Resort.

Other boards (Régie de l'énergie, BCUC, NSUARB, PUB NL, US state PUCs)
approve a utility's own tariff; the utility's document is the official one
and a board docket copy classifies under rule 8 (unknown).

Pure functions only (stdlib): safe to import from migrations, scripts and
the ORM flush hook in ``app.models.tariff``.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping
from urllib.parse import urlparse

OFFICIAL = "official"
THIRD_PARTY = "third_party"
UNKNOWN = "unknown"
SOURCE_TYPES = (OFFICIAL, THIRD_PARTY, UNKNOWN)

# Fetch-target preference: lower sorts first.
_RANK = {OFFICIAL: 0, UNKNOWN: 1, THIRD_PARTY: 2}

# Aggregator / comparison / non-utility domains. The extraction pipeline
# hard-blocks these in search scoring and crawling
# (``scripts.tariff_pipeline.THIRD_PARTY_DOMAINS`` is this set), and the
# classifier labels any tariff sourced from them ``third_party``.
# Government / regulatory sites are NOT included — they can be legitimate.
THIRD_PARTY_DOMAINS = frozenset({
    # Social / directory / review
    "wikipedia.org", "yelp.com", "yellowpages.com", "bbb.org",
    "facebook.com", "twitter.com", "linkedin.com", "reddit.com",
    "nextdoor.com", "glassdoor.com", "mapquest.com",
    # News / press
    "opb.org", "prnewswire.com", "businesswire.com", "koin.com",
    "azfamily.com", "ecowatch.com",
    # Rate comparison / aggregator
    "openei.org", "utility-rates.com", "costcheckusa.com",
    "findenergy.com", "energysage.com", "choosetexaspower.org",
    "electricityplans.com", "saveonenergy.com", "electricrate.com",
    "wattbuy.com", "energybot.com", "electricitylocal.com",
    "energypal.com", "electricchoice.com", "paylesspower.com",
    "texaselectricityratings.com", "powertochoose.org",
    "electricityrates.com", "utilitygenius.com", "switchwise.com",
    "energyrates.ca", "ratehub.ca", "chooseenergy.com",
    "smartenergyusa.com", "gatby.com", "poweroutage.us",
    "njenergyratings.com", "energypricing.com",
    "power2switch.com", "homeotter.com", "nyenergyratings.com",
    "texaschoicepower.com", "compareelectricity.com",
    "electricalratesgeorgia.com", "electricityrate.com",
    "gotelectric.com", "ratetruth.com",
    # Added 2026-05-05 after detecting historical contamination from
    # multi-utility aggregator pages that listed rates for several
    # utilities side-by-side (the LLM extracted them all and our
    # pipeline attributed every row to whichever utility we were
    # searching for at the time). See contamination cleanup commit.
    "comparepower.com", "ohenergyratings.com", "vaultelectricity.com",
    "maenergyratings.com", "energysavings.com", "getcurrents.com",
    "utilitiesformyhome.com", "uselectricgrid.com", "qmerit.com",
    # Retail-electric-provider promotional offer sites (not the
    # utility's own default tariff)
    "xoomenergy.com",
    # Added 2026-05-12 after Chunk 1 caught these polluting ComEd's
    # tariff list with REP promotional plans and "price to compare"
    # blurbs that aren't ComEd's own filings.
    "ilagg.com", "ilenergyratings.com", "goananta.com",
    # Solar / green energy marketing
    "solar.com", "nrgcleanpower.com", "greenridgesolar.com",
    "madison.com", "sandboxsolar.com",
    # Government DATA aggregators (not a utility's own tariff source).
    # Unlike a utility's .gov site (bpa.gov, tva.gov) or a state PSC, these
    # host generic multi-utility statistics. eia.gov in particular sent the
    # crawler into an archive of state electricity PDFs going back decades
    # (sep2011.pdf, ...062905.pdf), burning minutes + tokens for zero rates.
    # Added 2026-07-08.
    "eia.gov",
    # Rate blogs that republish utility rates (issue #28: Hydro-Québec
    # Rate D / DT / Flex D / DPC were live from callmepower.ca).
    "callmepower.ca", "callmepower.com", "energyhub.org",
})

# File/CDN/site-builder hosts shared by many unrelated organisations. A host
# match there says nothing about who published the document.
GENERIC_HOSTS = frozenset({
    "amazonaws.com", "cloudfront.net", "azureedge.net", "blob.core.windows.net",
    "googleusercontent.com", "storage.googleapis.com", "docs.google.com",
    "drive.google.com", "sites.google.com", "dropbox.com", "box.com",
    "squarespace.com", "static1.squarespace.com", "wixstatic.com",
    "website-files.com", "webflow.com", "weebly.com", "wordpress.com",
    "files.wordpress.com", "godaddysites.com", "civicplus.com",
    "revize.com", "municode.com", "ecode360.com", "constantcontact.com",
    "hubspot.net", "hubspotusercontent-na1.net", "hs-sites.com",
    "issuu.com", "scribd.com", "yumpu.com", "mailchimp.com", "list-manage.com",
    "cdn-website.com", "multiscreensite.com", "sharepoint.com",
})

# Boards that publish the rate itself for every utility in the jurisdiction.
REGULATOR_PUBLISHERS: dict[tuple[str, str], frozenset[str]] = {
    ("CA", "ON"): frozenset({"oeb.ca", "oeb.gov.on.ca"}),
    ("CA", "AB"): frozenset({"auc.ab.ca"}),
}

# Two-label public suffixes we see on utility hosts (``hydro.qc.ca``,
# ``ville.montreal.qc.ca``, ``co.uk``). Anything else: last two labels.
_TWO_LABEL_SUFFIXES = frozenset({
    "ab.ca", "bc.ca", "mb.ca", "nb.ca", "nf.ca", "nl.ca", "ns.ca", "nt.ca",
    "nu.ca", "on.ca", "pe.ca", "qc.ca", "sk.ca", "yk.ca", "yt.ca", "gc.ca",
    "co.uk", "org.uk", "gov.uk", "com.au", "gov.au", "co.nz",
})
_US_STATE_CODES = frozenset(
    "al ak az ar ca co ct de dc fl ga hi id il in ia ks ky la me md ma mi mn ms "
    "mo mt ne nv nh nj nm ny nc nd oh ok or pa ri sc sd tn tx ut vt va wa wv wi "
    "wy pr gu vi as mp".split()
)


@dataclass(frozen=True)
class SourceClassification:
    source_type: str
    reason: str


@dataclass(frozen=True)
class UtilitySourceContext:
    """What the classifier knows about one utility's official web presence."""

    website_url: str | None = None
    official_urls: tuple[str, ...] = ()
    country: str | None = None
    state_province: str | None = None


def normalize_host(url: str | None) -> str:
    """Lower-cased host without ``www.`` / port; '' when unparseable."""
    if not url or not isinstance(url, str):
        return ""
    raw = url.strip()
    if not raw:
        return ""
    if "://" not in raw:
        raw = f"https://{raw}"
    try:
        host = (urlparse(raw).hostname or "").lower().strip(".")
    except ValueError:
        return ""
    if host.startswith("www."):
        host = host[4:]
    return host if "." in host else ""


def registrable_domain(host: str) -> str:
    """Organisation-level domain: ``rates.hydroquebec.com`` → ``hydroquebec.com``.

    Heuristic (no public-suffix download): two labels, three under a known
    two-label suffix (``qc.ca``), four under US locality domains
    (``ci.anaheim.ca.us``).
    """
    labels = [p for p in (host or "").split(".") if p]
    if len(labels) <= 2:
        return ".".join(labels)
    if labels[-1] == "us" and labels[-2] in _US_STATE_CODES:
        return ".".join(labels[-4:])
    if ".".join(labels[-2:]) in _TWO_LABEL_SUFFIXES:
        return ".".join(labels[-3:])
    return ".".join(labels[-2:])


def _host_in(host: str, domains: Iterable[str]) -> bool:
    return any(host == d or host.endswith(f".{d}") for d in domains)


def is_third_party_host(url_or_host: str | None) -> bool:
    host = normalize_host(url_or_host)
    return bool(host) and _host_in(host, THIRD_PARTY_DOMAINS)


def _is_generic_host(host: str) -> bool:
    return _host_in(host, GENERIC_HOSTS)


def _is_government_host(host: str) -> bool:
    labels = host.split(".")
    if labels[-1] in ("gov", "mil") or host.endswith(".gc.ca"):
        return True
    if len(labels) >= 3 and labels[-1] == "ca" and labels[-3] == "gov":
        return True  # gov.bc.ca, gov.on.ca
    if labels[-1] == "us" and "state" in labels[:-2]:
        return True  # puc.state.nh.us
    return False


def configured_urls(tariff_page_urls: Any, rate_page_url_override: str | None = None) -> tuple[str, ...]:
    """Flatten ``utilities.tariff_page_urls`` (list or dict JSONB) + override."""
    out: list[str] = []
    if rate_page_url_override:
        out.append(rate_page_url_override)
    if isinstance(tariff_page_urls, Mapping):
        values: Iterable[Any] = tariff_page_urls.values()
    elif isinstance(tariff_page_urls, (list, tuple)):
        values = tariff_page_urls
    elif isinstance(tariff_page_urls, str):
        values = [tariff_page_urls]
    else:
        values = []
    for v in values:
        if isinstance(v, str) and v.strip():
            out.append(v.strip())
    return tuple(dict.fromkeys(out))


def context_from_utility(utility: Any) -> UtilitySourceContext:
    """Build a context from a Utility row, a mapping, or a SimpleNamespace."""
    def get(key: str) -> Any:
        if isinstance(utility, Mapping):
            return utility.get(key)
        return getattr(utility, key, None)

    country = get("country")
    return UtilitySourceContext(
        website_url=get("website_url"),
        official_urls=configured_urls(get("tariff_page_urls"), get("rate_page_url_override")),
        country=getattr(country, "value", country),
        state_province=get("state_province"),
    )


def _official_hosts(ctx: UtilitySourceContext) -> tuple[str, set[str]]:
    """(website registrable domain, registrable domains of configured URLs)."""
    site = normalize_host(ctx.website_url)
    site_dom = "" if not site or is_third_party_host(site) or _is_generic_host(site) else registrable_domain(site)
    configured: set[str] = set()
    for url in ctx.official_urls:
        h = normalize_host(url)
        if h and not is_third_party_host(h) and not _is_generic_host(h):
            configured.add(registrable_domain(h))
    return site_dom, configured


def _url_key(url: str) -> str:
    p = urlparse(url if "://" in url else f"https://{url}")
    return f"{normalize_host(url)}{p.path.rstrip('/')}".lower()


def classify_source(source_url: str | None, ctx: UtilitySourceContext | None = None) -> SourceClassification:
    ctx = ctx or UtilitySourceContext()
    host = normalize_host(source_url)
    if not host:
        return SourceClassification(UNKNOWN, "no_url")
    if _host_in(host, THIRD_PARTY_DOMAINS):
        return SourceClassification(THIRD_PARTY, "aggregator_blocklist")

    generic = _is_generic_host(host)
    site_dom, configured = _official_hosts(ctx)
    if not generic:
        dom = registrable_domain(host)
        if site_dom and dom == site_dom:
            return SourceClassification(OFFICIAL, "domain_match")
        if dom in configured:
            return SourceClassification(OFFICIAL, "configured_host")
    else:
        key = _url_key(source_url or "")
        for url in (*ctx.official_urls, ctx.website_url or ""):
            ck = _url_key(url) if url else ""
            if "/" not in ck or is_third_party_host(url):
                continue  # a bare shared host says nothing about the publisher
            if key == ck or key.startswith(ck + "/"):
                return SourceClassification(OFFICIAL, "configured_url")

    publishers = REGULATOR_PUBLISHERS.get(
        (str(ctx.country or "").upper(), str(ctx.state_province or "").upper()), frozenset()
    )
    if _host_in(host, publishers):
        return SourceClassification(OFFICIAL, "regulator_publisher")
    if generic:
        return SourceClassification(UNKNOWN, "generic_host")
    if _is_government_host(host):
        return SourceClassification(UNKNOWN, "government_host")
    if site_dom or configured:
        return SourceClassification(THIRD_PARTY, "domain_mismatch")
    return SourceClassification(UNKNOWN, "no_official_host")


def source_rank(source_type: str | None) -> int:
    return _RANK.get(source_type or UNKNOWN, _RANK[UNKNOWN])


def has_official_host(ctx: UtilitySourceContext) -> bool:
    site_dom, configured = _official_hosts(ctx)
    return bool(site_dom or configured)


def rank_urls(urls: Iterable[str], ctx: UtilitySourceContext) -> list[str]:
    """De-duplicated URLs, official first, then unknown, third-party last.

    Stable within a class, so the caller's own ordering (search score,
    configured order) still breaks ties. Nothing is dropped: a third-party
    URL that is the only candidate is still returned.
    """
    seen: list[str] = []
    for u in urls:
        if u and u not in seen:
            seen.append(u)
    return sorted(seen, key=lambda u: source_rank(classify_source(u, ctx).source_type))


# ---------------------------------------------------------------------------
# DB helpers (sync Connection / Session)
# ---------------------------------------------------------------------------

_UTILITY_CTX_SQL = (
    "SELECT website_url, tariff_page_urls, rate_page_url_override, "
    "country::text AS country, state_province FROM utilities WHERE id = :id"
)


def load_utility_context(conn, utility_id: int | None) -> UtilitySourceContext:
    """Context for one utility via a sync Connection or Session."""
    from sqlalchemy import text

    if utility_id is None:
        return UtilitySourceContext()
    row = conn.execute(text(_UTILITY_CTX_SQL), {"id": utility_id}).mappings().first()
    return context_from_utility(dict(row)) if row else UtilitySourceContext()


def reclassify_tariffs(conn, *, utility_ids: Iterable[int] | None = None, dry_run: bool = False) -> dict:
    """Re-run the classifier over stored tariffs (live and superseded).

    Only the two classification columns are written (no rate content), in
    one ``UPDATE ... WHERE id = ANY`` per class. Returns
    ``{"counts": {(type, reason): n}, "changed": {(old, new): n}}``.
    """
    from collections import Counter, defaultdict

    from sqlalchemy import text

    ids_filter = sorted(set(utility_ids)) if utility_ids is not None else None
    u_sql = (
        "SELECT id, website_url, tariff_page_urls, rate_page_url_override, "
        "country::text AS country, state_province FROM utilities"
    )
    t_sql = "SELECT id, utility_id, source_url, source_type, source_type_reason FROM tariffs"
    params: dict = {}
    if ids_filter is not None:
        u_sql += " WHERE id = ANY(:uids)"
        t_sql += " WHERE utility_id = ANY(:uids)"
        params["uids"] = ids_filter
    contexts = {
        r["id"]: context_from_utility(dict(r))
        for r in conn.execute(text(u_sql), params).mappings()
    }
    counts: Counter = Counter()
    changed: Counter = Counter()
    groups: dict[tuple[str, str], list[int]] = defaultdict(list)
    for tid, uid, url, old_type, old_reason in conn.execute(text(t_sql), params):
        res = classify_source(url, contexts.get(uid))
        counts[(res.source_type, res.reason)] += 1
        if (old_type, old_reason) != (res.source_type, res.reason):
            groups[(res.source_type, res.reason)].append(tid)
            if old_type != res.source_type:
                changed[(old_type, res.source_type)] += 1
    if not dry_run:
        stmt = text(
            "UPDATE tariffs SET source_type = :st, source_type_reason = :reason "
            "WHERE id = ANY(:ids)"
        )
        for (st, reason), ids in groups.items():
            for i in range(0, len(ids), 5000):
                conn.execute(stmt, {"st": st, "reason": reason, "ids": ids[i:i + 5000]})
    return {"counts": dict(counts), "changed": dict(changed)}


def stamp_source_type(conn, target, *, force: bool = False) -> None:
    """Set ``source_type`` / ``source_type_reason`` on a Tariff before flush.

    Recomputes when forced, when unset, or when ``source_url`` changed in
    this flush without the caller also setting ``source_type``.
    """
    from sqlalchemy import inspect as sa_inspect

    state = sa_inspect(target)
    if not force and target.source_type:
        if state.pending or state.transient:
            return
        attrs = state.attrs
        if not attrs.source_url.history.has_changes() or attrs.source_type.history.has_changes():
            return
    ctx = load_utility_context(conn, target.utility_id)
    result = classify_source(target.source_url, ctx)
    target.source_type = result.source_type
    target.source_type_reason = result.reason
