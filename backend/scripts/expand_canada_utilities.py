"""
Expand Canadian utility coverage in the database.

Adds missing utilities across all provinces based on authoritative
regulatory lists. Only adds utilities that don't already exist (matched
by name similarity to avoid duplicates).

Usage:
    python -m scripts.expand_canada_utilities --dry-run   # preview what would be added
    python -m scripts.expand_canada_utilities              # add to database
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("expand_ca")


@dataclass
class UtilityEntry:
    name: str
    province: str
    utility_type: str  # maps to UtilityType enum
    website_url: str | None = None


# ---------------------------------------------------------------------------
# Authoritative utility lists per province
# Sources: OEB, AUC/UCA, BCUC, Régie de l'énergie, provincial regulators
# ---------------------------------------------------------------------------

ONTARIO_LDCS: list[UtilityEntry] = [
    UtilityEntry("Algoma Power", "ON", "municipal", "https://www.algomapower.com"),
    UtilityEntry("Atikokan Hydro", "ON", "municipal", "https://www.atikokanhydro.com"),
    UtilityEntry("Bluewater Power Distribution", "ON", "municipal", "https://www.bluewaterpower.com"),
    UtilityEntry("Canadian Niagara Power", "ON", "investor_owned", "https://www.cnpower.com"),
    UtilityEntry("Centre Wellington Hydro", "ON", "municipal", "https://www.cwhydro.ca"),
    UtilityEntry("Chapleau Public Utilities", "ON", "municipal", None),
    UtilityEntry("Cooperative Hydro Embrun", "ON", "cooperative", None),
    UtilityEntry("Cornwall Street Railway Light & Power", "ON", "municipal", "https://www.cornwallelectric.com"),
    UtilityEntry("E.L.K. Energy", "ON", "municipal", "https://www.elkenergy.com"),
    UtilityEntry("Entegrus Powerlines", "ON", "municipal", "https://www.entegrus.com"),
    UtilityEntry("ENWIN Utilities", "ON", "municipal", "https://www.enwin.com"),
    UtilityEntry("EPCOR Electricity Distribution Ontario", "ON", "investor_owned", "https://www.epcor.com/power-natural-gas/electricity-distribution/"),
    UtilityEntry("PUC Distribution (Sault Ste. Marie)", "ON", "municipal", "https://www.ssmpuc.com"),
    UtilityEntry("ERTH Power Corporation", "ON", "municipal", "https://www.erthcorp.com"),
    UtilityEntry("Espanola Regional Hydro", "ON", "municipal", "https://www.espanolahydro.com"),
    UtilityEntry("Essex Powerlines", "ON", "municipal", "https://www.essexpowerlines.com"),
    UtilityEntry("Festival Hydro", "ON", "municipal", "https://www.festivalhydro.com"),
    UtilityEntry("Fort Frances Power", "ON", "municipal", None),
    UtilityEntry("GrandBridge Energy", "ON", "municipal", "https://www.grandbridge.ca"),
    UtilityEntry("Greater Sudbury Hydro", "ON", "municipal", "https://www.greatersudburyhydro.com"),
    UtilityEntry("Grimsby Power", "ON", "municipal", "https://www.grimsbypower.com"),
    UtilityEntry("Guelph Hydro Electric Systems", "ON", "municipal", "https://www.guelphhydro.com"),
    UtilityEntry("Hearst Power Distribution", "ON", "municipal", "https://www.hearstpower.ca"),
    UtilityEntry("Hydro 2000", "ON", "municipal", None),
    UtilityEntry("Hydro Hawkesbury", "ON", "municipal", "https://www.hydrohawkesbury.com"),
    UtilityEntry("Hydro One Remote Communities", "ON", "state", "https://www.hydroone.com"),
    UtilityEntry("InnPower Corporation", "ON", "municipal", "https://www.innpower.ca"),
    UtilityEntry("Kingston Hydro", "ON", "municipal", "https://www.kingstonhydro.com"),
    UtilityEntry("Lakefront Utilities", "ON", "municipal", "https://www.lakefrontutilities.com"),
    UtilityEntry("Lakeland Power Distribution", "ON", "municipal", "https://www.lakelandpower.on.ca"),
    UtilityEntry("Newmarket-Tay Power Distribution", "ON", "municipal", "https://www.nmhydro.ca"),
    UtilityEntry("Niagara-on-the-Lake Hydro", "ON", "municipal", "https://www.notlhydro.com"),
    UtilityEntry("North Bay Hydro Distribution", "ON", "municipal", "https://www.northbayhydro.com"),
    UtilityEntry("Northern Ontario Wires", "ON", "municipal", "https://www.northernontariowires.com"),
    UtilityEntry("Orangeville Hydro", "ON", "municipal", "https://www.orangevillehydro.on.ca"),
    UtilityEntry("Ottawa River Power", "ON", "municipal", "https://www.ottawariverpower.com"),
    UtilityEntry("Renfrew Hydro", "ON", "municipal", None),
    UtilityEntry("Rideau St. Lawrence Distribution", "ON", "municipal", None),
    UtilityEntry("Sioux Lookout Hydro", "ON", "municipal", "https://www.siouxlookouthydro.com"),
    UtilityEntry("Synergy North Corporation", "ON", "municipal", "https://www.synergynorth.ca"),
    UtilityEntry("Tillsonburg Hydro", "ON", "municipal", "https://www.tillsonburghydro.com"),
    UtilityEntry("Wasaga Distribution", "ON", "municipal", "https://www.wasagadistribution.com"),
    UtilityEntry("Welland Hydro-Electric System", "ON", "municipal", "https://www.wellandhydro.com"),
    UtilityEntry("Wellington North Power", "ON", "municipal", "https://www.wellingtonnorthpower.com"),
    UtilityEntry("Westario Power", "ON", "municipal", "https://www.westario.com"),
]

ALBERTA_EXTRAS: list[UtilityEntry] = [
    UtilityEntry("FortisAlberta", "AB", "investor_owned", "https://www.fortisalberta.com"),
    UtilityEntry("City of Lethbridge Electric", "AB", "municipal", "https://www.lethbridge.ca"),
]

BC_EXTRAS: list[UtilityEntry] = [
    UtilityEntry("Nelson Hydro", "BC", "municipal", "https://www.nelson.ca/167/Nelson-Hydro"),
    UtilityEntry("New Westminster Electrical Utility", "BC", "municipal", "https://www.newwestcity.ca/electrical"),
    UtilityEntry("City of Penticton Electric Utility", "BC", "municipal", "https://www.penticton.ca"),
    UtilityEntry("City of Grand Forks Electric", "BC", "municipal", "https://www.grandforks.ca"),
    UtilityEntry("District of Summerland Electric", "BC", "municipal", "https://www.summerland.ca"),
]

QUEBEC_EXTRAS: list[UtilityEntry] = [
    UtilityEntry("Hydro-Sherbrooke", "QC", "municipal", "https://www.sherbrooke.ca"),
    UtilityEntry("Hydro Magog", "QC", "municipal", "https://www.ville.magog.qc.ca"),
    UtilityEntry("Hydro Joliette", "QC", "municipal", "https://www.joliette.ca"),
    UtilityEntry("Hydro Coaticook", "QC", "municipal", "https://www.coaticook.ca"),
    UtilityEntry("Hydro Alma", "QC", "municipal", "https://www.ville.alma.qc.ca"),
    UtilityEntry("Hydro Westmount", "QC", "municipal", "https://www.westmount.org"),
    UtilityEntry("Baie-Comeau Électricité", "QC", "municipal", "https://www.ville.baie-comeau.qc.ca"),
    UtilityEntry("Ville d'Amos Électricité", "QC", "municipal", "https://www.ville.amos.qc.ca"),
    UtilityEntry("Ville de Saguenay Électricité", "QC", "municipal", "https://www.ville.saguenay.ca"),
]

SASK_EXTRAS: list[UtilityEntry] = [
    UtilityEntry("Saskatoon Light & Power", "SK", "municipal", "https://www.saskatoon.ca/services-residents/power-water/saskatoon-light-power"),
    UtilityEntry("Swift Current Light & Power", "SK", "municipal", "https://www.swiftcurrent.ca"),
]

NB_EXTRAS: list[UtilityEntry] = [
    UtilityEntry("Saint John Energy", "NB", "municipal", "https://www.sjenergy.com"),
    UtilityEntry("Edmundston Energy", "NB", "municipal", "https://www.edmundston.ca"),
    UtilityEntry("Perth-Andover Electric Light Commission", "NB", "municipal", None),
]

NS_EXTRAS: list[UtilityEntry] = [
    UtilityEntry("Antigonish Electric Utility", "NS", "municipal", "https://www.townofantigonish.ca"),
    UtilityEntry("Berwick Electric Light Commission", "NS", "municipal", "https://www.berwick.ca"),
    UtilityEntry("Mahone Bay Electric Utility", "NS", "municipal", "https://www.townofmahonebay.ca"),
    UtilityEntry("Lunenburg Electric Utility", "NS", "municipal", "https://www.explorelunenburg.ca"),
    UtilityEntry("Riverport Electric Light Commission", "NS", "municipal", None),
    UtilityEntry("Canso Electric Light Commission", "NS", "municipal", None),
]

PE_EXTRAS: list[UtilityEntry] = [
    UtilityEntry("Summerside Electric", "PE", "municipal", "https://www.summerside.ca"),
]

NT_EXTRAS: list[UtilityEntry] = [
    UtilityEntry("Northland Utilities (NWT)", "NT", "investor_owned", "https://www.northlandutilities.com"),
]

ALL_NEW_UTILITIES = (
    ONTARIO_LDCS + ALBERTA_EXTRAS + BC_EXTRAS + QUEBEC_EXTRAS +
    SASK_EXTRAS + NB_EXTRAS + NS_EXTRAS + PE_EXTRAS + NT_EXTRAS
)


def _normalize(name: str) -> str:
    """Normalize utility name for fuzzy matching."""
    n = name.lower().strip()
    for remove in ["inc.", "inc", "ltd.", "ltd", "corporation", "corp.", "corp",
                    "limited", "distribution", "electric system", "networks",
                    "utilities", "utility", "hydro-electric", "electric"]:
        n = n.replace(remove, "")
    n = " ".join(n.split())
    return n


def find_missing(dry_run: bool = True) -> list[UtilityEntry]:
    from app.db.session import get_sync_engine
    from app.models import Utility

    engine = get_sync_engine()
    with Session(engine) as s:
        existing = s.execute(
            select(Utility).where(Utility.country == "CA")
        ).scalars().all()

    existing_normalized = {_normalize(u.name): u.name for u in existing}
    existing_names_lower = {u.name.lower() for u in existing}

    to_add: list[UtilityEntry] = []
    skipped: list[tuple[str, str]] = []

    for entry in ALL_NEW_UTILITIES:
        if entry.name.lower() in existing_names_lower:
            skipped.append((entry.name, "exact match"))
            continue

        norm = _normalize(entry.name)
        matched = False
        for existing_norm, existing_full in existing_normalized.items():
            # Require both names to be reasonably long for substring matching
            # to avoid false matches like "PUC" matching "Oshawa PUC"
            if len(norm) > 6 and len(existing_norm) > 6:
                if norm == existing_norm:
                    skipped.append((entry.name, f"fuzzy match -> {existing_full}"))
                    matched = True
                    break
        if matched:
            continue

        to_add.append(entry)

    log.info(f"Existing Canadian utilities: {len(existing)}")
    log.info(f"Candidates checked: {len(ALL_NEW_UTILITIES)}")
    log.info(f"Already exist (skipped): {len(skipped)}")
    log.info(f"New to add: {len(to_add)}")

    if skipped:
        log.info("")
        log.info("=== SKIPPED (already in DB) ===")
        for name, reason in skipped:
            log.info(f"  {name} ({reason})")

    log.info("")
    log.info("=== TO ADD ===")
    by_prov: dict[str, list[UtilityEntry]] = {}
    for e in to_add:
        by_prov.setdefault(e.province, []).append(e)
    for prov in sorted(by_prov.keys()):
        log.info(f"  {prov}:")
        for e in by_prov[prov]:
            log.info(f"    {e.name} ({e.utility_type}) {e.website_url or '(no website)'}")

    if not dry_run and to_add:
        from app.models.utility import Utility, UtilityType, Country
        added = 0
        with Session(engine) as s:
            for entry in to_add:
                u = Utility(
                    name=entry.name,
                    country=Country.CA,
                    state_province=entry.province,
                    utility_type=UtilityType(entry.utility_type),
                    website_url=entry.website_url,
                    is_active=True,
                )
                s.add(u)
                added += 1
            s.commit()
        log.info(f"\nAdded {added} new utilities to database.")
    elif dry_run and to_add:
        log.info(f"\nDRY RUN: Would add {len(to_add)} utilities. Run without --dry-run to commit.")

    return to_add


def main():
    parser = argparse.ArgumentParser(description="Expand Canadian utility coverage")
    parser.add_argument("--dry-run", action="store_true", help="Preview only, don't write to DB")
    args = parser.parse_args()
    find_missing(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
