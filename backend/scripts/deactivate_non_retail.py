"""
Deactivate non-retail entities: energy marketers, REPs, wholesale/generation
co-ops, transmission entities, and other non-territory-based utilities.

These entities don't have their own rate pages and shouldn't be in the
active tariff database. Their tariff data typically comes from 3rd party
aggregator sites (energybot.com, etc.) and is unreliable.

Usage:
    python -m scripts.deactivate_non_retail --dry-run    # preview only
    python -m scripts.deactivate_non_retail               # actually deactivate
"""

import argparse
import logging

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.db.session import get_sync_engine
from app.models import Tariff, Utility

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("deactivate_non_retail")

# Utility IDs to deactivate, organized by reason.
# Curated manually from database analysis of source URLs.

ENERGY_MARKETERS_REPS = [
    # Texas REPs (Retail Electric Providers) — sell via comparison sites
    1391,  # Alliance Power Company, LLC.
    1695,  # Ammper Power LLC
    1690,  # Arrow Energy TX LLC
    1584,  # Atlantic Energy LLC
    32,    # BP Energy Company
    1659,  # BP Energy Retail LLC
    1671,  # Branch Energy Texas LLC
    1692,  # CPV Retail Energy LP
    1457,  # Citigroup Energy Inc
    1668,  # Declaration Energy LLC
    1541,  # Dynegy Energy Services, LLC
    1151,  # ENGIE Energy Marketing NA, Inc
    1579,  # Electranet Power LLC
    1502,  # Everyday Energy LLC
    1482,  # FairPoint Energy LLC
    1437,  # Frontier Utilities, LLC
    1677,  # Good Charlie & Co., LLC
    1432,  # Macquarie Energy LLC
    1519,  # Mega Energy of New Hampshire, LLC
    1453,  # Smart Prepaid Electric
    1568,  # Spruce Finance
    1464,  # Summer Energy LLC
    1630,  # Summer Energy Midwest, LLC
    1601,  # Summer Energy Northeast, LLC
    1546,  # SunPower Capital, LLC
    1648,  # Tenaska Power Management, LLC
    1603,  # Total Gas & Power North America Inc
    1465,  # Value Based Brands LLC
    1649,  # Provision Power & Gas, LLC
    426,   # NextEra Energy Services, LLC
    1501,  # North American Power and Gas, LLC
    1477,  # Public Power & Utility of Maryland, LLC
    1479,  # Public Power LLC (CT)
    1478,  # Public Power LLC (PA)
    # NJ / NY / CT / PA / IL energy marketers
    1521,  # Astral Energy LLC (NJ)
    1673,  # Lanyard Power Marketing, LLC (NJ)
    1544,  # Mesquite Generation Holdings LLC (NJ)
    87,    # Messer Energy Services, Inc. (NJ)
    1420,  # PSEG Energy Resources and Trade (NJ)
    1560,  # Residents Energy, LLC (NJ)
    1697,  # ABN Energy, LLC (NY)
    1635,  # Agressive Energy LLC (NY)
    339,   # Empire Natural Gas Corporation (NY)
    1446,  # JP Morgan (NY)
    1442,  # Palmco Power NJ, LLC (NY)
    1623,  # Pure Energy USA, LLC (NY)
    1500,  # Respond Power LLC (NY)
    1708,  # TerraForm Renewable Energy Services, LLC (NY)
    1696,  # Citadel Energy Marketing LLC (CT)
    1299,  # HQ Energy Services (U.S.), Inc. (CT)
    1515,  # Realgy, LLC (CT)
    19,    # AGC Division of APGI Inc (PA)
    1402,  # APN Starfirst, L.P. (PA)
    1107,  # Direct Energy Business (PA)
    1434,  # Energy Plus Holdings LLC (PA)
    1389,  # The Energy Coop (PA)
    1534,  # Constellation Solar Holding, LLC (MD)
    1466,  # Wolverine Alternative Investments, LLC (IL)
    1401,  # Texas Retail Energy, LLC (AR)
    1593,  # Current Power & Gas Inc. (AZ)
    1688,  # Pay Less Energy LLC (FL)
]

WHOLESALE_TRANSMISSION_GENERATION = [
    421,   # Georgia Transmission Corp
    744,   # Municipal Electric Authority (GA)
    1378,  # American Mun Power-Ohio, Inc
    841,   # Ohio Valley Electric Corp
    801,   # North Carolina El Member Corp
    245,   # Connecticut Mun Elec Engy Coop
    1384,  # Northern California Power Agny
    120,   # Brazos Electric Power Coop Inc (TX generation co-op)
    1344,  # East Texas Electric Coop, Inc (G&T co-op)
    1006,  # San Miguel Electric Coop, Inc (generation co-op)
    1150,  # Toledo Bend Project Joint Oper (generation)
    437,   # Greenbelt Electric Coop, Inc (TX — actually a distribution co-op, but only 3P data)
    697,   # System Energy Resources, Inc (MS — generation subsidiary)
    1322,  # DTE Energy Trading, Inc (MI — trading arm)
    1441,  # UP Power Marketing, LLC (MI — marketing arm)
    1435,  # TransCanada Energy Marketing ULC (CA — marketing)
    1575,  # OE Holdings (NC — holding company)
]

NON_UTILITY_ENTITIES = [
    1190,  # University of Illinois
    1476,  # Texas A&M, Utilities & Energy Services
    1570,  # The Regents of the Univ. of California
    991,   # Safe Harbor Water Power Corp (PA — hydroelectric plant, not retail)
]

# Additional entities identified as non-retail from the 100-utility test
ADDITIONAL_DEACTIVATE = [
    229,   # CMS Energy Resource Management (MI — marketing subsidiary)
    1567,  # Texpo Power, L.P. (TX — energy marketer)
    1647,  # Varsity Energy (TX — energy marketer)
    1607,  # Philadelphia Authority for Industrial Development (PA — not a utility)
    1380,  # Northern Municipal Power Agency (MN — wholesale)
    792,   # North Carolina Mun Power Agny #1 (NC — wholesale/generation)
]

ALL_IDS = ENERGY_MARKETERS_REPS + WHOLESALE_TRANSMISSION_GENERATION + NON_UTILITY_ENTITIES + ADDITIONAL_DEACTIVATE


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    engine = get_sync_engine()

    with Session(engine) as session:
        utilities = session.execute(
            select(Utility)
            .where(Utility.id.in_(ALL_IDS))
            .where(Utility.is_active.is_(True))
        ).scalars().all()

        if not utilities:
            log.info("No active utilities to deactivate.")
            return

        log.info(f"{'DRY RUN: ' if args.dry_run else ''}Deactivating {len(utilities)} non-retail entities:\n")

        by_reason = {
            "Energy Marketer / REP": ENERGY_MARKETERS_REPS,
            "Wholesale / Transmission / Generation": WHOLESALE_TRANSMISSION_GENERATION,
            "Non-Utility Entity": NON_UTILITY_ENTITIES,
            "Additional (from test analysis)": ADDITIONAL_DEACTIVATE,
        }

        total_tariffs = 0
        for reason, ids in by_reason.items():
            group = [u for u in utilities if u.id in ids]
            if not group:
                continue
            log.info(f"  {reason} ({len(group)}):")
            for u in sorted(group, key=lambda x: (x.state_province, x.name)):
                tariff_count = session.execute(
                    select(Tariff.id).where(Tariff.utility_id == u.id)
                ).scalars().all()
                total_tariffs += len(tariff_count)
                log.info(f"    {u.id:5d}  {u.name:<50s} ({u.state_province})  {len(tariff_count)} tariffs")

        log.info(f"\n  Total: {len(utilities)} utilities, {total_tariffs} tariffs to be removed\n")

        if args.dry_run:
            log.info("DRY RUN — no changes made. Run without --dry-run to execute.")
            return

        # Deactivate utilities
        deactivated = 0
        for u in utilities:
            u.is_active = False
            deactivated += 1

        # Delete their tariffs (they're from 3rd party sites, not reliable)
        from sqlalchemy import delete as sa_delete
        from app.models.tariff import RateComponent

        tariff_ids = session.execute(
            select(Tariff.id).where(Tariff.utility_id.in_(ALL_IDS))
        ).scalars().all()

        if tariff_ids:
            session.execute(
                sa_delete(RateComponent).where(RateComponent.tariff_id.in_(tariff_ids))
            )
            session.execute(
                sa_delete(Tariff).where(Tariff.id.in_(tariff_ids))
            )

        session.commit()
        log.info(f"Done: deactivated {deactivated} utilities, deleted {len(tariff_ids)} tariffs.")


if __name__ == "__main__":
    main()
