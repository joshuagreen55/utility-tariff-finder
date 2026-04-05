"""
Seed Canadian utility data.

Canada has ~80 distribution utilities, mostly provincial monopolies.
This script inserts a curated list of major Canadian electric utilities.

Usage:
    python -m scripts.seed_canada
"""

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.session import get_sync_engine
from app.db.base import Base
from app.models import Utility, UtilityType, Country


CANADIAN_UTILITIES = [
    # British Columbia
    {"name": "BC Hydro", "state_province": "BC", "utility_type": UtilityType.IOU,
     "website_url": "https://www.bchydro.com",
     "tariff_page_urls": ["https://www.bchydro.com/accounts-billing/rates-energy-use/electricity-rates.html"]},
    {"name": "FortisBC", "state_province": "BC", "utility_type": UtilityType.IOU,
     "website_url": "https://www.fortisbc.com",
     "tariff_page_urls": ["https://www.fortisbc.com/accounts-billing/billing-rates/electricity-rates"]},

    # Alberta
    {"name": "ENMAX Energy", "state_province": "AB", "utility_type": UtilityType.RETAIL_MARKETER,
     "website_url": "https://www.enmax.com",
     "tariff_page_urls": ["https://www.enmax.com/home/electricity-and-natural-gas/rates"]},
    {"name": "EPCOR", "state_province": "AB", "utility_type": UtilityType.IOU,
     "website_url": "https://www.epcor.com",
     "tariff_page_urls": ["https://www.epcor.com/products-services/power/rates-and-billing"]},
    {"name": "ATCO Electric", "state_province": "AB", "utility_type": UtilityType.IOU,
     "website_url": "https://www.atco.com",
     "tariff_page_urls": ["https://www.atco.com/en-ca/for-home/electricity/rates.html"]},
    {"name": "Direct Energy (Alberta)", "state_province": "AB", "utility_type": UtilityType.RETAIL_MARKETER,
     "website_url": "https://www.directenergy.ca"},

    # Saskatchewan
    {"name": "SaskPower", "state_province": "SK", "utility_type": UtilityType.IOU,
     "website_url": "https://www.saskpower.com",
     "tariff_page_urls": ["https://www.saskpower.com/Accounts-and-Services/Rates/Power-Rates"]},

    # Manitoba
    {"name": "Manitoba Hydro", "state_province": "MB", "utility_type": UtilityType.IOU,
     "website_url": "https://www.hydro.mb.ca",
     "tariff_page_urls": ["https://www.hydro.mb.ca/accounts-and-billing/rates/"]},

    # Ontario - Major LDCs
    {"name": "Hydro One", "state_province": "ON", "utility_type": UtilityType.IOU,
     "website_url": "https://www.hydroone.com",
     "tariff_page_urls": ["https://www.hydroone.com/rates-and-billing/rates-and-charges"]},
    {"name": "Toronto Hydro", "state_province": "ON", "utility_type": UtilityType.MUNICIPAL,
     "website_url": "https://www.torontohydro.com",
     "tariff_page_urls": ["https://www.torontohydro.com/accounts-services/rates"]},
    {"name": "Alectra Utilities", "state_province": "ON", "utility_type": UtilityType.MUNICIPAL,
     "website_url": "https://www.alectrautilities.com",
     "tariff_page_urls": ["https://www.alectrautilities.com/rates-and-billing"]},
    {"name": "Ottawa Hydro (Hydro Ottawa)", "state_province": "ON", "utility_type": UtilityType.MUNICIPAL,
     "website_url": "https://hydroottawa.com",
     "tariff_page_urls": ["https://hydroottawa.com/accounts-services/accounts/rates-conditions"]},
    {"name": "London Hydro", "state_province": "ON", "utility_type": UtilityType.MUNICIPAL,
     "website_url": "https://www.londonhydro.com"},
    {"name": "Elexicon Energy", "state_province": "ON", "utility_type": UtilityType.MUNICIPAL,
     "website_url": "https://www.elexiconenergy.com"},
    {"name": "Oakville Hydro", "state_province": "ON", "utility_type": UtilityType.MUNICIPAL,
     "website_url": "https://www.oakvillehydro.com"},
    {"name": "Burlington Hydro", "state_province": "ON", "utility_type": UtilityType.MUNICIPAL,
     "website_url": "https://www.burlingtonhydro.com"},
    {"name": "Kitchener-Wilmot Hydro", "state_province": "ON", "utility_type": UtilityType.MUNICIPAL,
     "website_url": "https://www.kwhydro.ca"},
    {"name": "Waterloo North Hydro", "state_province": "ON", "utility_type": UtilityType.MUNICIPAL,
     "website_url": "https://www.wnhydro.com"},
    {"name": "Oshawa PUC Networks", "state_province": "ON", "utility_type": UtilityType.MUNICIPAL,
     "website_url": "https://www.opuc.on.ca"},
    {"name": "Niagara Peninsula Energy", "state_province": "ON", "utility_type": UtilityType.MUNICIPAL,
     "website_url": "https://npei.ca",
     "tariff_page_urls": ["https://npei.ca/info-resources/regulatory-information/policies-and-tariffs"]},
    {"name": "Halton Hills Hydro", "state_province": "ON", "utility_type": UtilityType.MUNICIPAL,
     "website_url": "https://www.haltonhillshydro.com"},
    {"name": "Milton Hydro", "state_province": "ON", "utility_type": UtilityType.MUNICIPAL,
     "website_url": "https://www.miltonhydro.com"},
    {"name": "Enersource Hydro Mississauga", "state_province": "ON", "utility_type": UtilityType.MUNICIPAL,
     "website_url": "https://www.alectrautilities.com"},  # merged into Alectra

    # Quebec
    {"name": "Hydro-Québec", "state_province": "QC", "utility_type": UtilityType.IOU,
     "website_url": "https://www.hydroquebec.com",
     "tariff_page_urls": ["https://www.hydroquebec.com/residential/customer-space/rates/"]},

    # New Brunswick
    {"name": "NB Power", "state_province": "NB", "utility_type": UtilityType.IOU,
     "website_url": "https://www.nbpower.com",
     "tariff_page_urls": ["https://www.nbpower.com/en/products-services/electricity/rates"]},

    # Nova Scotia
    {"name": "Nova Scotia Power", "state_province": "NS", "utility_type": UtilityType.IOU,
     "website_url": "https://www.nspower.ca",
     "tariff_page_urls": ["https://www.nspower.ca/about-us/electricity/rates-tariffs"]},

    # Prince Edward Island
    {"name": "Maritime Electric", "state_province": "PE", "utility_type": UtilityType.IOU,
     "website_url": "https://www.maritimeelectric.com",
     "tariff_page_urls": ["https://www.maritimeelectric.com/about-us/regulatory/rates-tariffs/"]},

    # Newfoundland and Labrador
    {"name": "Newfoundland Power", "state_province": "NL", "utility_type": UtilityType.IOU,
     "website_url": "https://www.newfoundlandpower.com",
     "tariff_page_urls": ["https://www.newfoundlandpower.com/CustomerService/ElectricityRates"]},
    {"name": "Newfoundland and Labrador Hydro", "state_province": "NL", "utility_type": UtilityType.IOU,
     "website_url": "https://nlhydro.com"},

    # Territories
    {"name": "Yukon Energy", "state_province": "YT", "utility_type": UtilityType.IOU,
     "website_url": "https://yukonenergy.ca"},
    {"name": "ATCO Electric Yukon", "state_province": "YT", "utility_type": UtilityType.IOU,
     "website_url": "https://www.atcoelectricyukon.com"},
    {"name": "Northwest Territories Power Corporation", "state_province": "NT", "utility_type": UtilityType.IOU,
     "website_url": "https://www.ntpc.com",
     "tariff_page_urls": ["https://www.ntpc.com/customer-service/residential-service/rate-schedule"]},
    {"name": "Qulliq Energy Corporation", "state_province": "NU", "utility_type": UtilityType.IOU,
     "website_url": "https://www.qec.nu.ca",
     "tariff_page_urls": ["https://www.qec.nu.ca/customer-care/accounts-and-billing/rates"]},
]


def seed_canadian_utilities(session: Session):
    """Insert curated Canadian utilities."""
    created = 0
    skipped = 0

    for data in CANADIAN_UTILITIES:
        existing = session.execute(
            select(Utility).where(
                Utility.name == data["name"],
                Utility.country == Country.CA,
            )
        ).scalar_one_or_none()

        if existing:
            skipped += 1
            continue

        utility = Utility(
            name=data["name"],
            country=Country.CA,
            state_province=data["state_province"],
            utility_type=data["utility_type"],
            website_url=data.get("website_url"),
            tariff_page_urls=data.get("tariff_page_urls"),
        )
        session.add(utility)
        created += 1

    session.commit()
    print(f"Canadian utilities: {created} created, {skipped} already existed")
    return created


def main():
    engine = get_sync_engine()
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        seed_canadian_utilities(session)

    print("Canadian utility seeding complete!")


if __name__ == "__main__":
    main()
