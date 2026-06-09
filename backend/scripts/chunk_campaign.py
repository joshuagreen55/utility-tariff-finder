"""Chunk definitions for the stale OpenEI cleanup campaign.

Each chunk lists utilities, optional extract-time URL overrides (Celery run),
and optional retry overrides (sync follow-up with skip_search=True).
"""
from dataclasses import dataclass, field


@dataclass(frozen=True)
class RetryOverride:
    url: str
    website: str = ""


@dataclass(frozen=True)
class ChunkSpec:
    number: int
    utilities: list[tuple[int, str]]
    extract_overrides: dict[int, str] = field(default_factory=dict)
    retry_overrides: dict[int, RetryOverride] = field(default_factory=dict)

    @property
    def utility_ids(self) -> list[int]:
        return [uid for uid, _ in self.utilities]


CHUNKS: dict[int, ChunkSpec] = {
    2: ChunkSpec(
        number=2,
        utilities=[
            (1064, "Southern California Edison"),
            (870, "Pacific Gas & Electric"),
            (246, "Consolidated Edison"),
            (819, "Northern States Power MN (Xcel)"),
            (858, "Otter Tail Power"),
            (382, "Duke Energy Florida"),
            (316, "Duke Energy Carolinas"),
            (850, "Orange & Rockland"),
            (1020, "City of Seattle"),
            (238, "Commonwealth Edison (ComEd IL)"),
        ],
        extract_overrides={
            1064: "https://www.sce.com/residential/rates",
            870: "https://www.pge.com/tariffs/electric.shtml",
        },
        retry_overrides={
            382: RetryOverride(
                url="https://p-cd.duke-energy.com/-/media/pdfs/for-your-home/rates/rates-fl/pe-rates-rs-1.pdf",
                website="https://www.duke-energy.com",
            ),
        },
    ),
    3: ChunkSpec(
        number=3,
        utilities=[
            (1064, "Southern California Edison (re-run)"),
            (933, "New York Power Authority"),
            (1053, "South Plains Electric Coop TX"),
            (326, "East Mississippi Elec Pwr Assn"),
            (844, "Oklahoma Gas & Electric"),
            (338, "El Paso Electric"),
            (675, "Montana-Dakota Utilities"),
            (514, "Indiana Michigan Power"),
            (1261, "Wheatland Electric Coop KS"),
            (1314, "Tucson Electric Power"),
        ],
        extract_overrides={
            1064: (
                "https://www.sce.com/sites/default/files/custom-files/PDF_Files/"
                "Residential%20Rates%20Fact%20Sheet%20English%20FINAL%20WCAG%20August%202023_edits.pdf"
            ),
        },
        retry_overrides={
            1064: RetryOverride(
                url=(
                    "https://www.sce.com/sites/default/files/custom-files/PDF_Files/"
                    "Residential%20Rates%20Fact%20Sheet%20English%20FINAL%20WCAG%20August%202023_edits.pdf"
                ),
                website="https://www.sce.com",
            ),
            933: RetryOverride(
                url=(
                    "https://www.nypa.gov/-/media/nypa/documents/document-library/"
                    "rates/nyc091417.pdf"
                ),
                website="https://www.nypa.gov",
            ),
            338: RetryOverride(
                url=(
                    "https://www.epelectric.com/files/html/Rates_and_Regulatory/"
                    "Docket_46831_Stamped_Tariffs/03_-_Rate_01_Residential_Service_Rate.pdf"
                ),
                website="https://www.epelectric.com",
            ),
        },
    ),
    4: ChunkSpec(
        number=4,
        utilities=[
            (872, "PacifiCorp"),
            (249, "Consumers Energy Co (MI)"),
            (122, "Broad River Electric Coop"),
            (1339, "Southeastern Power Admin"),
            (553, "Evergy Metro"),
            (1309, "Evergy Kansas Central"),
            (344, "Elkhorn Rural Public Pwr Dist"),
            (51, "Arizona Public Service"),
            (304, "DTE Electric Company"),
            (10, "Alabama Power Co"),
        ],
        extract_overrides={
            249: (
                "https://www.consumersenergy.com/-/media/CE/Documents/rates/"
                "electric-rate-book.ashx?la=en&hash=3A1AD0"
            ),
            304: (
                "https://www.michigan.gov/-/media/Project/Websites/mpsc/consumer/"
                "rate-books/electric/dte/dtee1cur.pdf"
            ),
        },
        retry_overrides={
            1339: RetryOverride(
                url="https://www.energy.gov/sites/prod/files/2013/06/f1/SCE%26G-4-E.pdf",
                website="https://www.energy.gov/sepa",
            ),
            304: RetryOverride(
                url=(
                    "https://www.michigan.gov/-/media/Project/Websites/mpsc/consumer/"
                    "rate-books/electric/dte/dtee1cur.pdf"
                ),
                website="https://www.dteenergy.com",
            ),
            872: RetryOverride(
                url=(
                    "https://www.pacificpower.net/content/dam/pcorp/documents/en/pacificpower/"
                    "rates-regulation/oregon/tariffs/rates/"
                    "004_Residential_Service_Delivery_Service.pdf"
                ),
                website="https://www.pacificpower.net",
            ),
        },
    ),
    5: ChunkSpec(
        number=5,
        utilities=[
            (818, "Northern States Power Co"),
            (62, "Austin Energy"),
            (826, "Northwestern Wisconsin Elec Co"),
            (688, "City of Mesa (AZ)"),
            (281, "Custer Public Power District"),
            (927, "Portland General Electric Co"),
            (1307, "Kentucky Power Co"),
            (307, "Dixie Electric Membership Corp (LA)"),
            (930, "Potomac Electric Power Co"),
            (517, "Interstate Power and Light Co"),
        ],
        extract_overrides={
            927: "https://www.portlandgeneral.com/rates/electric-service-schedules",
        },
        retry_overrides={},
    ),
}


def get_chunk(number: int) -> ChunkSpec:
    if number not in CHUNKS:
        known = ", ".join(str(n) for n in sorted(CHUNKS))
        raise SystemExit(f"Unknown chunk {number}. Known chunks: {known}")
    return CHUNKS[number]


def all_touched_utility_ids() -> set[int]:
    ids: set[int] = set()
    for spec in CHUNKS.values():
        ids.update(spec.utility_ids)
    return ids


def merged_extract_overrides() -> dict[int, str]:
    out: dict[int, str] = {}
    for spec in CHUNKS.values():
        out.update(spec.extract_overrides)
    return out


def merged_retry_overrides() -> dict[int, RetryOverride]:
    out: dict[int, RetryOverride] = {}
    for spec in CHUNKS.values():
        out.update(spec.retry_overrides)
    return out
