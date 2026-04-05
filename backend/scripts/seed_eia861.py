"""
Seed US utility data from EIA Form 861.

Downloads the EIA-861 data files and populates the utilities table with
all US electric utilities including their EIA IDs, types, and states.

Usage:
    python -m scripts.seed_eia861
"""

import io
import zipfile

import httpx
import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.session import get_sync_engine
from app.db.base import Base
from app.models import Utility, UtilityType, Country

EIA_861_URL = "https://zenodo.org/api/records/15111634/files/eia861-2023.zip/content"

OWNERSHIP_MAP = {
    "Investor Owned": UtilityType.IOU,
    "Municipal": UtilityType.MUNICIPAL,
    "Cooperative": UtilityType.COOPERATIVE,
    "Political Subdivision": UtilityType.POLITICAL_SUBDIVISION,
    "Federal": UtilityType.FEDERAL,
    "State": UtilityType.STATE,
    "Retail Power Marketer": UtilityType.RETAIL_MARKETER,
    "Behind the Meter": UtilityType.BEHIND_METER,
    "Community Choice Aggregator": UtilityType.COMMUNITY_CHOICE,
}


def download_eia861() -> pd.DataFrame:
    """Download and extract the EIA-861 utility data."""
    print("Downloading EIA-861 data...")
    with httpx.Client(timeout=60, follow_redirects=True) as client:
        resp = client.get(EIA_861_URL)
        resp.raise_for_status()

    zf = zipfile.ZipFile(io.BytesIO(resp.content))

    service_file = None
    utility_file = None
    for name in zf.namelist():
        lower = name.lower()
        if "utility_data" in lower and lower.endswith(".xlsx"):
            utility_file = name
        elif "service_territory" in lower and lower.endswith(".xlsx"):
            service_file = name

    if utility_file:
        print(f"  Reading {utility_file}...")
        df = pd.read_excel(zf.open(utility_file), header=1, dtype=str)
        return df

    print("  Available files in ZIP:", zf.namelist())
    for name in zf.namelist():
        if name.lower().endswith(".xlsx") or name.lower().endswith(".csv"):
            print(f"  Trying {name}...")
            try:
                if name.lower().endswith(".xlsx"):
                    df = pd.read_excel(zf.open(name), dtype=str)
                else:
                    df = pd.read_csv(zf.open(name), dtype=str)
                if any("utility" in c.lower() for c in df.columns):
                    return df
            except Exception as e:
                print(f"    Skipped: {e}")

    raise RuntimeError("Could not find utility data file in EIA-861 ZIP")


def find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Find the first matching column name (case-insensitive)."""
    cols_lower = {c.lower().strip(): c for c in df.columns}
    for candidate in candidates:
        if candidate.lower() in cols_lower:
            return cols_lower[candidate.lower()]
    for candidate in candidates:
        for col_lower, col_orig in cols_lower.items():
            if candidate.lower() in col_lower:
                return col_orig
    return None


def seed_utilities(session: Session, df: pd.DataFrame):
    """Insert utilities from the EIA-861 dataframe."""
    eia_id_col = find_column(df, ["Utility Number", "utility_id", "eia_id", "Utility_Number"])
    name_col = find_column(df, ["Utility Name", "utility_name", "Utility_Name"])
    state_col = find_column(df, ["State", "state"])
    ownership_col = find_column(df, ["Ownership", "ownership_type", "Ownership Type"])

    print(f"  Columns found: eia_id={eia_id_col}, name={name_col}, state={state_col}, ownership={ownership_col}")
    print(f"  Total rows: {len(df)}")

    if not eia_id_col or not name_col:
        print("  ERROR: Could not identify required columns. Available columns:")
        print(f"  {list(df.columns)}")
        return 0

    created = 0
    skipped = 0

    for _, row in df.iterrows():
        try:
            eia_id = int(float(row[eia_id_col]))
        except (ValueError, TypeError):
            skipped += 1
            continue

        name = str(row[name_col]).strip()
        if not name or name == "nan":
            skipped += 1
            continue

        existing = session.execute(
            select(Utility).where(Utility.eia_id == eia_id)
        ).scalar_one_or_none()
        if existing:
            skipped += 1
            continue

        state = str(row[state_col]).strip() if state_col and pd.notna(row.get(state_col)) else "Unknown"
        ownership = str(row[ownership_col]).strip() if ownership_col and pd.notna(row.get(ownership_col)) else ""
        utility_type = OWNERSHIP_MAP.get(ownership, UtilityType.OTHER)

        utility = Utility(
            name=name,
            eia_id=eia_id,
            country=Country.US,
            state_province=state,
            utility_type=utility_type,
        )
        session.add(utility)
        created += 1

        if created % 500 == 0:
            session.commit()
            print(f"  Committed {created} utilities...")

    session.commit()
    print(f"  Done: {created} utilities created, {skipped} skipped")
    return created


def main():
    engine = get_sync_engine()
    Base.metadata.create_all(engine)

    df = download_eia861()
    print(f"Loaded {len(df)} rows with columns: {list(df.columns[:10])}...")

    with Session(engine) as session:
        seed_utilities(session, df)

    print("\nEIA-861 seeding complete!")


if __name__ == "__main__":
    main()
