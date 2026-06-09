"""Retry Duke Florida using the official RS-1 rate PDF."""
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import get_sync_engine
from app.models import Utility
from scripts.tariff_pipeline import run_pipeline

DUKE_FL_ID = 382
DUKE_FL_PDF = "https://p-cd.duke-energy.com/-/media/pdfs/for-your-home/rates/rates-fl/pe-rates-rs-1.pdf"

engine = get_sync_engine()
with Session(engine) as session:
    util = session.get(Utility, DUKE_FL_ID)
    print(f"Override: {DUKE_FL_PDF}")
    util.rate_page_url_override = DUKE_FL_PDF
    session.commit()
    before = session.execute(
        text("SELECT COUNT(*) FILTER (WHERE last_verified_at IS NOT NULL), "
             "COUNT(*) FILTER (WHERE last_verified_at IS NULL AND openei_id IS NOT NULL) "
             "FROM tariffs WHERE utility_id = :uid"),
        {"uid": DUKE_FL_ID},
    ).first()
    print(f"Before: fresh={before[0]}, old={before[1]}")

result = run_pipeline(DUKE_FL_ID, dry_run=False)
valid = (result.phase4_validation or {}).get("valid", 0)
print(f"Valid extracted: {valid}")
print(f"Errors: {result.errors}")

with Session(engine) as session:
    after = session.execute(
        text("SELECT COUNT(*) FILTER (WHERE last_verified_at IS NOT NULL), "
             "COUNT(*) FILTER (WHERE last_verified_at IS NULL AND openei_id IS NOT NULL) "
             "FROM tariffs WHERE utility_id = :uid"),
        {"uid": DUKE_FL_ID},
    ).first()
    print(f"After: fresh={after[0]}, old={after[1]}")
