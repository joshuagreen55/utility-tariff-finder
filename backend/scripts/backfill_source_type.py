"""Re-run the tariff source classifier (official | third_party | unknown).

The migration that added ``tariffs.source_type`` already classified every
row, and new/updated rows are stamped on flush. Run this after changing a
utility's ``website_url`` / ``tariff_page_urls`` / ``rate_page_url_override``
or the classifier rules (``app/services/source_type.py``). Only the
``source_type`` / ``source_type_reason`` columns are written — never rate
content — so live and superseded rows are both reclassified.

Dry-run is the default; pass ``--apply`` to write.

Usage:
  python -m scripts.backfill_source_type
  python -m scripts.backfill_source_type --utility-id 1737
  python -m scripts.backfill_source_type --apply
"""
from __future__ import annotations

import argparse
import sys

from app.db.session import get_sync_engine
from app.services.source_type import reclassify_tariffs


def main(argv: list[str] | None = None) -> dict:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--utility-id", type=int, action="append", help="Limit to these utilities (repeatable)")
    parser.add_argument("--apply", action="store_true", help="Write the classification (default: dry run)")
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    engine = get_sync_engine()
    with engine.begin() as conn:
        result = reclassify_tariffs(conn, utility_ids=args.utility_id, dry_run=not args.apply)

    print(f"{'APPLIED' if args.apply else 'DRY RUN'}: tariff source classification")
    for (st, reason), n in sorted(result["counts"].items(), key=lambda kv: (kv[0][0], -kv[1])):
        print(f"  {st:<12} {reason:<22} {n:>8,}")
    if result["changed"]:
        print("  changes (old → new):")
        for (old, new), n in sorted(result["changed"].items()):
            print(f"    {old or '-':<12} → {new:<12} {n:>8,}")
    else:
        print("  no source_type changes")
    return result


if __name__ == "__main__":
    main()
