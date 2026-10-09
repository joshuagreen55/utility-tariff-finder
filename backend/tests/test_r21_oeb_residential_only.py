"""R21: the Ontario OEB feed stores residential plans only.

R20 trial: every OEB refresh also wrote three "Small Business" (COMMERCIAL)
rows per LDC (24 rows on the 8 trial LDCs). The database is residential-only.

    cd backend && python -m unittest tests.test_r21_oeb_residential_only -v
"""
from __future__ import annotations

import unittest
from unittest import mock

from scripts import scrape_oeb_rates as oeb
from scripts import tariff_pipeline as tp
from scripts import repair_hydro_one_oeb_residential as repair
from tests.pg_harness import PostgresTestCase


class TestPipelineOebPathResidentialOnly(unittest.TestCase):
    def test_centralized_path_passes_only_residential_entries(self):
        seen = {}

        def _store(uid, entries, dry_run):
            seen["entries"] = list(entries)
            return len(entries)

        with mock.patch.object(oeb, "fetch_oeb_page", return_value="<html/>"), \
             mock.patch.object(oeb, "parse_oeb_rates", return_value=repair.gold_oeb_rate_set()), \
             mock.patch.object(oeb, "fetch_billdata_xml", side_effect=RuntimeError("offline")), \
             mock.patch.object(oeb, "get_ontario_utilities", return_value=[{"id": 1729, "name": "Kitchener-Wilmot Hydro"}]), \
             mock.patch.object(oeb, "store_oeb_tariffs", side_effect=_store):
            tp._try_centralized_regulator(1729, "ON", "CA", dry_run=False)
        classes = {e["customer_class"] for e in seen["entries"]}
        self.assertEqual(classes, {"residential"})
        self.assertFalse(any("Small Business" in e["name"] for e in seen["entries"]))
        self.assertEqual(len(seen["entries"]), 3)


class TestStoreOebSkipsCommercial(PostgresTestCase):
    def test_commercial_entries_are_never_written(self):
        uid = self.make_utility("Burlington Hydro", state="ON", country="CA")
        rates = repair.gold_oeb_rate_set()
        entries = oeb.build_tariff_entries(rates, "residential") + oeb.build_tariff_entries(rates, "commercial")
        n = oeb.store_oeb_tariffs(uid, entries, dry_run=False)
        self.assertEqual(n, 3)
        rows = self.tariffs_for(uid, live_only=True)
        self.assertEqual({t.customer_class.value for t in rows}, {"residential"})
        self.assertEqual(len(rows), 3)


if __name__ == "__main__":
    unittest.main()
