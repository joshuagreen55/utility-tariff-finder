"""Ontario LDC delivery/regulatory from OEB BillData.xml (PR2)."""
from __future__ import annotations

import unittest
from unittest import mock

from scripts import scrape_oeb_rates as oeb


SAMPLE_BILLDATA = """<?xml version="1.0" encoding="UTF-8"?>
<BillDataTable>
  <BillDataRow>
    <Dist>Toronto Hydro-Electric System Limited</Dist>
    <Class>RESIDENTIAL</Class>
    <YEAR>2026</YEAR>
    <SC>51.56</SC>
    <DC>0.0016</DC>
    <Net>0.01346</Net>
    <Conn>0.00895</Conn>
    <WMSR>0.0047</WMSR>
    <RRRP>0.0006</RRRP>
    <SSS>0.25</SSS>
    <OFC>0.38</OFC>
    <LF>1.0295</LF>
    <VC/>
  </BillDataRow>
  <BillDataRow>
    <Dist>Toronto Hydro-Electric System Limited</Dist>
    <Class>COMPETITIVE SECTOR MULTI-UNIT RESIDENTIAL</Class>
    <YEAR>2026</YEAR>
    <SC>42.36</SC>
    <DC>0.00121</DC>
    <Net>0.01346</Net>
    <Conn>0.00895</Conn>
    <WMSR>0.0047</WMSR>
    <RRRP>0.0006</RRRP>
    <SSS>0.25</SSS>
    <OFC>0.38</OFC>
    <LF>1.0295</LF>
  </BillDataRow>
  <BillDataRow>
    <Dist>Hydro One Networks Inc.</Dist>
    <Class>UR RESIDENTIAL</Class>
    <YEAR>2026</YEAR>
    <SC>42.62</SC>
    <DC>-0.0007</DC>
    <Net>0.0114</Net>
    <Conn>0.0085</Conn>
    <WMSR>0.0047</WMSR>
    <RRRP>0.0006</RRRP>
    <SSS>0.25</SSS>
    <OFC>0.42</OFC>
    <LF>1.057</LF>
  </BillDataRow>
  <BillDataRow>
    <Dist>Hydro One Networks Inc.</Dist>
    <Class>R2 RESIDENTIAL</Class>
    <YEAR>2026</YEAR>
    <SC>120.0</SC>
    <DC>0.05</DC>
    <Net>0.01</Net>
    <Conn>0.01</Conn>
    <WMSR>0.0047</WMSR>
    <RRRP>0.0006</RRRP>
    <SSS>0.25</SSS>
  </BillDataRow>
  <BillDataRow>
    <Dist>Alectra Utilities Corporation-PowerStream Rate Zone</Dist>
    <Class>RESIDENTIAL</Class>
    <YEAR>2026</YEAR>
    <SC>35.48</SC>
    <DC>0.0009</DC>
    <Net>0.0126</Net>
    <Conn>0.0049</Conn>
    <WMSR>0.0047</WMSR>
    <RRRP>0.0006</RRRP>
    <SSS>0.25</SSS>
  </BillDataRow>
  <BillDataRow>
    <Dist>Alectra Utilities Corporation-Brampton Rate Zone</Dist>
    <Class>RESIDENTIAL</Class>
    <YEAR>2026</YEAR>
    <SC>30.8</SC>
    <DC>-0.0002</DC>
    <Net>0.0126</Net>
    <Conn>0.0085</Conn>
    <WMSR>0.0047</WMSR>
    <RRRP>0.0006</RRRP>
    <SSS>0.25</SSS>
  </BillDataRow>
  <BillDataRow>
    <Dist>Algoma Power Inc.</Dist>
    <Class>SEASONAL CUSTOMERS</Class>
    <YEAR>2026</YEAR>
    <SC>105.55</SC>
    <DC>0.025</DC>
    <Net>0.0121</Net>
    <Conn>0.0088</Conn>
    <WMSR>0.0047</WMSR>
    <RRRP>0.0006</RRRP>
  </BillDataRow>
  <BillDataRow>
    <Dist>Enova Power Corp.-Kitchener-Wilmot Hydro Rate Zone</Dist>
    <Class>RESIDENTIAL</Class>
    <YEAR>2026</YEAR>
    <SC>40.0</SC>
    <DC>0.001</DC>
    <Net>0.012</Net>
    <Conn>0.008</Conn>
    <WMSR>0.0047</WMSR>
    <RRRP>0.0006</RRRP>
    <SSS>0.25</SSS>
  </BillDataRow>
  <BillDataRow>
    <Dist>Enova Power Corp.-Waterloo North Rate Zone</Dist>
    <Class>RESIDENTIAL</Class>
    <YEAR>2026</YEAR>
    <SC>41.0</SC>
    <DC>0.0011</DC>
    <Net>0.012</Net>
    <Conn>0.008</Conn>
    <WMSR>0.0047</WMSR>
    <RRRP>0.0006</RRRP>
    <SSS>0.25</SSS>
  </BillDataRow>
  <BillDataRow>
    <Dist>Synergy North Corporation</Dist>
    <Class>RESIDENTIAL</Class>
    <YEAR>2026</YEAR>
    <SC>33.0</SC>
    <DC>0.001</DC>
    <Net>0.012</Net>
    <Conn>0.005</Conn>
    <WMSR>0.0047</WMSR>
    <RRRP>0.0006</RRRP>
    <SSS>0.25</SSS>
  </BillDataRow>
  <BillDataRow>
    <Dist>Alectra Utilities Corporation-Enersource Rate Zone</Dist>
    <Class>RESIDENTIAL</Class>
    <YEAR>2026</YEAR>
    <SC>31.19</SC>
    <DC>0.0017</DC>
    <Net>0.0134</Net>
    <Conn>0.0102</Conn>
    <WMSR>0.0047</WMSR>
    <RRRP>0.0006</RRRP>
    <SSS>0.25</SSS>
  </BillDataRow>
</BillDataTable>
"""


def _gold_rates() -> oeb.OEBRateSet:
    return oeb.OEBRateSet(
        tou=oeb.TOURates(
            effective_date="2025-11-01",
            off_peak=0.098, mid_peak=0.157, on_peak=0.203,
        ),
        tiered=oeb.TieredRates(
            effective_date="2025-11-01",
            lower_tier_price=0.120, higher_tier_price=0.142,
            summer_threshold_kwh=600, winter_threshold_kwh=1000,
        ),
    )


class TestBillDataParseAndMatch(unittest.TestCase):
    def setUp(self):
        self.rows = oeb.parse_billdata_xml(SAMPLE_BILLDATA)

    def test_skips_seasonal_and_multi_unit(self):
        dists = {r.distributor for r in self.rows}
        self.assertIn("Toronto Hydro-Electric System Limited", dists)
        self.assertNotIn("Algoma Power Inc.", dists)  # only SEASONAL in sample
        tor = [r for r in self.rows if "Toronto" in r.distributor]
        self.assertEqual(len(tor), 1)
        self.assertEqual(tor[0].rate_class, "RESIDENTIAL")

    def test_toronto_vs_hydro_one_differ(self):
        tor = oeb.match_ldc_delivery("Toronto Hydro", self.rows)
        h1 = oeb.match_ldc_delivery("Hydro One", self.rows)
        self.assertIsNotNone(tor)
        self.assertIsNotNone(h1)
        self.assertNotAlmostEqual(tor.per_kwh_adder, h1.per_kwh_adder, places=5)
        self.assertAlmostEqual(tor.delivery_kwh, 0.0016 + 0.01346 + 0.00895, places=5)
        self.assertAlmostEqual(tor.regulatory_kwh, 0.0047 + 0.0006, places=5)

    def test_hydro_one_prefers_ur_over_r2(self):
        h1 = oeb.match_ldc_delivery("Hydro One", self.rows)
        self.assertEqual(h1.rate_class, "UR RESIDENTIAL")

    def test_alectra_prefers_powerstream_zone(self):
        a = oeb.match_ldc_delivery("Alectra Utilities", self.rows)
        self.assertIsNotNone(a)
        self.assertIn("PowerStream", a.distributor)

    def test_enova_zones_and_no_false_north_match(self):
        kw = oeb.match_ldc_delivery("Kitchener-Wilmot Hydro", self.rows)
        wn = oeb.match_ldc_delivery("Waterloo North Hydro", self.rows)
        self.assertIsNotNone(kw)
        self.assertIsNotNone(wn)
        self.assertIn("Kitchener-Wilmot", kw.distributor)
        self.assertIn("Waterloo North", wn.distributor)
        self.assertNotIn("Synergy", wn.distributor)

    def test_enersource_alias_to_alectra_zone(self):
        e = oeb.match_ldc_delivery("Enersource Hydro Mississauga", self.rows)
        self.assertIsNotNone(e)
        self.assertIn("Enersource", e.distributor)


class TestBuildTariffEntriesWithLdc(unittest.TestCase):
    def setUp(self):
        self.rows = oeb.parse_billdata_xml(SAMPLE_BILLDATA)
        self.rates = _gold_rates()

    def test_commodity_only_unchanged_without_ldc(self):
        entries = oeb.build_tariff_entries(self.rates, "residential")
        tou = next(e for e in entries if e["code"] == "OEB-RPP-TOU")
        energy = [c for c in tou["components"] if c["component_type"] == "energy"]
        self.assertTrue(all(abs(float(c["rate_value"]) - 0.098) < 1e-9
                            or abs(float(c["rate_value"]) - 0.157) < 1e-9
                            or abs(float(c["rate_value"]) - 0.203) < 1e-9
                            for c in energy))
        self.assertFalse(any(c["component_type"] == "adjustment" for c in tou["components"]))

    def test_toronto_energy_includes_delivery_breakdown(self):
        ldc = oeb.match_ldc_delivery("Toronto Hydro", self.rows)
        entries = oeb.build_tariff_entries(self.rates, "residential", ldc=ldc)
        tou = next(e for e in entries if e["code"] == "OEB-RPP-TOU")
        self.assertEqual(tou["energy_scope"], "delivery_plus_default_supply")
        adder = ldc.per_kwh_adder
        lf = ldc.loss_factor or 1.0
        off = [c for c in tou["components"]
               if c["component_type"] == "energy" and c["period_label"] == "Off-Peak"][0]
        self.assertAlmostEqual(
            float(off["rate_value"]), round(0.098 * lf + adder, 6), places=5,
        )
        adjs = [c for c in tou["components"] if c["component_type"] == "adjustment"]
        self.assertGreaterEqual(len(adjs), 4)
        self.assertTrue(all(c.get("included_in_energy") is True for c in adjs))
        # LF is metadata only — never a priced $/kWh ADJUSTMENT (R8).
        self.assertFalse(
            any(
                abs(float(c.get("rate_value") or 0) - float(lf)) < 1e-9
                and "loss" in str(c.get("tier_label") or "").lower()
                for c in adjs
            )
        )
        fixed = [c for c in tou["components"] if c["component_type"] == "fixed"]
        self.assertTrue(any(abs(float(c["rate_value"]) - 51.56) < 1e-6 for c in fixed))
        self.assertIn("ldc_delivery", tou)
        self.assertEqual(tou["ldc_delivery"].get("loss_factor"), lf)

    def test_toronto_and_hydro_one_energy_differ(self):
        tor = oeb.build_tariff_entries(
            self.rates, "residential",
            ldc=oeb.match_ldc_delivery("Toronto Hydro", self.rows),
        )
        h1 = oeb.build_tariff_entries(
            self.rates, "residential",
            ldc=oeb.match_ldc_delivery("Hydro One", self.rows),
        )
        tor_off = next(
            c["rate_value"] for c in tor[0]["components"]
            if c["component_type"] == "energy" and c.get("period_label") == "Off-Peak"
        )
        h1_off = next(
            c["rate_value"] for c in h1[0]["components"]
            if c["component_type"] == "energy" and c.get("period_label") == "Off-Peak"
        )
        self.assertNotAlmostEqual(float(tor_off), float(h1_off), places=5)

    def test_commercial_never_folds_ldc(self):
        ldc = oeb.match_ldc_delivery("Toronto Hydro", self.rows)
        entries = oeb.build_tariff_entries(self.rates, "commercial", ldc=ldc)
        tou = next(e for e in entries if "TOU" in e["code"])
        self.assertNotIn("ldc_delivery", tou)
        self.assertFalse(any(c["component_type"] == "adjustment" for c in tou["components"]))


class TestCommodityOnlyWarning(unittest.TestCase):
    def test_warning_skipped_when_ldc_folded(self):
        from app.services.computable import tariff_contract
        from types import SimpleNamespace

        bare = SimpleNamespace(
            code="OEB-RPP-TOU", rate_type="seasonal_tou", name="TOU",
            rate_components=[
                {"component_type": "energy", "unit": "$/kWh", "rate_value": 0.098,
                 "period_start_time": "00:00", "period_end_time": "07:00",
                 "day_type": "weekday",
                 "season_start_month": 11, "season_start_day": 1,
                 "season_end_month": 4, "season_end_day": 30},
            ],
            confidence_factors={"origin": "oeb_feed"},
        )
        folded = SimpleNamespace(
            code="OEB-RPP-TOU", rate_type="flat", name="TOU",
            rate_components=[
                {"component_type": "energy", "unit": "$/kWh", "rate_value": 0.12},
            ],
            confidence_factors={"origin": "oeb_feed", "ontario_ldc_delivery": True},
        )
        self.assertIn(
            "commodity_only_bill_incomplete",
            tariff_contract(bare)["computable_warnings"],
        )
        self.assertNotIn(
            "commodity_only_bill_incomplete",
            tariff_contract(folded)["computable_warnings"],
        )


if __name__ == "__main__":
    unittest.main()
