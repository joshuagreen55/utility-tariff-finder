"""PR R27-1: per-utility document set builder + golden source URLs."""
from __future__ import annotations

import json
import unittest
from datetime import date
from pathlib import Path

from app.services.pricing.document_set import (
    DocumentCandidate,
    GOLDEN_DIR,
    build_document_set,
    build_golden_document_set,
    choose_current_edition,
    classify_role,
    demote_marketing,
    is_marketing_url,
    load_curated_document_sets,
    parse_effective_date,
    r27_coverage_report,
    required_roles,
)
from app.services.pricing.document_set import DocumentMember


class TestRequiredRoles(unittest.TestCase):
    def test_ontario_needs_commodity_and_delivery(self):
        self.assertEqual(
            required_roles("provincial_ontario"),
            frozenset({"provincial_commodity", "delivery"}),
        )

    def test_alberta_needs_supply_and_delivery(self):
        self.assertEqual(
            required_roles("provincial_alberta"),
            frozenset({"default_supply", "delivery"}),
        )

    def test_texas_delivery_only(self):
        self.assertEqual(required_roles("texas_tdu"), frozenset({"delivery"}))


class TestClassifyAndDates(unittest.TestCase):
    def test_oeb_commodity_vs_billdata_delivery(self):
        commodity = DocumentCandidate(
            url="https://www.oeb.ca/consumer-information-and-protection/electricity-rates"
        )
        delivery = DocumentCandidate(
            url="https://www.oeb.ca/_html/calculator/data/BillData.xml"
        )
        self.assertEqual(
            classify_role(commodity, recipe_code="provincial_ontario"),
            "provincial_commodity",
        )
        self.assertEqual(
            classify_role(delivery, recipe_code="provincial_ontario"),
            "delivery",
        )

    def test_parse_jul_2026_and_oct2026(self):
        self.assertEqual(
            parse_effective_date(
                url="https://nlhydro.com/.../Schedule_Jul_2026.pdf"
            ),
            date(2026, 7, 1),
        )
        self.assertEqual(
            parse_effective_date(
                url="https://www.fpl.com/.../residential-rates-oct2026.pdf"
            ),
            date(2026, 10, 1),
        )

    def test_snippet_date_must_be_the_effective_date(self):
        self.assertEqual(
            parse_effective_date(
                url="https://x.com/rates/schedule-r.pdf",
                text_snippet="Filed 2024-11-15 in Docket 24-0001. Effective January 1, 2026.",
            ),
            date(2026, 1, 1),
        )
        self.assertEqual(
            parse_effective_date(
                text_snippet="Issued: May 1, 2026\nEffective for bills rendered on and after June 15, 2026",
            ),
            date(2026, 6, 15),
        )
        self.assertIsNone(
            parse_effective_date(text_snippet="Advice Letter 4021-E dated 2023-03-02"),
        )

    def test_url_and_title_dates_still_count(self):
        self.assertEqual(
            parse_effective_date(url="https://x.com/2026-05-01/schedule.pdf"),
            date(2026, 5, 1),
        )
        self.assertEqual(
            parse_effective_date(title="Rates effective October 1, 2026"),
            date(2026, 10, 1),
        )

    def test_ambiguous_numeric_order_not_guessed(self):
        self.assertIsNone(parse_effective_date(url="https://x.com/rates-06-07-2026.pdf"))
        self.assertEqual(
            parse_effective_date(url="https://x.com/rates-07-15-2026.pdf"),
            date(2026, 7, 15),
        )
        self.assertEqual(
            parse_effective_date(text_snippet="Effective 15/07/2026"),
            date(2026, 7, 15),
        )

    def test_basic_service_charge_is_not_default_supply(self):
        bundled = DocumentCandidate(
            url="https://www.georgiapower.com/rates/residential-service-r.pdf",
            title="Residential Service R",
            text_snippet="Basic Service Charge $14.00 per month. Energy Charge 9 ¢/kWh",
        )
        self.assertEqual(classify_role(bundled, recipe_code="bundled"), "tariff")
        dereg = DocumentCandidate(
            url="https://www.utility.com/delivery/schedule.pdf",
            text_snippet="Basic Service Charge $8.00 per month. Distribution 4 ¢/kWh",
        )
        self.assertEqual(classify_role(dereg, recipe_code="deregulated"), "delivery")
        supply = DocumentCandidate(
            url="https://www.nationalgridus.com/MA-Home/Rates/Basic-Service",
            title="Basic Service Supply",
        )
        self.assertEqual(classify_role(supply, recipe_code="deregulated"), "default_supply")

    def test_bundled_never_gets_default_supply_role(self):
        cand = DocumentCandidate(
            url="https://www.utility.com/rates/schedule-rs.pdf",
            text_snippet="Customers may compare this price to compare with standard offer.",
        )
        self.assertEqual(classify_role(cand, recipe_code="bundled"), "tariff")

    def test_supply_tokens_are_whole_words(self):
        cand = DocumentCandidate(
            url="https://www.utility.com/delivery/errors-and-corrections.pdf",
            title="Carrollton delivery rates",
        )
        self.assertEqual(classify_role(cand, recipe_code="deregulated"), "delivery")
        rro = DocumentCandidate(url="https://www.epcor.com/rro-rates.pdf", title="RRO rates")
        self.assertEqual(classify_role(rro, recipe_code="provincial_alberta"), "default_supply")

    def test_ldc_page_mentioning_oeb_is_delivery(self):
        ldc = DocumentCandidate(
            url="https://www.londonhydro.com/residential/rates",
            text_snippet="RPP prices are set by the OEB, see oeb.ca. Delivery charge 3 ¢/kWh",
        )
        self.assertEqual(classify_role(ldc, recipe_code="provincial_ontario"), "delivery")

    def test_rider_path_without_rider_word(self):
        cand = DocumentCandidate(url="https://www.utility.com/fuel/adjustment-2026.pdf")
        self.assertEqual(classify_role(cand, recipe_code="bundled"), "rider_sheet")

    def test_marketing_marker(self):
        self.assertTrue(
            is_marketing_url(
                "https://www.fpl.com/content/dam/fplgp/us/en/rates/pdf/new-customer-overview.pdf"
            )
        )
        self.assertFalse(
            is_marketing_url(
                "https://www.fpl.com/content/dam/fplgp/us/en/rates/pdf/residential-rates-oct2026.pdf"
            )
        )


class TestEditionAndMarketing(unittest.TestCase):
    def test_choose_current_edition_prefers_later(self):
        members = [
            DocumentMember(
                role="tariff",
                url="jan",
                effective_date=date(2026, 1, 1),
            ),
            DocumentMember(
                role="tariff",
                url="jul",
                effective_date=date(2026, 7, 1),
            ),
        ]
        out = choose_current_edition(members, as_of=date(2026, 10, 9))
        selected = [m for m in out if m.is_selected]
        demoted = [m for m in out if m.reject_reason == "superseded_edition"]
        self.assertEqual([m.url for m in selected], ["jul"])
        self.assertEqual([m.url for m in demoted], ["jan"])

    def test_fpl_marketing_demoted_when_rates_present(self):
        result = build_document_set(
            [
                DocumentCandidate(
                    url="https://www.fpl.com/content/dam/fplgp/us/en/rates/pdf/new-customer-overview.pdf",
                    role="tariff",
                    title="marketing overview",
                ),
                DocumentCandidate(
                    url="https://www.fpl.com/content/dam/fplgp/us/en/rates/pdf/residential-rates-oct2026.pdf",
                    role="tariff",
                    title="October 2026 rates",
                    effective_date=date(2026, 10, 1),
                ),
            ],
            recipe_code="bundled",
            as_of=date(2026, 10, 9),
            utility_name="Florida Power & Light",
        )
        selected = result.selected()
        self.assertEqual(len(selected), 1)
        self.assertIn("residential-rates-oct2026", selected[0].url)
        self.assertIn("demoted_marketing_pdf", result.notes)
        demoted = [m for m in result.members if m.reject_reason == "marketing_pdf"]
        self.assertEqual(len(demoted), 1)


class TestGoldenCoverage(unittest.TestCase):
    def test_curated_covers_all_25_utilities(self):
        curated = load_curated_document_sets()
        plans = json.loads((GOLDEN_DIR / "plans.json").read_text())["plans"]
        utils = {p["utility_name"] for p in plans}
        self.assertEqual(len(utils), 25)
        missing = sorted(utils - set(curated))
        self.assertEqual(missing, [], f"curated document_sets missing: {missing}")

    def test_r27_coverage_all_complete(self):
        rep = r27_coverage_report(as_of=date(2026, 10, 9))
        self.assertEqual(rep["incomplete"], 0, rep["rows"])
        self.assertEqual(rep["complete"], 25)

    def test_every_golden_plan_has_source_url(self):
        plans = json.loads((GOLDEN_DIR / "plans.json").read_text())["plans"]
        missing = [p["plan_key"] for p in plans if not p.get("source_url")]
        self.assertEqual(missing, [])
        incomplete = [
            p["plan_key"] for p in plans
            if not (p.get("document_set") or {}).get("complete")
        ]
        self.assertEqual(incomplete, [])

    def test_nl_selects_july_not_january(self):
        result = build_golden_document_set(
            "Newfoundland and Labrador Hydro",
            "bundled",
            as_of=date(2026, 10, 9),
        )
        urls = result.selected_urls()
        self.assertTrue(any("Jul_2026" in u for u in urls))
        self.assertFalse(any(
            m.is_selected and "Jan_2026" in m.url for m in result.members
        ))

    def test_ontario_has_commodity_and_delivery(self):
        result = build_golden_document_set(
            "Toronto Hydro", "provincial_ontario", as_of=date(2026, 10, 9)
        )
        roles = {m.role for m in result.selected()}
        self.assertIn("provincial_commodity", roles)
        self.assertIn("delivery", roles)

    def test_oncor_has_delivery(self):
        result = build_golden_document_set(
            "Oncor Electric Delivery", "texas_tdu", as_of=date(2026, 10, 9)
        )
        self.assertTrue(result.complete)
        self.assertTrue(any(m.role == "delivery" for m in result.selected()))


class TestDemoteHelper(unittest.TestCase):
    def test_demote_noop_without_rates_peer(self):
        members = [
            DocumentMember(
                role="tariff",
                url="https://example.com/new-customer-overview.pdf",
            )
        ]
        out = demote_marketing(members)
        self.assertTrue(out[0].is_selected)
        self.assertIsNone(out[0].reject_reason)


if __name__ == "__main__":
    unittest.main()
