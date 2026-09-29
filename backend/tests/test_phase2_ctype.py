"""Phase 2: `ctype` must be bound on every fetch path (incl. Playwright).

Regression for overnight campaign failures:
  Phase 2 error on <url>: cannot access local variable 'ctype' where it is
  not associated with a value

Trigger: Phase 1 marks a domain JS-rendered → Phase 2 skips httpx, never
assigns `ctype`, then the PDF content-type check reads it.

    cd backend && python -m unittest tests.test_phase2_ctype -v
"""
from __future__ import annotations

import logging
import unittest
from unittest import mock

from scripts import tariff_pipeline as tp


def _html_page(title: str = "Residential Rates", body: str = "") -> str:
    # Enough text that Phase 2 does not treat the page as "thin" and
    # re-enter Playwright / browser-agent fallbacks.
    filler = body or (
        "Our residential electricity rates are listed below. "
        "Energy charge 0.12 $/kWh. Customer charge $15 per month. "
        + ("Rate schedule details. " * 40)
    )
    return (
        f"<html><head><title>{title}</title></head>"
        f"<body><h1>{title}</h1><p>{filler}</p></body></html>"
    )


class TestPhase2CtypeAlwaysBound(unittest.TestCase):
    def setUp(self):
        self._prev_js = set(tp._js_rendered_domains)
        tp._js_rendered_domains.clear()
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        tp._js_rendered_domains.clear()
        tp._js_rendered_domains.update(self._prev_js)
        logging.disable(logging.NOTSET)

    def test_js_rendered_domain_does_not_raise_unbound_ctype(self):
        """Domain already in `_js_rendered_domains` → Playwright path, no ctype."""
        url = "https://amanasociety.com/services/"
        domain = "amanasociety.com"
        tp._js_rendered_domains.add(domain)
        html = _html_page()

        with mock.patch.object(tp, "fetch_page_js", return_value=(html, "Residential Rates")) as js, \
                mock.patch.object(tp, "fetch_page") as httpx_fetch, \
                mock.patch.object(tp, "_try_browser_agent_fallback", return_value=[]):
            pages = tp.phase2_discover_tariff_pages(url)

        js.assert_called()
        httpx_fetch.assert_not_called()
        self.assertGreaterEqual(len(pages), 1)
        self.assertEqual(pages[0].url, url)
        self.assertGreater(len(pages[0].content.strip()), 50)

    def test_browser_required_domain_does_not_raise_unbound_ctype(self):
        """`_BROWSER_REQUIRED_DOMAINS` takes the same Playwright path."""
        # hydroquebec.com is always browser-required; use a rates-like path.
        url = "https://www.hydroquebec.com/residential/customer-space/rates/"
        html = _html_page(title="Hydro-Québec Rates")

        with mock.patch.object(tp, "fetch_page_js", return_value=(html, "Hydro-Québec Rates")) as js, \
                mock.patch.object(tp, "fetch_page") as httpx_fetch, \
                mock.patch.object(tp, "_try_browser_agent_fallback", return_value=[]):
            pages = tp.phase2_discover_tariff_pages(url)

        js.assert_called()
        httpx_fetch.assert_not_called()
        self.assertGreaterEqual(len(pages), 1)

    def test_httpx_path_still_uses_content_type_for_pdf(self):
        """Non-JS domain: PDF content-type still routes to PDF download."""
        url = "https://examplecoop.coop/rates"
        pdf_page = tp.RatePage(
            url=url,
            title="rates.pdf",
            page_type="pdf",
            content="Residential energy rate 0.11 $/kWh. " * 20,
            content_hash="abc",
        )

        with mock.patch.object(
            tp, "fetch_page", return_value=("%PDF-1.4 binary junk", "application/pdf", 200)
        ), mock.patch.object(
            tp, "_fetch_as_pdf_via_download", return_value=pdf_page
        ) as pdf_dl, mock.patch.object(tp, "fetch_page_js") as js:
            pages = tp.phase2_discover_tariff_pages(url)

        js.assert_not_called()
        pdf_dl.assert_called_once_with(url)
        self.assertEqual(pages, [pdf_page])


if __name__ == "__main__":
    unittest.main()
