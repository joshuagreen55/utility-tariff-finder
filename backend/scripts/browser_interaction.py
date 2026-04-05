"""
Browser interaction module for utility rate page scraping.

Goes beyond basic Playwright page rendering to actually INTERACT with
pages — clicking dropdowns, selecting radio buttons, waiting for dynamic
content, and extracting data from interactive widgets.

This handles sites like Alectra, Elexicon, Oshawa PUC that:
  1. Block simple HTTP requests (403 Forbidden)
  2. Require JavaScript to render content
  3. Have interactive elements (dropdowns, tabs, radio buttons)
  4. Load rate data dynamically based on user selections

Usage as a module:
    from scripts.browser_interaction import BrowserAgent, InteractionPlan

    agent = BrowserAgent()
    result = agent.scrape_interactive_rate_page(
        url="https://alectrautilities.com/rates-service-charges",
        interactions=[
            {"action": "select_radio", "label": "Residential Customer"},
            {"action": "click_expand", "label": "Time-of-Use Pricing"},
        ]
    )

Usage as CLI:
    python -m scripts.browser_interaction --url https://alectrautilities.com/rates-service-charges
    python -m scripts.browser_interaction --utility-id 1234
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("browser_agent")

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
LLM_MODEL = "claude-3-5-haiku-20241022"


def _css_escape(text: str) -> str:
    """Escape a string for safe use inside CSS selector quotes."""
    return text.replace("\\", "\\\\").replace("'", "\\'").replace('"', '\\"')


@dataclass
class PageSnapshot:
    """A snapshot of page content after an interaction."""
    url: str
    title: str
    html: str
    text: str
    screenshot_path: str = ""
    interactions_performed: list[str] = field(default_factory=list)


@dataclass
class InteractionStep:
    """A single browser interaction."""
    action: str  # click, select_radio, select_dropdown, expand_section, wait, scroll
    selector: str = ""
    label: str = ""
    value: str = ""
    wait_ms: int = 1000


class BrowserAgent:
    """Browser agent that can navigate and interact with utility rate pages."""

    def __init__(self, headless: bool = True, timeout_ms: int = 30000):
        self.headless = headless
        self.timeout_ms = timeout_ms
        self._pw = None
        self._browser = None

    def __enter__(self):
        from playwright.sync_api import sync_playwright
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(
            headless=self.headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )
        return self

    def __exit__(self, *args):
        if self._browser:
            self._browser.close()
        if self._pw:
            self._pw.stop()

    def _new_page(self):
        context = self._browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
            java_script_enabled=True,
        )
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        """)
        return context.new_page()

    def fetch_page(self, url: str, wait_for_idle: bool = True) -> PageSnapshot:
        """Navigate to a URL and return page content after JS rendering."""
        page = self._new_page()
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
            if wait_for_idle:
                try:
                    page.wait_for_load_state("networkidle", timeout=10000)
                except Exception:
                    pass
            time.sleep(1)

            return PageSnapshot(
                url=page.url,
                title=page.title(),
                html=page.content(),
                text=page.inner_text("body"),
            )
        finally:
            page.context.close()

    def interact_and_scrape(
        self,
        url: str,
        interactions: list[InteractionStep] | None = None,
        auto_discover: bool = True,
    ) -> list[PageSnapshot]:
        """Navigate to URL, perform interactions, and collect page snapshots.

        If auto_discover=True and no interactions provided, the agent will
        automatically discover and interact with rate-related elements.
        """
        page = self._new_page()
        snapshots = []

        try:
            log.info(f"Navigating to {url}")
            page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
            try:
                page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                pass
            time.sleep(1)

            # Initial snapshot
            snapshots.append(PageSnapshot(
                url=page.url,
                title=page.title(),
                html=page.content(),
                text=page.inner_text("body"),
                interactions_performed=["initial_load"],
            ))

            if interactions:
                for step in interactions:
                    snapshot = self._perform_interaction(page, step)
                    if snapshot:
                        snapshots.append(snapshot)
            elif auto_discover:
                discovered = self._auto_discover_interactions(page)
                for step in discovered:
                    snapshot = self._perform_interaction(page, step)
                    if snapshot:
                        snapshots.append(snapshot)

            return snapshots
        finally:
            page.context.close()

    def _perform_interaction(self, page, step: InteractionStep) -> PageSnapshot | None:
        """Execute a single interaction step and return a snapshot."""
        try:
            if step.action == "click":
                self._do_click(page, step)
            elif step.action == "select_radio":
                self._do_select_radio(page, step)
            elif step.action == "select_dropdown":
                self._do_select_dropdown(page, step)
            elif step.action == "expand_section":
                self._do_expand_section(page, step)
            elif step.action == "click_tab":
                self._do_click_tab(page, step)
            elif step.action == "scroll":
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            elif step.action == "wait":
                time.sleep(step.wait_ms / 1000)
            else:
                log.warning(f"Unknown action: {step.action}")
                return None

            time.sleep(step.wait_ms / 1000)

            return PageSnapshot(
                url=page.url,
                title=page.title(),
                html=page.content(),
                text=page.inner_text("body"),
                interactions_performed=[f"{step.action}: {step.label or step.selector}"],
            )
        except Exception as e:
            log.warning(f"Interaction failed ({step.action} '{step.label}'): {e}")
            return None

    def _do_click(self, page, step: InteractionStep):
        if step.selector:
            page.click(step.selector, timeout=5000)
        elif step.label:
            page.get_by_text(step.label, exact=False).first.click(timeout=5000)

    def _do_select_radio(self, page, step: InteractionStep):
        if step.selector:
            page.click(step.selector, timeout=5000)
        elif step.label:
            # Try clicking the label text (common pattern for radio buttons)
            el = page.get_by_label(step.label, exact=False)
            if el.count() > 0:
                el.first.check()
            else:
                page.get_by_text(step.label, exact=False).first.click(timeout=5000)

    def _do_select_dropdown(self, page, step: InteractionStep):
        if step.selector:
            page.select_option(step.selector, label=step.value or step.label)
        elif step.label:
            selects = page.locator("select")
            for i in range(selects.count()):
                sel = selects.nth(i)
                options = sel.locator("option")
                for j in range(options.count()):
                    if step.label.lower() in (options.nth(j).text_content() or "").lower():
                        sel.select_option(label=options.nth(j).text_content())
                        return

    def _do_expand_section(self, page, step: InteractionStep):
        """Click on expandable sections (accordions, collapsibles)."""
        if step.selector:
            page.click(step.selector, timeout=5000)
        elif step.label:
            safe = _css_escape(step.label)
            candidates = page.locator(
                f"button:has-text('{safe}'), "
                f"[role='button']:has-text('{safe}'), "
                f"summary:has-text('{safe}'), "
                f"h2:has-text('{safe}'), "
                f"h3:has-text('{safe}'), "
                f".accordion:has-text('{safe}'), "
                f"[data-toggle]:has-text('{safe}')"
            )
            if candidates.count() > 0:
                candidates.first.click(timeout=5000)

    def _do_click_tab(self, page, step: InteractionStep):
        if step.selector:
            page.click(step.selector, timeout=5000)
        elif step.label:
            safe = _css_escape(step.label)
            tab = page.locator(
                f"[role='tab']:has-text('{safe}'), "
                f".tab:has-text('{safe}'), "
                f"a[data-toggle='tab']:has-text('{safe}')"
            )
            if tab.count() > 0:
                tab.first.click(timeout=5000)

    def _expand_all_collapsed(self, page):
        """Expand all collapsed/hidden sections on the page — sidebars, accordions,
        dropdowns, details elements, etc."""
        # Click on collapsed sidebar sections / accordion headers
        collapse_selectors = [
            "[aria-expanded='false']",
            "details:not([open]) summary",
            ".collapsed",
            "[data-toggle='collapse'].collapsed",
            ".accordion-button.collapsed",
        ]
        for sel in collapse_selectors:
            try:
                elements = page.locator(sel)
                for i in range(min(elements.count(), 20)):
                    try:
                        el = elements.nth(i)
                        el_text = (el.text_content() or "").strip().lower()
                        if any(kw in el_text for kw in [
                            "service type", "customer", "residential", "rate",
                            "pricing", "charge", "electric", "tiered", "tou",
                            "time-of-use", "overnight", "location",
                        ]):
                            el.click(timeout=3000)
                            time.sleep(0.5)
                            log.info(f"  Expanded collapsed section: {el_text[:50]}")
                    except Exception:
                        continue
            except Exception:
                continue

    def _select_customer_class(self, page, customer_class: str) -> bool:
        """Try multiple strategies to select a customer class on the page."""
        # Strategy 1: find actual radio inputs and click their visible label
        try:
            radios = page.locator("input[type='radio']")
            for i in range(radios.count()):
                radio = radios.nth(i)
                radio_id = radio.get_attribute("id") or ""
                if radio_id:
                    label = page.locator(f"label[for='{radio_id}']")
                    if label.count() > 0:
                        label_text = (label.first.text_content() or "").strip()
                        if customer_class.lower() in label_text.lower():
                            if label.first.is_visible():
                                label.first.click(timeout=5000)
                                log.info(f"  Clicked visible radio label: {label_text}")
                                return True
                            else:
                                # Label not visible — try clicking the radio directly via JS
                                radio.evaluate("el => el.click()")
                                radio.evaluate("el => el.dispatchEvent(new Event('change', {bubbles: true}))")
                                log.info(f"  JS-clicked hidden radio: {label_text}")
                                return True
        except Exception as e:
            log.debug(f"  Radio strategy 1 failed: {e}")

        # Strategy 2: look for visible links/buttons with the text (in sidebar, not nav)
        try:
            safe_cc = _css_escape(customer_class)
            for sel in [
                f"aside :text-is('{safe_cc}')",
                f".sidebar :text-is('{safe_cc}')",
                f"[class*='filter'] :text-is('{safe_cc}')",
                f"fieldset :text-is('{safe_cc}')",
            ]:
                el = page.locator(sel)
                if el.count() > 0 and el.first.is_visible():
                    el.first.click(timeout=5000)
                    log.info(f"  Clicked sidebar element: {sel}")
                    return True
        except Exception as e:
            log.debug(f"  Sidebar strategy failed: {e}")

        # Strategy 3: use Playwright force-click on hidden radio/label
        try:
            radios = page.locator("input[type='radio']")
            for i in range(radios.count()):
                radio = radios.nth(i)
                radio_id = radio.get_attribute("id") or ""
                if radio_id:
                    label = page.locator(f"label[for='{radio_id}']")
                    if label.count() > 0:
                        label_text = (label.first.text_content() or "").strip()
                        if customer_class.lower() in label_text.lower():
                            radio.click(force=True, timeout=5000)
                            time.sleep(2)
                            try:
                                page.wait_for_load_state("networkidle", timeout=5000)
                            except Exception:
                                pass
                            log.info(f"  Force-clicked radio: {label_text}")
                            return True
        except Exception as e:
            log.debug(f"  Force-click strategy failed: {e}")

        # Strategy 4: use JavaScript to check the radio and trigger full event chain
        try:
            result = page.evaluate("""(targetClass) => {
                const radios = document.querySelectorAll('input[type="radio"]');
                for (const r of radios) {
                    const label = document.querySelector('label[for="' + r.id + '"]');
                    const text = label ? label.textContent : r.parentElement?.textContent || '';
                    if (text.toLowerCase().includes(targetClass)) {
                        r.checked = true;
                        r.dispatchEvent(new MouseEvent('click', {bubbles: true, cancelable: true}));
                        r.dispatchEvent(new Event('change', {bubbles: true}));
                        r.dispatchEvent(new Event('input', {bubbles: true}));
                        if (typeof jQuery !== 'undefined') {
                            jQuery(r).trigger('click').trigger('change');
                        }
                        return text.trim();
                    }
                }
                return null;
            }""", customer_class.lower())
            if result:
                time.sleep(3)
                try:
                    page.wait_for_load_state("networkidle", timeout=5000)
                except Exception:
                    pass
                log.info(f"  JS-selected radio with full events: {result}")
                return True
        except Exception as e:
            log.debug(f"  JS radio strategy failed: {e}")

        return False

    def _auto_discover_interactions(self, page) -> list[InteractionStep]:
        """Automatically discover interactive elements on a rate page.

        Looks for:
        - Customer type selectors (residential/commercial radio buttons or dropdowns)
        - Expandable rate sections (accordions, collapsibles)
        - Tab panels for different rate types
        - Location/city selectors
        """
        interactions = []
        text = page.inner_text("body").lower()

        # Look for customer class radio buttons
        for label in ["residential", "small business", "commercial"]:
            radio = page.get_by_label(label, exact=False)
            if radio.count() > 0:
                interactions.append(InteractionStep(
                    action="select_radio",
                    label=label.title(),
                    wait_ms=1500,
                ))

        # Look for expandable rate sections
        rate_keywords = [
            "time-of-use", "tiered pricing", "ultra-low overnight",
            "tou pricing", "flat rate", "general service",
        ]
        for keyword in rate_keywords:
            safe_kw = _css_escape(keyword)
            expandables = page.locator(
                f"button:has-text('{safe_kw}'), "
                f"[role='button']:has-text('{safe_kw}'), "
                f"summary:has-text('{safe_kw}'), "
                f".accordion-header:has-text('{safe_kw}')"
            )
            if expandables.count() > 0:
                interactions.append(InteractionStep(
                    action="expand_section",
                    label=keyword,
                    wait_ms=1000,
                ))

        # Look for rate-related tabs
        tabs = page.locator("[role='tab']")
        for i in range(tabs.count()):
            tab_text = (tabs.nth(i).text_content() or "").strip().lower()
            if any(kw in tab_text for kw in ["residential", "commercial", "business", "rate"]):
                interactions.append(InteractionStep(
                    action="click_tab",
                    label=tabs.nth(i).text_content().strip(),
                    wait_ms=1500,
                ))

        if interactions:
            log.info(f"Auto-discovered {len(interactions)} interactions: "
                     f"{[f'{s.action}:{s.label}' for s in interactions]}")
        else:
            log.info("No interactive rate elements discovered")

        return interactions

    def scrape_interactive_rate_page(
        self,
        url: str,
        customer_classes: list[str] | None = None,
    ) -> list[PageSnapshot]:
        """High-level method: scrape a utility rate page by cycling through
        customer class selections and expanding all rate sections.

        This is the main entry point for the tariff pipeline integration.
        """
        if customer_classes is None:
            customer_classes = ["Residential Customer", "Small Business"]

        all_snapshots = []
        page = self._new_page()

        try:
            log.info(f"Navigating to {url}")
            page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
            try:
                page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                pass
            time.sleep(2)

            # Capture initial state
            all_snapshots.append(PageSnapshot(
                url=page.url,
                title=page.title(),
                html=page.content(),
                text=page.inner_text("body"),
                interactions_performed=["initial_load"],
            ))

            # First, expand any collapsed sidebar sections that might hide
            # customer type selectors or rate details
            self._expand_all_collapsed(page)
            time.sleep(1)

            for customer_class in customer_classes:
                log.info(f"Selecting customer class: {customer_class}")
                selected = self._select_customer_class(page, customer_class)

                if not selected:
                    log.warning(f"Could not find selector for '{customer_class}' — using current page state")

                time.sleep(1.5)

                # Expand all rate sections
                expandable_buttons = page.locator(
                    "button[aria-expanded='false'], "
                    "[role='button'][aria-expanded='false'], "
                    "summary, "
                    ".accordion-header"
                )
                for i in range(expandable_buttons.count()):
                    try:
                        btn_text = (expandable_buttons.nth(i).text_content() or "").lower()
                        if any(kw in btn_text for kw in [
                            "pricing", "rate", "charge", "tou", "tiered",
                            "overnight", "time-of-use", "service",
                        ]):
                            expandable_buttons.nth(i).click()
                            time.sleep(0.5)
                    except Exception:
                        pass

                time.sleep(1)

                # Capture snapshot — use both visible text and full HTML
                full_html = page.content()
                visible_text = page.inner_text("body")
                # Also extract text from the HTML in case inner_text misses dynamically loaded content
                try:
                    from bs4 import BeautifulSoup as BS4
                    soup = BS4(full_html, "lxml")
                    for tag in soup(["script", "style", "nav", "noscript"]):
                        tag.decompose()
                    main = soup.find("main") or soup.find("article") or soup
                    parsed_text = main.get_text(separator="\n", strip=True)
                    if len(parsed_text) > len(visible_text):
                        visible_text = parsed_text
                except Exception:
                    pass

                all_snapshots.append(PageSnapshot(
                    url=page.url,
                    title=page.title(),
                    html=full_html,
                    text=visible_text,
                    interactions_performed=[f"selected: {customer_class}", "expanded all sections"],
                ))

            return all_snapshots
        finally:
            page.context.close()


def extract_tariffs_from_snapshots(
    snapshots: list[PageSnapshot],
    utility_name: str,
) -> list[dict]:
    """Use LLM to extract structured tariff data from browser snapshots."""
    if not ANTHROPIC_API_KEY:
        log.error("ANTHROPIC_API_KEY not set — cannot extract tariffs")
        return []

    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    combined_text = ""
    for snap in snapshots:
        combined_text += f"\n\n--- Page: {snap.url} (after: {', '.join(snap.interactions_performed)}) ---\n"
        # Use text content, truncated to avoid token limits
        combined_text += snap.text[:15000]

    prompt = f"""Extract ALL electricity rate tariffs from the following page content for {utility_name}.

For each tariff found, provide:
- name: descriptive name (e.g. "Time-of-Use (TOU) — Residential")
- customer_class: "residential" or "commercial"
- rate_type: one of "flat", "tou", "tiered", "tou_tiered", "demand"
- effective_date: in YYYY-MM-DD format if shown
- description: brief description
- components: list of rate components, each with:
  - component_type: "energy", "demand", or "fixed"
  - unit: "$/kWh", "$/kW", or "$/month"
  - rate_value: numeric value in dollars (e.g. 0.098 not 9.8 cents)
  - period_label: for TOU rates (e.g. "On-Peak", "Mid-Peak", "Off-Peak")
  - tier_label: for tiered rates (e.g. "First 600 kWh")
  - tier_min_kwh / tier_max_kwh: for tiered rates
  - season: "summer" or "winter" if applicable

IMPORTANT:
- Convert cents/kWh to $/kWh (divide by 100)
- Only include residential and small business/commercial rates
- Skip industrial, large power, and lighting rates
- Include all rate plans: TOU, Tiered, Ultra-Low Overnight, Flat, etc.

Return JSON array of tariff objects. Return ONLY the JSON, no other text.

PAGE CONTENT:
{combined_text[:30000]}"""

    response = client.messages.create(
        model=LLM_MODEL,
        max_tokens=4000,
        messages=[{"role": "user", "content": prompt}],
    )

    try:
        text = response.content[0].text.strip()
        if text.startswith("```"):
            text = re.sub(r"^```\w*\n?", "", text)
            text = re.sub(r"\n?```$", "", text)
        return json.loads(text)
    except (json.JSONDecodeError, IndexError) as e:
        log.error(f"Failed to parse LLM response: {e}")
        return []


def main():
    parser = argparse.ArgumentParser(description="Browser interaction agent for utility rate pages")
    parser.add_argument("--url", type=str, help="URL to scrape")
    parser.add_argument("--utility-id", type=int, help="Utility ID (looks up URL from DB)")
    parser.add_argument("--auto", action="store_true", default=True, help="Auto-discover interactions")
    parser.add_argument("--extract", action="store_true", help="Extract tariffs via LLM")
    parser.add_argument("--store", action="store_true", help="Store extracted tariffs in DB")
    parser.add_argument("--output", type=str, help="Write results to JSON file")
    parser.add_argument("--visible", action="store_true", help="Run browser in visible mode")
    args = parser.parse_args()

    url = args.url
    utility_name = "Unknown"

    if args.utility_id and not url:
        from sqlalchemy import select
        from sqlalchemy.orm import Session
        from app.db.session import get_sync_engine
        from app.models import Utility

        engine = get_sync_engine()
        with Session(engine) as session:
            u = session.execute(
                select(Utility).where(Utility.id == args.utility_id)
            ).scalar_one_or_none()
            if not u:
                log.error(f"Utility {args.utility_id} not found")
                return
            utility_name = u.name
            if u.tariff_page_urls:
                url = list(u.tariff_page_urls.values())[0] if isinstance(u.tariff_page_urls, dict) else u.tariff_page_urls[0]
            elif u.website_url:
                url = u.website_url + "/rates"

    if not url:
        log.error("No URL provided. Use --url or --utility-id")
        return

    log.info(f"Starting browser agent for: {url}")

    with BrowserAgent(headless=not args.visible) as agent:
        snapshots = agent.scrape_interactive_rate_page(url)

    log.info(f"Captured {len(snapshots)} page snapshots")
    for i, snap in enumerate(snapshots):
        log.info(f"  Snapshot {i+1}: {snap.url} — {len(snap.text)} chars — {snap.interactions_performed}")

    tariffs = []
    if args.extract:
        tariffs = extract_tariffs_from_snapshots(snapshots, utility_name)
        log.info(f"Extracted {len(tariffs)} tariffs")
        for t in tariffs:
            log.info(f"  {t.get('name', '?')} ({t.get('customer_class', '?')}) — {len(t.get('components', []))} components")

    if args.store and tariffs and args.utility_id:
        from scripts.tariff_pipeline import store_tariffs, ExtractedTariff
        extracted = []
        for t in tariffs:
            extracted.append(ExtractedTariff(
                name=t.get("name", ""),
                code=t.get("code", ""),
                customer_class=t.get("customer_class", ""),
                rate_type=t.get("rate_type", ""),
                description=t.get("description", ""),
                source_url=url,
                effective_date=t.get("effective_date", ""),
                components=t.get("components", []),
            ))
        count = store_tariffs(args.utility_id, extracted, dry_run=False)
        log.info(f"Stored {count} tariffs for utility {args.utility_id}")

    if args.output:
        output_data = {
            "url": url,
            "snapshots": [
                {
                    "url": s.url,
                    "title": s.title,
                    "text_length": len(s.text),
                    "interactions": s.interactions_performed,
                    "text_preview": s.text[:2000],
                }
                for s in snapshots
            ],
            "tariffs": tariffs,
        }
        with open(args.output, "w") as f:
            json.dump(output_data, f, indent=2)
        log.info(f"Results written to {args.output}")

    # Print summary
    print("\n" + "=" * 60)
    print("BROWSER AGENT SUMMARY")
    print("=" * 60)
    print(f"  URL: {url}")
    print(f"  Snapshots captured: {len(snapshots)}")
    for i, snap in enumerate(snapshots):
        print(f"    {i+1}. {snap.interactions_performed} — {len(snap.text)} chars")
    if tariffs:
        print(f"  Tariffs extracted: {len(tariffs)}")
        for t in tariffs:
            nc = len(t.get("components", []))
            print(f"    - {t.get('name', '?')} ({t.get('customer_class', '?')}) — {nc} components")


if __name__ == "__main__":
    main()
