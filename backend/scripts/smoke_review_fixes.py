"""Post-deploy smoke test for the code-review fix campaign (P0-P3).

Run inside the api or celery-worker container:
    python -m scripts.smoke_review_fixes
"""
import sys


def main() -> int:
    import scripts.tariff_pipeline as tp
    import app.tasks.refresh as rf  # noqa: F401
    import scripts.supersede_via_llm  # noqa: F401
    import scripts.triage_seeds as ts
    import scripts.dedup_tariffs  # noqa: F401
    import scripts.cleanup_duplicate_tariffs  # noqa: F401
    import scripts.quality_cleanup  # noqa: F401
    import scripts.run_campaign  # noqa: F401
    print("imports OK")

    # Matcher: letter-prefix code match still pairs
    assert tp.tariffs_likely_same(
        "SC1C [NYC Zone J]", "Service Classification No. 1C Time of Use"
    ), "sc1c pairing broke"
    # Matcher: trailing-letter variants must NOT merge
    assert not tp.tariffs_likely_same(
        "SC-1 Residential", "SC-1C Residential TOU"
    ), "sc1 vs sc1c false merge"
    # Matcher: conflicting discriminators block the code fast path
    assert not tp.tariffs_likely_same(
        "Rate D1 TOU", "Rate D1 Flat"
    ), "discriminator conflict merged"
    print("matcher OK")

    # Unit normalization: cents unit converts deterministically
    t = tp.ExtractedTariff(
        name="X",
        components=[{"component_type": "energy", "unit": "cents/kWh", "rate_value": 12.5}],
    )
    notes = tp._normalize_component_units(t, 0.45)
    assert notes and abs(t.components[0]["rate_value"] - 0.125) < 1e-9, "cents conversion broke"
    assert t.components[0]["unit"] == "$/kWh"
    # Mislabel rescue: absurd $/kWh value lands back in band
    t2 = tp.ExtractedTariff(
        name="Y",
        components=[{"component_type": "energy", "unit": "$/kWh", "rate_value": 14.2}],
    )
    tp._normalize_component_units(t2, 0.45)
    assert abs(t2.components[0]["rate_value"] - 0.142) < 1e-9, "mislabel rescue broke"
    # Legit CPP rate below hard-reject line untouched
    t3 = tp.ExtractedTariff(
        name="Z",
        components=[{"component_type": "energy", "unit": "$/kWh", "rate_value": 0.95}],
    )
    tp._normalize_component_units(t3, 0.45)
    assert abs(t3.components[0]["rate_value"] - 0.95) < 1e-9, "CPP rate wrongly converted"
    print("unit normalization OK")

    # Triage: product words preserved
    assert ts.normalize_name("Optional Residential Service") != ts.normalize_name(
        "Residential Service"
    ), "triage normalize still collapses optional/standard"
    print("triage normalize OK")

    # Phase 6 JSON recovery
    report = (
        'Intro text\n```json\n[{"name": "Example"}]\n```\nfinal answer:\n'
        '```json\n[{"name": "Residential Service", "customer_class": "residential", '
        '"components": [{"component_type": "energy", "rate_value": 0.12,},],},]\n```'
    )
    raw = tp._phase6_extract_json(report)
    assert raw and raw[0]["name"] == "Residential Service", "phase6 JSON recovery broke"
    print("phase6 JSON recovery OK")

    # Job A: vintage product match (NF Rate #1.1 vs Flat sibling)
    assert tp.same_vintage_product(
        "Rate #1.1 Domestic Service",
        "Domestic Service (Flat)",
        code_a="1.1",
        rate_type_a="flat",
        rate_type_b="flat",
    ), "NF vintage match broke"
    # Job A: must NOT widen tariffs_likely_same for TOU vs Flat
    assert not tp.tariffs_likely_same(
        "Rate D1 TOU", "Rate D1 Flat"
    ), "vintage work widened likely_same TOU vs Flat"
    print("vintage match OK")

    # Job B: fixed/minimum amp-tier dedupe
    deduped = tp.dedupe_rate_components([
        {"component_type": "fixed", "unit": "$/month", "rate_value": 17.36,
         "tier_label": "0-10 Amp"},
        {"component_type": "minimum", "unit": "$/month", "rate_value": 17.36,
         "tier_label": "Basic Customer Charge (0-10 Amp)"},
        {"component_type": "energy", "unit": "$/kWh", "rate_value": 0.15587},
    ])
    assert len(deduped) == 2, f"component dedupe broke: {deduped}"
    assert deduped[0]["component_type"] == "fixed"
    print("component dedupe OK")

    print("ALL SMOKE CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
