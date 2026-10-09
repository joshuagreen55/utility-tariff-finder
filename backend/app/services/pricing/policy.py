"""Locked product policy for the pricing compiler (Joshua, Oct 9 2026).

Texas competitive (ERCOT wires-only / no default supply)
    Store TDU delivery charges. Mark supply ``choose_a_retailer``.
    Never invent a supply price. Never publish an all-in $/kWh.

Ontario Electricity Rebate (OER)
    Percentage credit on the whole bill — not a per-kWh rate.
    Leave it out of the compiled per-kWh price; record as a bill-level
    note, same class as taxes / franchise fees.
"""
from __future__ import annotations

# Recipe code for ERCOT competitive / other markets with no default supply.
TEXAS_TDU_RECIPE = "texas_tdu"

SUPPLY_STATUS_CHOOSE_RETAILER = "choose_a_retailer"

# Bill-level notes attached to every Ontario compiled plan.
OER_BILL_LEVEL_NOTE = (
    "Ontario Electricity Rebate (OER): percentage credit on the whole bill; "
    "not included in the per-kWh price (treated like taxes)."
)

# Component codes / name fragments that must never enter the per-kWh sum.
OER_FORBIDDEN_CODES = frozenset({
    "oer",
    "ontario_electricity_rebate",
    "ontario-electricity-rebate",
})
OER_FORBIDDEN_NAME_FRAGMENTS = (
    "ontario electricity rebate",
    "o.e.r.",
)
