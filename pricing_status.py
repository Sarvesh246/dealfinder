"""
Shared source/product pricing status helpers.
"""

from __future__ import annotations


def status_for_price(price, target_price, alert_mode: str = "target_threshold") -> str:
    if price is None:
        return "watching"
    if alert_mode == "any_drop" or target_price is None:
        return "watching"
    return "deal_found" if float(price) <= float(target_price) else "watching"
