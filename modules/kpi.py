from __future__ import annotations

from typing import Dict, Optional


def compute_intensity_metrics(
    total_co2e: float,
    production_units: Optional[float] = None,
    revenue_usd: Optional[float] = None,
    employees: Optional[float] = None,
) -> Dict[str, float]:
    """Compute emissions intensity KPIs from optional denominators."""
    total = float(total_co2e)
    metrics: Dict[str, float] = {}

    if production_units and production_units > 0:
        metrics["tCO2e_per_unit"] = total / float(production_units)
    if revenue_usd and revenue_usd > 0:
        metrics["tCO2e_per_musd"] = total / (float(revenue_usd) / 1_000_000.0)
    if employees and employees > 0:
        metrics["tCO2e_per_employee"] = total / float(employees)

    return metrics
