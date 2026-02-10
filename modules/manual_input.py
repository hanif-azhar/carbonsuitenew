from __future__ import annotations

from typing import Dict, Tuple

import pandas as pd


def validate_manual_inputs(entries: Dict[str, float]) -> Tuple[bool, str]:
    """Validate manual UI inputs before building emissions dataframe."""
    if any(value < 0 for value in entries.values()):
        return False, "Inputs must be non-negative values."

    if all(value == 0 for value in entries.values()):
        return False, "Enter at least one activity amount greater than zero."

    return True, ""


def build_manual_dataframe(
    fuel_amount: float,
    fuel_unit: str,
    electricity_kwh: float,
    renewable_fraction: float,
    transport_km: float,
    waste_kg: float,
    fuel_ef: float = 2.68,
    electricity_ef: float = 0.4,
    transport_ef: float = 0.12,
    waste_ef: float = 0.45,
) -> pd.DataFrame:
    """Convert manual form values into standardized emissions rows."""
    if not 0 <= renewable_fraction <= 1:
        raise ValueError("renewable_fraction must be between 0 and 1.")

    electricity_non_renewable = electricity_kwh * (1 - renewable_fraction)

    rows = [
        {
            "category": "scope1",
            "activity": "Fuel combustion",
            "unit": fuel_unit,
            "amount": fuel_amount,
            "emission_factor": fuel_ef,
            "source": "manual_input",
            "ch4": 0.0,
            "n2o": 0.0,
        },
        {
            "category": "scope2",
            "activity": "Electricity consumption",
            "unit": "kWh",
            "amount": electricity_non_renewable,
            "emission_factor": electricity_ef,
            "source": "manual_input",
            "ch4": 0.0,
            "n2o": 0.0,
        },
        {
            "category": "scope3",
            "activity": "Transportation",
            "unit": "km",
            "amount": transport_km,
            "emission_factor": transport_ef,
            "source": "manual_input",
            "ch4": 0.0,
            "n2o": 0.0,
        },
        {
            "category": "scope3",
            "activity": "Waste",
            "unit": "kg",
            "amount": waste_kg,
            "emission_factor": waste_ef,
            "source": "manual_input",
            "ch4": 0.0,
            "n2o": 0.0,
        },
    ]

    df = pd.DataFrame(rows)
    return df[df["amount"] > 0].reset_index(drop=True)
