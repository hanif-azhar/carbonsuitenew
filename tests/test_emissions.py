from __future__ import annotations

import pandas as pd
import pytest

from modules.emissions import calculate_emissions


def test_calculate_emissions_basic_with_ch4_n2o() -> None:
    df = pd.DataFrame(
        [
            {
                "category": "scope1",
                "activity": "Diesel use",
                "unit": "L",
                "amount": 10,
                "emission_factor": 2.0,
                "ch4": 0.01,
                "n2o": 0.001,
            }
        ]
    )

    result = calculate_emissions(df)

    expected = 10 * 2.0 + 10 * 0.01 * 28.0 + 10 * 0.001 * 265.0
    assert result["total_co2e"] == pytest.approx(expected)
    assert result["by_scope"]["scope1"] == pytest.approx(expected)


def test_calculate_emissions_applies_factor_lookup() -> None:
    df = pd.DataFrame(
        [
            {
                "category": "scope2",
                "activity": "Electricity consumption",
                "unit": "kWh",
                "amount": 100,
                "emission_factor": None,
            }
        ]
    )

    factors = pd.DataFrame(
        [{"activity": "electricity consumption", "unit": "kwh", "emission_factor": 0.4}]
    )

    result = calculate_emissions(df, factors_df=factors)

    assert result["total_co2e"] == pytest.approx(40.0)


def test_calculate_emissions_rejects_missing_columns() -> None:
    df = pd.DataFrame([{"activity": "x", "amount": 1}])

    with pytest.raises(ValueError):
        calculate_emissions(df)
