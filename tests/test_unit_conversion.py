from __future__ import annotations

import pandas as pd
import pytest

from modules.unit_conversion import normalize_units


def test_normalize_units_preserves_co2e() -> None:
    df = pd.DataFrame(
        [
            {
                "category": "scope1",
                "activity": "Fuel",
                "unit": "g",
                "amount": 1000.0,
                "emission_factor": 0.5,
            }
        ]
    )

    normalized, warnings = normalize_units(df)

    assert normalized.iloc[0]["unit"] == "kg"
    assert normalized.iloc[0]["amount"] == pytest.approx(1.0)
    assert normalized.iloc[0]["emission_factor"] == pytest.approx(500.0)
    assert any("Converted units" in text for text in warnings)


def test_normalize_units_unknown_unit_warns() -> None:
    df = pd.DataFrame(
        [
            {
                "category": "scope1",
                "activity": "Fuel",
                "unit": "bbl",
                "amount": 10,
                "emission_factor": 2.0,
            }
        ]
    )

    normalized, warnings = normalize_units(df)

    assert normalized.iloc[0]["unit"] == "bbl"
    assert any("Unknown unit" in text for text in warnings)
