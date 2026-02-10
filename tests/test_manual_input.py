from __future__ import annotations

import pytest

from modules.manual_input import build_manual_dataframe, validate_manual_inputs


def test_validate_manual_inputs_zero_values() -> None:
    valid, message = validate_manual_inputs({"fuel": 0, "electricity": 0, "transport": 0, "waste": 0})

    assert not valid
    assert "at least one" in message.lower()


def test_validate_manual_inputs_negative_value() -> None:
    valid, message = validate_manual_inputs({"fuel": -1, "electricity": 0, "transport": 0, "waste": 0})

    assert not valid
    assert "non-negative" in message.lower()


def test_build_manual_dataframe_rejects_bad_renewable_fraction() -> None:
    with pytest.raises(ValueError):
        build_manual_dataframe(
            fuel_amount=1,
            fuel_unit="L",
            electricity_kwh=100,
            renewable_fraction=1.2,
            transport_km=10,
            waste_kg=5,
        )


def test_build_manual_dataframe_filters_zero_rows() -> None:
    df = build_manual_dataframe(
        fuel_amount=5,
        fuel_unit="L",
        electricity_kwh=0,
        renewable_fraction=0,
        transport_km=0,
        waste_kg=0,
    )

    assert len(df) == 1
    assert df.iloc[0]["activity"] == "Fuel combustion"
