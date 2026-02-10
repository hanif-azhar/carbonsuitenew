from __future__ import annotations

import pandas as pd

from modules.scenario import run_reduction_scenario


def _raw_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "category": "scope1",
                "activity": "Fuel combustion",
                "unit": "l",
                "amount": 100,
                "emission_factor": 2.0,
                "ch4": 0.0,
                "n2o": 0.0,
            },
            {
                "category": "scope2",
                "activity": "Electricity consumption",
                "unit": "kwh",
                "amount": 100,
                "emission_factor": 1.0,
                "ch4": 0.0,
                "n2o": 0.0,
            },
        ]
    )


def test_run_reduction_scenario_scope_reduction() -> None:
    result = run_reduction_scenario(_raw_df(), scope_reduction_pct={"scope1": 50})

    assert result["scenario_total"] < result["baseline_total"]
    assert result["abatement"] > 0


def test_run_reduction_scenario_target_flag() -> None:
    result = run_reduction_scenario(_raw_df(), scope_reduction_pct={"scope1": 50, "scope2": 50}, target_total=160)

    assert result["meets_target"] is True
