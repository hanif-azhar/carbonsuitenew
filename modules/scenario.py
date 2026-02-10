from __future__ import annotations

from typing import Dict, Optional

import pandas as pd

from modules.emissions import calculate_emissions


def _safe_pct(value: float) -> float:
    return max(0.0, min(100.0, float(value)))


def run_reduction_scenario(
    raw_df: pd.DataFrame,
    factors_df: Optional[pd.DataFrame] = None,
    scope_reduction_pct: Optional[Dict[str, float]] = None,
    activity_reduction_pct: Optional[Dict[str, float]] = None,
    target_total: Optional[float] = None,
) -> Dict[str, object]:
    """Apply reduction assumptions and compare scenario emissions against baseline."""
    if raw_df is None or raw_df.empty:
        raise ValueError("Scenario requires non-empty raw activity data.")

    baseline = calculate_emissions(raw_df, factors_df=factors_df)
    scenario_df = raw_df.copy()

    scope_reduction_pct = scope_reduction_pct or {}
    activity_reduction_pct = activity_reduction_pct or {}

    scenario_df["category"] = scenario_df["category"].astype(str).str.lower().str.replace(" ", "", regex=False)
    scenario_df["activity"] = scenario_df["activity"].astype(str).str.strip().str.lower()

    for scope, reduction in scope_reduction_pct.items():
        reduction = _safe_pct(reduction)
        mask = scenario_df["category"] == scope.strip().lower().replace(" ", "")
        scenario_df.loc[mask, "amount"] = pd.to_numeric(scenario_df.loc[mask, "amount"], errors="coerce") * (1 - reduction / 100.0)

    for activity, reduction in activity_reduction_pct.items():
        reduction = _safe_pct(reduction)
        mask = scenario_df["activity"] == activity.strip().lower()
        scenario_df.loc[mask, "amount"] = pd.to_numeric(scenario_df.loc[mask, "amount"], errors="coerce") * (1 - reduction / 100.0)

    scenario = calculate_emissions(scenario_df, factors_df=factors_df)

    baseline_total = float(baseline["total_co2e"])
    scenario_total = float(scenario["total_co2e"])
    abatement = baseline_total - scenario_total
    abatement_pct = (abatement / baseline_total * 100.0) if baseline_total else 0.0

    compare_df = pd.DataFrame(
        {
            "metric": ["Baseline Total", "Scenario Total", "Abatement"],
            "tCO2e": [baseline_total, scenario_total, abatement],
        }
    )

    meets_target = None
    if target_total is not None:
        meets_target = scenario_total <= float(target_total)

    return {
        "baseline": baseline,
        "scenario": scenario,
        "baseline_total": baseline_total,
        "scenario_total": scenario_total,
        "abatement": abatement,
        "abatement_pct": abatement_pct,
        "meets_target": meets_target,
        "target_total": target_total,
        "comparison_df": compare_df,
        "scenario_df": scenario_df,
    }
