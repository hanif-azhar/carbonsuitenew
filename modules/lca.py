from __future__ import annotations

from typing import Dict, Optional

import pandas as pd

STAGES = ["Materials", "Transport", "Processing", "Distribution", "End-Of-Life"]
BOUNDARY_PRESETS = {
    "cradle-to-grave": ["Materials", "Transport", "Processing", "Distribution", "End-Of-Life"],
    "cradle-to-gate": ["Materials", "Transport", "Processing"],
    "gate-to-gate": ["Processing"],
}


def _to_dataframe(model_inputs) -> pd.DataFrame:
    if isinstance(model_inputs, pd.DataFrame):
        return model_inputs.copy()

    if isinstance(model_inputs, dict):
        if "inventory" in model_inputs:
            return pd.DataFrame(model_inputs["inventory"])
        return pd.DataFrame([model_inputs])

    if isinstance(model_inputs, list):
        return pd.DataFrame(model_inputs)

    raise ValueError("model_inputs must be a DataFrame, list, or dict.")


def run_lca(
    model_inputs,
    boundary: str = "cradle-to-grave",
    allocation_method: str = "none",
    default_allocation_factor: float = 1.0,
    stage_allocation: Optional[Dict[str, float]] = None,
    sensitivity_pct: float = 10.0,
) -> Dict[str, object]:
    """Run an LCA model with boundary presets, allocation, and sensitivity analysis."""
    df = _to_dataframe(model_inputs)

    required = {"stage", "amount", "emission_factor"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"LCA input missing columns: {', '.join(sorted(missing))}")

    df = df.copy()
    df["stage"] = (
        df["stage"]
        .astype(str)
        .str.strip()
        .str.title()
        .str.replace("-of-", "-Of-", regex=False)
    )
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["emission_factor"] = pd.to_numeric(df["emission_factor"], errors="coerce")

    df = df.dropna(subset=["stage", "amount", "emission_factor"])
    df = df[df["amount"] >= 0]

    if df.empty:
        raise ValueError("No valid LCA rows remain after validation.")

    preset = BOUNDARY_PRESETS.get(boundary.strip().lower())
    if preset:
        df = df[df["stage"].isin(preset)]

    if df.empty:
        raise ValueError("No rows remain after applying selected boundary.")

    allocation_method = allocation_method.strip().lower()
    stage_allocation = stage_allocation or {}
    default_allocation_factor = max(0.0, min(1.0, float(default_allocation_factor)))

    df["allocation_factor"] = default_allocation_factor
    for stage, factor in stage_allocation.items():
        mask = df["stage"].str.lower() == str(stage).strip().lower()
        df.loc[mask, "allocation_factor"] = max(0.0, min(1.0, float(factor)))

    if "allocation_factor" in df.columns and allocation_method != "none":
        df["adjusted_emission_factor"] = df["emission_factor"] * df["allocation_factor"]
    else:
        df["adjusted_emission_factor"] = df["emission_factor"]

    # Maintain expected stage ordering for known stages while preserving custom stages.
    ordered_stages = STAGES + [stage for stage in df["stage"].unique() if stage not in STAGES]
    df["stage"] = pd.Categorical(df["stage"], ordered_stages, ordered=True)

    df["stage_emissions"] = df["amount"] * df["adjusted_emission_factor"]

    summary_df = (
        df.groupby("stage", as_index=False, observed=True)
        .agg(
            total_amount=("amount", "sum"),
            total_emissions=("stage_emissions", "sum"),
            avg_allocation_factor=("allocation_factor", "mean"),
        )
        .sort_values("stage")
    )

    total_emissions = float(summary_df["total_emissions"].sum())

    hotspots = (
        summary_df.sort_values("total_emissions", ascending=False)
        .head(3)[["stage", "total_emissions"]]
        .to_dict(orient="records")
    )

    sensitivity_pct = max(0.0, float(sensitivity_pct))
    low_multiplier = 1 - (sensitivity_pct / 100.0)
    high_multiplier = 1 + (sensitivity_pct / 100.0)

    low_total = float((df["amount"] * df["adjusted_emission_factor"] * low_multiplier).sum())
    high_total = float((df["amount"] * df["adjusted_emission_factor"] * high_multiplier).sum())

    sankey_df = pd.DataFrame(
        {
            "source": ["System Boundary"] * len(summary_df),
            "target": summary_df["stage"].astype(str),
            "value": summary_df["total_emissions"].astype(float),
        }
    )

    by_stage = {row["stage"]: float(row["total_emissions"]) for _, row in summary_df.iterrows()}

    return {
        "boundary": boundary,
        "allocation_method": allocation_method,
        "total_emissions": total_emissions,
        "hotspot_categories": hotspots,
        "by_stage": by_stage,
        "summary_df": summary_df,
        "sankey_df": sankey_df,
        "sensitivity": {
            "pct": sensitivity_pct,
            "low_total": low_total,
            "high_total": high_total,
        },
    }
