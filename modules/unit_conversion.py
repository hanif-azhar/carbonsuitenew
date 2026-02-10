from __future__ import annotations

from typing import Dict, List, Tuple

import pandas as pd

UNIT_MAP = {
    "l": ("l", 1.0),
    "liter": ("l", 1.0),
    "litre": ("l", 1.0),
    "ml": ("l", 0.001),
    "kg": ("kg", 1.0),
    "g": ("kg", 0.001),
    "ton": ("kg", 907.18474),
    "tonne": ("kg", 1000.0),
    "t": ("kg", 1000.0),
    "kwh": ("kwh", 1.0),
    "mwh": ("kwh", 1000.0),
    "wh": ("kwh", 0.001),
    "mj": ("mj", 1.0),
    "gj": ("mj", 1000.0),
    "km": ("km", 1.0),
    "m": ("km", 0.001),
    "mile": ("km", 1.60934),
    "miles": ("km", 1.60934),
}


def normalize_units(
    df: pd.DataFrame,
    amount_col: str = "amount",
    unit_col: str = "unit",
    ef_col: str = "emission_factor",
) -> Tuple[pd.DataFrame, List[str]]:
    """Convert units to canonical units and adjust emission factors to preserve CO2e."""
    if df is None or df.empty:
        return df.copy(), []

    working = df.copy()
    warnings: List[str] = []

    if amount_col not in working.columns or unit_col not in working.columns:
        return working, ["Unit conversion skipped: missing amount/unit columns."]

    if ef_col not in working.columns:
        working[ef_col] = pd.NA

    working[amount_col] = pd.to_numeric(working[amount_col], errors="coerce")
    working[ef_col] = pd.to_numeric(working[ef_col], errors="coerce")

    converted_units: List[str] = []
    unknown_unit_rows: Dict[str, List[int]] = {}

    for idx, row in working.iterrows():
        raw_unit = str(row[unit_col]).strip().lower()
        mapping = UNIT_MAP.get(raw_unit)

        if mapping is None:
            if raw_unit and raw_unit != "nan":
                display_unit = str(row[unit_col]).strip()
                unknown_unit_rows.setdefault(display_unit, []).append(int(idx) + 1)
            continue

        canonical_unit, multiplier = mapping
        if multiplier <= 0:
            warnings.append(f"Invalid conversion multiplier for '{row[unit_col]}'; kept as-is.")
            continue

        amount = row[amount_col]
        ef = row[ef_col]

        if pd.notna(amount):
            working.at[idx, amount_col] = float(amount) * multiplier
        if pd.notna(ef):
            working.at[idx, ef_col] = float(ef) / multiplier

        working.at[idx, unit_col] = canonical_unit
        if raw_unit != canonical_unit:
            converted_units.append(f"{raw_unit}->{canonical_unit}")

    for unit_label, rows in sorted(unknown_unit_rows.items(), key=lambda item: item[0].lower()):
        row_list = ", ".join(str(row) for row in rows[:6])
        if len(rows) > 6:
            row_list = f"{row_list}, +{len(rows) - 6} more"
        warnings.append(f"Unknown unit '{unit_label}' at row(s) {row_list}; kept as-is.")

    if converted_units:
        unique = sorted(set(converted_units))
        warnings.append(f"Converted units: {', '.join(unique)}")

    return working, warnings


def normalize_factor_units(factors_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize unit labels in emission factor library."""
    if factors_df is None or factors_df.empty:
        return factors_df.copy()

    factors = factors_df.copy()
    if "unit" not in factors.columns:
        return factors

    factors["unit"] = factors["unit"].astype(str).str.strip().str.lower()

    for idx, unit in factors["unit"].items():
        mapping = UNIT_MAP.get(unit)
        if mapping:
            factors.at[idx, "unit"] = mapping[0]

    return factors
