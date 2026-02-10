from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from modules.unit_conversion import normalize_factor_units, normalize_units

CO2E_CH4_GWP = 28.0
CO2E_N2O_GWP = 265.0
REQUIRED_COLUMNS = {"category", "activity", "unit", "amount", "emission_factor"}

SCOPE_MAP = {
    "scope1": "scope1",
    "scope_1": "scope1",
    "scope 1": "scope1",
    "s1": "scope1",
    "scope2": "scope2",
    "scope_2": "scope2",
    "scope 2": "scope2",
    "s2": "scope2",
    "scope3": "scope3",
    "scope_3": "scope3",
    "scope 3": "scope3",
    "s3": "scope3",
}


def load_emission_factors(csv_path: str | Path) -> pd.DataFrame:
    """Load emission factors from CSV with optional metadata columns."""
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"Emission factor file not found: {path}")

    factors = pd.read_csv(path)
    required = {"activity", "unit", "emission_factor"}
    missing = required - set(factors.columns)
    if missing:
        raise ValueError(f"Emission factor file missing columns: {', '.join(sorted(missing))}")

    for col, default in [
        ("scope", ""),
        ("scope_category", ""),
        ("region", "global"),
        ("year", pd.NA),
        ("source", "unspecified"),
        ("version", "v1"),
        ("active", 1),
    ]:
        if col not in factors.columns:
            factors[col] = default

    factors["activity"] = factors["activity"].astype(str).str.strip().str.lower()
    factors["unit"] = factors["unit"].astype(str).str.strip().str.lower()
    factors["emission_factor"] = pd.to_numeric(factors["emission_factor"], errors="coerce")
    factors["year"] = pd.to_numeric(factors["year"], errors="coerce")
    factors["active"] = pd.to_numeric(factors["active"], errors="coerce").fillna(1).astype(int)

    factors = factors[factors["active"] == 1].dropna(subset=["emission_factor"])
    return normalize_factor_units(factors)


def _normalize_scope(value: str) -> str:
    normalized = str(value).strip().lower()
    return SCOPE_MAP.get(normalized, normalized)


def apply_emission_factors(
    df: pd.DataFrame,
    factors_df: pd.DataFrame,
    region: Optional[str] = None,
    year: Optional[int] = None,
) -> pd.DataFrame:
    """Fill missing emission factors using activity+unit and metadata-aware fallback."""
    working = df.copy()
    factors = factors_df.copy()

    working["activity_key"] = working["activity"].astype(str).str.strip().str.lower()
    working["unit_key"] = working["unit"].astype(str).str.strip().str.lower()

    if "region" not in factors.columns:
        factors["region"] = "global"
    if "year" not in factors.columns:
        factors["year"] = pd.NA
    if "source" not in factors.columns:
        factors["source"] = "unspecified"
    if "version" not in factors.columns:
        factors["version"] = "v1"

    factors["activity"] = factors["activity"].astype(str).str.strip().str.lower()
    factors["unit"] = factors["unit"].astype(str).str.strip().str.lower()

    if region:
        region = region.strip().lower()
        factors = factors[(factors["region"].astype(str).str.lower() == region) | (factors["region"].astype(str).str.lower() == "global")]

    if year is not None:
        factors["year_numeric"] = pd.to_numeric(factors["year"], errors="coerce")
        factors = factors[(factors["year_numeric"].isna()) | (factors["year_numeric"] == int(year))]
    else:
        factors["year_numeric"] = pd.to_numeric(factors["year"], errors="coerce")

    factors = factors.sort_values(["activity", "unit", "year_numeric"])
    factors = factors.drop_duplicates(subset=["activity", "unit"], keep="last")

    factors = factors.rename(
        columns={
            "activity": "activity_key",
            "unit": "unit_key",
            "emission_factor": "emission_factor_lookup",
            "source": "factor_source",
            "version": "factor_version",
            "region": "factor_region",
            "year": "factor_year",
        }
    )

    merged = working.merge(
        factors[
            [
                "activity_key",
                "unit_key",
                "emission_factor_lookup",
                "factor_source",
                "factor_version",
                "factor_region",
                "factor_year",
            ]
        ],
        on=["activity_key", "unit_key"],
        how="left",
    )

    merged["emission_factor"] = pd.to_numeric(merged["emission_factor"], errors="coerce")
    merged["emission_factor"] = merged["emission_factor"].fillna(merged["emission_factor_lookup"])
    merged["factor_source"] = merged["factor_source"].fillna("user_input")
    merged["factor_version"] = merged["factor_version"].fillna("n/a")
    merged["factor_region"] = merged["factor_region"].fillna("n/a")

    return merged.drop(columns=["activity_key", "unit_key", "emission_factor_lookup"])


def calculate_emissions(
    df: pd.DataFrame,
    factors_df: Optional[pd.DataFrame] = None,
    region: Optional[str] = None,
    year: Optional[int] = None,
) -> Dict[str, object]:
    """Compute CO2e totals and scope breakdown with conversions and provenance."""
    if df is None or df.empty:
        raise ValueError("Input data is empty. Please provide activity records.")

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Input dataframe missing columns: {', '.join(sorted(missing))}")

    working = df.copy()

    for column in ["amount", "emission_factor", "ch4", "n2o"]:
        if column not in working.columns:
            working[column] = 0.0
        working[column] = pd.to_numeric(working[column], errors="coerce")

    working, conversion_warnings = normalize_units(working)

    if factors_df is not None and not factors_df.empty:
        factors_df = normalize_factor_units(factors_df)
        working = apply_emission_factors(working, factors_df, region=region, year=year)
    else:
        for col, default in [
            ("factor_source", "user_input"),
            ("factor_version", "n/a"),
            ("factor_region", "n/a"),
            ("factor_year", pd.NA),
        ]:
            if col not in working.columns:
                working[col] = default

    working = working.dropna(subset=["amount", "emission_factor"])
    if working.empty:
        raise ValueError("No valid rows remain after numeric validation.")

    working["category"] = working["category"].apply(_normalize_scope)

    working["co2e_co2"] = working["amount"] * working["emission_factor"]
    working["co2e_ch4"] = working["amount"] * working["ch4"].fillna(0.0) * CO2E_CH4_GWP
    working["co2e_n2o"] = working["amount"] * working["n2o"].fillna(0.0) * CO2E_N2O_GWP
    working["total_co2e"] = working["co2e_co2"] + working["co2e_ch4"] + working["co2e_n2o"]

    summary_df = (
        working.groupby(["category", "activity"], as_index=False)
        .agg(amount=("amount", "sum"), total_co2e=("total_co2e", "sum"))
        .sort_values("total_co2e", ascending=False)
    )

    scope_df = (
        working.groupby("category", as_index=False)
        .agg(total_co2e=("total_co2e", "sum"))
        .sort_values("category")
    )

    factor_provenance_df = (
        working.groupby(["activity", "unit", "factor_source", "factor_version", "factor_region"], as_index=False)
        .agg(emission_factor=("emission_factor", "mean"), rows=("activity", "size"))
        .sort_values(["activity", "unit"])
    )

    by_scope = {row["category"]: float(row["total_co2e"]) for _, row in scope_df.iterrows()}
    total_co2e = float(working["total_co2e"].sum())

    return {
        "summary_df": summary_df,
        "scope_df": scope_df,
        "detailed_df": working,
        "factor_provenance_df": factor_provenance_df,
        "conversion_warnings": conversion_warnings,
        "total_co2e": total_co2e,
        "by_scope": by_scope,
    }
