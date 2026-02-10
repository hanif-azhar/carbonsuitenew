from __future__ import annotations

from io import BytesIO
from typing import Dict, Iterable

import pandas as pd

STANDARD_COLUMNS = [
    "category",
    "activity",
    "unit",
    "amount",
    "emission_factor",
    "source",
    "ch4",
    "n2o",
]

REQUIRED_COLUMNS = {"category", "activity", "unit", "amount", "emission_factor"}

COLUMN_ALIASES = {
    "category": {"scope", "scope_tag", "emission_scope", "category_tag"},
    "activity": {"activity_name", "activity_type", "item", "description", "process"},
    "unit": {"uom", "units", "measurement_unit"},
    "amount": {"value", "quantity", "activity_amount", "consumption"},
    "emission_factor": {"ef", "factor", "co2_factor", "co2e_factor"},
    "source": {"data_source", "reference", "origin"},
    "ch4": {"ch4_factor", "methane", "methane_factor"},
    "n2o": {"n2o_factor", "nitrous_oxide", "nitrous_oxide_factor"},
}


def _normalize_column(name: str) -> str:
    return name.strip().lower().replace(" ", "_").replace("-", "_")


def _standardize_columns(columns: Iterable[str]) -> Dict[str, str]:
    mapped: Dict[str, str] = {}
    for original in columns:
        normalized = _normalize_column(str(original))
        for standard_name, aliases in COLUMN_ALIASES.items():
            if normalized == standard_name or normalized in aliases:
                mapped[str(original)] = standard_name
                break
    return mapped


def _coerce_numeric(df: pd.DataFrame, column: str) -> None:
    if column in df.columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")


def parse_excel(file: BytesIO) -> pd.DataFrame:
    """Parse uploaded workbook and return a cleaned standard emissions dataframe."""
    if file is None:
        raise ValueError("No file uploaded. Please upload a .xlsx file.")

    try:
        sheets = pd.read_excel(file, sheet_name=None)
    except Exception as exc:  # pragma: no cover - error text comes from pandas/engine
        raise ValueError("Unable to read Excel file. Ensure it is a valid .xlsx workbook.") from exc

    if not sheets:
        raise ValueError("The workbook has no sheets.")

    cleaned_sheets = []
    errors = []

    for sheet_name, sheet_df in sheets.items():
        if sheet_df.empty:
            continue

        df = sheet_df.copy()
        df.columns = [_normalize_column(str(col)) for col in df.columns]

        rename_map = _standardize_columns(df.columns)
        df = df.rename(columns=rename_map)

        missing = REQUIRED_COLUMNS - set(df.columns)
        if missing:
            errors.append(f"Sheet '{sheet_name}' missing columns: {', '.join(sorted(missing))}")
            continue

        for column in STANDARD_COLUMNS:
            if column not in df.columns:
                df[column] = pd.NA

        df = df[STANDARD_COLUMNS]
        df = df.dropna(how="all")
        df["source"] = df["source"].fillna(sheet_name)

        for text_col in ["category", "activity", "unit", "source"]:
            df[text_col] = df[text_col].astype("string").str.strip()

        for numeric_col in ["amount", "emission_factor", "ch4", "n2o"]:
            _coerce_numeric(df, numeric_col)

        invalid_numeric = df[df["amount"].isna() | df["emission_factor"].isna()]
        if not invalid_numeric.empty:
            errors.append(
                f"Sheet '{sheet_name}' has {len(invalid_numeric)} row(s) with non-numeric amount/emission_factor."
            )
            df = df.drop(index=invalid_numeric.index)

        df = df.dropna(subset=["category", "activity", "unit"])

        if not df.empty:
            cleaned_sheets.append(df)

    if not cleaned_sheets:
        if errors:
            raise ValueError("; ".join(errors))
        raise ValueError("No valid data found in workbook.")

    combined = pd.concat(cleaned_sheets, ignore_index=True)
    combined["category"] = (
        combined["category"].astype(str).str.lower().str.replace(" ", "", regex=False)
    )

    return combined
