from __future__ import annotations

from typing import Dict, Iterable, Optional

import pandas as pd


def build_compliance_tables(
    emissions_result: Dict[str, object],
    factors_df: Optional[pd.DataFrame] = None,
    metadata: Optional[Dict[str, object]] = None,
    assumptions: Optional[Iterable[str]] = None,
    change_log: Optional[Iterable[str]] = None,
    intensity_metrics: Optional[Dict[str, float]] = None,
    data_quality: Optional[Dict[str, object]] = None,
) -> Dict[str, pd.DataFrame]:
    """Build compliance-oriented data tables for export and review."""
    metadata = metadata or {}
    assumptions = list(assumptions or [])
    change_log = list(change_log or [])
    intensity_metrics = intensity_metrics or {}
    data_quality = data_quality or {}

    scope_df = emissions_result.get("scope_df")
    total_co2e = float(emissions_result.get("total_co2e") or 0.0)

    if isinstance(scope_df, pd.DataFrame) and not scope_df.empty:
        ghg_scope_table = scope_df.copy()
        ghg_scope_table["reporting_standard"] = metadata.get("reporting_standard", "GHG Protocol")
        ghg_scope_table["reporting_year"] = metadata.get("reporting_year", "")
        ghg_scope_table["organization"] = metadata.get("organization", "")
    else:
        ghg_scope_table = pd.DataFrame(
            [
                {
                    "category": "total",
                    "total_co2e": total_co2e,
                    "reporting_standard": metadata.get("reporting_standard", "GHG Protocol"),
                    "reporting_year": metadata.get("reporting_year", ""),
                    "organization": metadata.get("organization", ""),
                }
            ]
        )

    factor_table = pd.DataFrame()
    if isinstance(factors_df, pd.DataFrame) and not factors_df.empty:
        columns = [
            col
            for col in [
                "activity",
                "unit",
                "emission_factor",
                "scope",
                "scope_category",
                "region",
                "year",
                "source",
                "version",
                "active",
            ]
            if col in factors_df.columns
        ]
        factor_table = factors_df[columns].copy()

    assumptions_df = pd.DataFrame({"assumption": assumptions}) if assumptions else pd.DataFrame(
        {"assumption": ["No explicit assumptions provided"]}
    )

    change_log_df = pd.DataFrame({"change": change_log}) if change_log else pd.DataFrame(
        {"change": ["Initial report generation"]}
    )

    intensity_df = pd.DataFrame(
        [{"metric": key, "value": value} for key, value in intensity_metrics.items()]
    )

    data_quality_df = pd.DataFrame(
        [
            {
                "score": data_quality.get("score", None),
                "row_count": data_quality.get("row_count", None),
                "issue_count": len(data_quality.get("issue_counts", {})),
            }
        ]
    )

    return {
        "GHG Scope Table": ghg_scope_table,
        "Emission Factor Provenance": factor_table,
        "Assumptions": assumptions_df,
        "Change Log": change_log_df,
        "Intensity KPI": intensity_df,
        "Data Quality": data_quality_df,
    }
