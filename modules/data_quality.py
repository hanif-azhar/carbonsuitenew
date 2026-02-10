from __future__ import annotations

from typing import Dict, List

import pandas as pd


REQUIRED_COLUMNS = ["category", "activity", "unit", "amount", "emission_factor"]


def assess_data_quality(df: pd.DataFrame) -> Dict[str, object]:
    """Assess data integrity and return quality score plus issue details."""
    if df is None or df.empty:
        return {
            "score": 0.0,
            "row_count": 0,
            "issue_counts": {"empty_dataset": 1},
            "issues_df": pd.DataFrame([{"issue": "Empty dataset", "count": 1}]),
        }

    working = df.copy()
    issues: List[Dict[str, object]] = []

    for col in ["amount", "emission_factor"]:
        if col in working.columns:
            working[col] = pd.to_numeric(working[col], errors="coerce")

    row_count = len(working)
    score = 100.0

    missing_required = working[REQUIRED_COLUMNS].isna().sum().sum() if set(REQUIRED_COLUMNS).issubset(working.columns) else row_count
    if missing_required:
        issues.append({"issue": "Missing required values", "count": int(missing_required)})
        score -= min(25.0, missing_required * 1.2)

    non_numeric_amount = int(working["amount"].isna().sum()) if "amount" in working.columns else row_count
    if non_numeric_amount:
        issues.append({"issue": "Non-numeric amount", "count": non_numeric_amount})
        score -= min(20.0, non_numeric_amount * 2.0)

    non_numeric_ef = int(working["emission_factor"].isna().sum()) if "emission_factor" in working.columns else row_count
    if non_numeric_ef:
        issues.append({"issue": "Non-numeric emission_factor", "count": non_numeric_ef})
        score -= min(20.0, non_numeric_ef * 2.0)

    duplicate_count = int(working.duplicated().sum())
    if duplicate_count:
        issues.append({"issue": "Duplicate rows", "count": duplicate_count})
        score -= min(15.0, duplicate_count * 1.5)

    negative_amount = int((working.get("amount", pd.Series(dtype=float)) < 0).sum())
    if negative_amount:
        issues.append({"issue": "Negative amount values", "count": negative_amount})
        score -= min(15.0, negative_amount * 3.0)

    outlier_count = 0
    for col in ["amount", "emission_factor"]:
        if col not in working.columns:
            continue
        series = working[col].dropna()
        if len(series) < 4:
            continue
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        if iqr <= 0:
            continue
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        outlier_count += int(((working[col] < lower) | (working[col] > upper)).sum())

    if outlier_count:
        issues.append({"issue": "Potential outliers", "count": outlier_count})
        score -= min(10.0, outlier_count * 1.0)

    score = max(0.0, round(score, 2))
    issues_df = pd.DataFrame(issues) if issues else pd.DataFrame([{"issue": "No issues detected", "count": 0}])

    return {
        "score": score,
        "row_count": int(row_count),
        "issue_counts": {row["issue"]: int(row["count"]) for _, row in issues_df.iterrows()},
        "issues_df": issues_df,
    }
