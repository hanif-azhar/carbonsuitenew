from __future__ import annotations

from io import BytesIO
from typing import Dict, Optional

import pandas as pd
from openpyxl.styles import Font


def _style_sheet(writer: pd.ExcelWriter, sheet_name: str) -> None:
    ws = writer.book[sheet_name]

    for cell in ws[1]:
        cell.font = Font(bold=True)

    for row in ws.iter_rows(min_row=2):
        for cell in row:
            if isinstance(cell.value, (int, float)):
                cell.number_format = "0.000"


def _clean_sheet_name(name: str) -> str:
    invalid = ["[", "]", "*", "?", "/", "\\", ":"]
    cleaned = name
    for token in invalid:
        cleaned = cleaned.replace(token, " ")
    return cleaned.strip()[:31] or "Sheet"


def export_excel(
    summary_df: pd.DataFrame,
    raw_df: pd.DataFrame,
    scope_df: Optional[pd.DataFrame] = None,
    compliance_tables: Optional[Dict[str, pd.DataFrame]] = None,
) -> BytesIO:
    """Export raw data, summaries, and optional compliance tables to Excel."""
    buffer = BytesIO()

    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        raw_df.to_excel(writer, sheet_name="Raw Input Data", index=False)
        summary_df.to_excel(writer, sheet_name="Emissions Summary", index=False)

        if scope_df is None:
            scope_df = (
                summary_df.groupby("category", as_index=False)
                .agg(total_co2e=("total_co2e", "sum"))
                .sort_values("category")
            )
        scope_df.to_excel(writer, sheet_name="Scope Breakdown", index=False)

        for sheet in ["Raw Input Data", "Emissions Summary", "Scope Breakdown"]:
            _style_sheet(writer, sheet)

        if compliance_tables:
            for name, table in compliance_tables.items():
                if table is None or table.empty:
                    continue
                sheet_name = _clean_sheet_name(name)
                table.to_excel(writer, sheet_name=sheet_name, index=False)
                _style_sheet(writer, sheet_name)

    buffer.seek(0)
    return buffer
