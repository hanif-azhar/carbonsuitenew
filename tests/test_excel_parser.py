from __future__ import annotations

from io import BytesIO

import pandas as pd
import pytest

from modules.excel_parser import parse_excel


def _make_workbook(sheet_map: dict[str, pd.DataFrame]) -> BytesIO:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        for sheet_name, df in sheet_map.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    buffer.seek(0)
    return buffer


def test_parse_excel_multi_sheet_harmonized() -> None:
    workbook = _make_workbook(
        {
            "Fuel": pd.DataFrame(
                {
                    "Scope": ["Scope1"],
                    "Activity Name": ["Diesel Use"],
                    "Unit": ["L"],
                    "Amount": [100],
                    "EF": [2.68],
                }
            ),
            "Power": pd.DataFrame(
                {
                    "category": ["scope2"],
                    "activity": ["Electricity consumption"],
                    "unit": ["kWh"],
                    "amount": [200],
                    "emission_factor": [0.4],
                    "source": ["grid_supplier"],
                }
            ),
        }
    )

    parsed = parse_excel(workbook)

    assert len(parsed) == 2
    assert set(["category", "activity", "unit", "amount", "emission_factor", "source"]).issubset(parsed.columns)


def test_parse_excel_rejects_empty_upload() -> None:
    with pytest.raises(ValueError):
        parse_excel(BytesIO(b""))


def test_parse_excel_drops_non_numeric_rows() -> None:
    workbook = _make_workbook(
        {
            "Sheet1": pd.DataFrame(
                {
                    "category": ["scope3", "scope3"],
                    "activity": ["Waste", "Waste"],
                    "unit": ["kg", "kg"],
                    "amount": ["bad", 50],
                    "emission_factor": [0.45, 0.45],
                }
            )
        }
    )

    parsed = parse_excel(workbook)

    assert len(parsed) == 1
    assert parsed.iloc[0]["amount"] == 50


def test_parse_excel_raises_when_no_valid_rows() -> None:
    workbook = _make_workbook(
        {
            "Sheet1": pd.DataFrame(
                {
                    "category": ["scope3"],
                    "activity": ["Waste"],
                    "unit": ["kg"],
                    "amount": ["bad"],
                    "emission_factor": ["also_bad"],
                }
            )
        }
    )

    with pytest.raises(ValueError):
        parse_excel(workbook)
