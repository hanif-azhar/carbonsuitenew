# CarbonSuite: Carbon Accounting + LCA App

A modular Streamlit application for carbon accounting and lifecycle analysis (LCA), built with pandas-based processing, export tools, and persistent run history.

## What This App Does

CarbonSuite helps you:
- Calculate GHG emissions across Scope 1, Scope 2, and Scope 3.
- Ingest activity data via manual form input or multi-sheet Excel upload.
- Run simplified LCA with configurable system boundaries and allocation factors.
- Perform scenario and target planning (baseline vs reduction scenario).
- Track data quality and intensity KPIs.
- Manage emission factors with provenance metadata.
- Export compliance-ready Excel/PDF reports.
- Save, reload, and compare historical runs using SQLite.

## Core Features

### 1. Carbon Accounting Engine
- CO2e calculation formula: `amount * emission_factor`
- Optional CH4 and N2O handling with GWP factors
- Scope normalization and breakdown
- Factor provenance output per activity/unit

### 2. Excel Upload and Parsing
- Multi-sheet `.xlsx` reading with pandas
- Required-column validation
- Column alias harmonization
- Numeric coercion and row filtering for invalid values
- Standardized schema output:
  - `category, activity, unit, amount, emission_factor, source, ch4, n2o`

### 3. Manual Input Workflow
- Fuel, electricity, transport, and waste input fields
- Renewable electricity fraction support
- Input validation and error handling

### 4. Unit Conversion Layer
- Canonical unit normalization for activity amounts and factor compatibility
- Common conversions for energy, mass, volume, and distance units
- Conversion warnings surfaced in UI

### 5. Emission Factor Library (CRUD)
- Add/update/delete factors in SQLite
- Metadata fields: scope, scope_category, region, year, source, version, active
- Region/year-aware factor lookup for calculations
- CSV download of library

### 6. Scenario + Target Planner
- Scope-level reduction assumptions
- Optional activity-level reduction
- Baseline vs scenario comparison
- Abatement totals and target pass/fail evaluation

### 7. LCA Engine (Advanced Basic)
- System boundary presets:
  - cradle-to-grave
  - cradle-to-gate
  - gate-to-gate
- Allocation method support with per-stage factors
- Sensitivity range analysis
- Sankey-ready flow data and hotspot identification

### 8. Data Quality and KPIs
- Data quality scoring (missing values, non-numeric values, duplicates, negatives, outliers)
- Emission intensity metrics:
  - `tCO2e_per_unit`
  - `tCO2e_per_musd`
  - `tCO2e_per_employee`

### 9. Compliance Outputs
- Compliance-oriented table generation:
  - GHG scope table
  - emission factor provenance
  - assumptions
  - change log
  - intensity KPI
  - data quality summary

### 10. Export Center
- Excel export with formatted multi-sheet reports
- PDF export containing:
  - title and metadata
  - summary metrics
  - charts and flowchart
  - methodology
  - assumptions and change log

### 11. Persistence and Comparison
- Store runs (`manual`, `excel_upload`, `lca`, `scenario`) in SQLite
- Load historical runs into active dashboard context
- Compare two runs (absolute and percentage delta)

## App Tabs

The Streamlit UI includes:
1. Dashboard
2. Manual Input
3. Excel Upload
4. LCA Module
5. Scenario Planner
6. Factor Library
7. Data & Compliance
8. Export Center

## Project Structure

```text
carbonsuitenew/
├── app.py
├── guidelines.md
├── readme.md
├── requirements.txt
├── data/
│   └── emission_factors.csv
├── modules/
│   ├── __init__.py
│   ├── compliance.py
│   ├── data_quality.py
│   ├── emissions.py
│   ├── excel_parser.py
│   ├── export_excel.py
│   ├── export_pdf.py
│   ├── flowchart.py
│   ├── kpi.py
│   ├── lca.py
│   ├── manual_input.py
│   ├── scenario.py
│   ├── storage.py
│   └── unit_conversion.py
└── tests/
    ├── conftest.py
    ├── test_emissions.py
    ├── test_excel_parser.py
    ├── test_lca_advanced.py
    ├── test_manual_input.py
    ├── test_scenario.py
    ├── test_storage.py
    └── test_unit_conversion.py
```

## Requirements

### System Requirements
- Python 3.9+
- pip
- Internet access for package installation

### Python Dependencies
Defined in `requirements.txt`:
- `streamlit>=1.32.0`
- `pandas>=2.0.0`
- `numpy>=1.24.0`
- `plotly>=5.20.0`
- `openpyxl>=3.1.0`
- `reportlab>=4.0.0`
- `kaleido>=0.2.1`
- `pytest>=8.0.0`

## Installation

From your workspace root:

```bash
cd "personal project/carbonsuitenew"
python3 -m pip install -r requirements.txt
```

## Run the App

```bash
cd "personal project/carbonsuitenew"
streamlit run app.py
```

On first run, the app initializes SQLite at:
- `data/carbonsuite.db`

## Running Tests

```bash
cd "personal project/carbonsuitenew"
python3 -m pytest -q
```

## Data Input Notes

### Expected Carbon Accounting Columns
Your processed data should contain:
- `category`
- `activity`
- `unit`
- `amount`
- `emission_factor`
- optional: `source`, `ch4`, `n2o`

### LCA Input Columns
- `stage`
- `amount`
- `emission_factor`
- optional: `allocation_factor`

## Typical Workflow

1. Set factor context in sidebar (region/year).
2. Run Manual Input or Excel Upload.
3. Review Dashboard metrics, flowcharts, data quality, and intensity KPIs.
4. Use Scenario Planner for abatement planning and target checks.
5. Use Factor Library to maintain factor records.
6. Build compliance pack in Data & Compliance.
7. Export final Excel/PDF report from Export Center.
8. Use Historical Runs to reload and compare results over time.

## Notes

- Emission factors can come from seed CSV and/or factor library DB.
- Seed factor library includes expanded IPCC-default-style Scope 1/2/3 entries and scope category taxonomy.
- Unit conversion is automatic for supported units; unknown units are kept and flagged.
- LCA implementation is a simplified model suitable for baseline analysis and expansion.
