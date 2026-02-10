from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import plotly.express as px
import streamlit as st

from modules.compliance import build_compliance_tables
from modules.data_quality import assess_data_quality
from modules.emissions import calculate_emissions, load_emission_factors
from modules.excel_parser import parse_excel
from modules.export_excel import export_excel
from modules.export_pdf import export_pdf
from modules.flowchart import generate_flowchart, generate_sankey_from_edges
from modules.kpi import compute_intensity_metrics
from modules.lca import run_lca
from modules.manual_input import build_manual_dataframe, validate_manual_inputs
from modules.scenario import run_reduction_scenario
from modules.storage import (
    compare_runs,
    delete_factor,
    factors_for_calculation,
    init_database,
    list_factors,
    list_runs,
    list_scope_categories,
    load_run,
    save_run,
    upsert_factor,
)

PROJECT_ROOT = Path(__file__).resolve().parent
FACTOR_FILE = PROJECT_ROOT / "data" / "emission_factors.csv"
DB_FILE = PROJECT_ROOT / "data" / "carbonsuite.db"


def _ensure_state() -> None:
    defaults = {
        "raw_df": None,
        "emissions_result": None,
        "emissions_flowchart": None,
        "lca_result": None,
        "lca_flowchart": None,
        "scenario_result": None,
        "data_quality": None,
        "intensity_metrics": {},
        "compliance_tables": {},
        "report_metadata": {
            "organization": "",
            "reporting_year": datetime.now().year,
            "reporting_standard": "GHG Protocol",
            "prepared_by": "",
        },
        "assumptions": [],
        "change_log": [],
        "pdf_bytes": None,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def _load_factor_df(region: str, year: Optional[int]) -> pd.DataFrame:
    factor_df = factors_for_calculation(DB_FILE, region=region, year=year)
    if factor_df.empty:
        factor_df = load_emission_factors(FACTOR_FILE)
    return factor_df


def _render_emissions(result: dict) -> Optional[object]:
    st.metric("Total Emissions", f"{result['total_co2e']:,.3f} tCO2e")

    if result.get("conversion_warnings"):
        warnings = result["conversion_warnings"]
        st.info(f"Unit conversion notes found: {len(warnings)}")
        with st.expander("Show unit conversion details"):
            for warning in warnings:
                st.caption(warning)

    st.dataframe(result["summary_df"], use_container_width=True)

    bar_fig = px.bar(
        result["summary_df"],
        x="activity",
        y="total_co2e",
        color="category",
        title="Emissions by Activity",
    )
    st.plotly_chart(bar_fig, use_container_width=True)
    return bar_fig


def _figure_to_png_bytes(fig) -> Optional[bytes]:
    if fig is None:
        return None
    try:
        return fig.to_image(format="png", width=1200, height=650, scale=2)
    except Exception:
        return None


def _save_emissions_run(run_name: str, run_type: str, metadata: Dict[str, object]) -> None:
    if st.session_state.raw_df is None or st.session_state.emissions_result is None:
        return

    payload = {
        "raw_records": st.session_state.raw_df.to_dict(orient="records"),
        "run_type": run_type,
    }
    save_run(
        DB_FILE,
        run_name=run_name,
        run_type=run_type,
        payload=payload,
        metadata=metadata,
        total_co2e=float(st.session_state.emissions_result.get("total_co2e", 0.0)),
    )


def _load_emissions_run_payload(payload: Dict[str, object], factor_df: pd.DataFrame, region: str, year: Optional[int]) -> None:
    raw_records = payload.get("raw_records")
    if not raw_records:
        raise ValueError("Selected run does not include raw records.")

    raw_df = pd.DataFrame(raw_records)
    result = calculate_emissions(raw_df, factors_df=factor_df, region=region, year=year)

    st.session_state.raw_df = raw_df
    st.session_state.emissions_result = result
    st.session_state.emissions_flowchart = generate_flowchart(result["summary_df"])
    st.session_state.data_quality = assess_data_quality(raw_df)


def _render_intensity_panel() -> None:
    if st.session_state.emissions_result is None:
        st.info("Run an emissions analysis to compute intensity KPIs.")
        return

    st.markdown("### Intensity KPI Inputs")
    col1, col2, col3 = st.columns(3)
    production = col1.number_input("Production Units", min_value=0.0, value=0.0, step=1.0)
    revenue = col2.number_input("Revenue (USD)", min_value=0.0, value=0.0, step=1000.0)
    employees = col3.number_input("Employees", min_value=0.0, value=0.0, step=1.0)

    metrics = compute_intensity_metrics(
        total_co2e=st.session_state.emissions_result["total_co2e"],
        production_units=production if production > 0 else None,
        revenue_usd=revenue if revenue > 0 else None,
        employees=employees if employees > 0 else None,
    )
    st.session_state.intensity_metrics = metrics

    if metrics:
        metric_cols = st.columns(len(metrics))
        for i, (name, value) in enumerate(metrics.items()):
            metric_cols[i].metric(name, f"{value:,.4f}")


st.set_page_config(page_title="CarbonSuite", page_icon="🌍", layout="wide")
st.title("CarbonSuite: Carbon Accounting + LCA")

init_database(DB_FILE, seed_factors_csv=FACTOR_FILE)
_ensure_state()

st.sidebar.header("Calculation Context")
context_region = st.sidebar.text_input("Factor Region", value="global")
use_year = st.sidebar.checkbox("Filter by Factor Year", value=True)
context_year_input = st.sidebar.number_input("Factor Year", min_value=1990, max_value=2100, value=2025, step=1)
context_year = int(context_year_input) if use_year else None

active_factor_df = _load_factor_df(region=context_region, year=context_year)

(
    tab_dashboard,
    tab_manual,
    tab_upload,
    tab_lca,
    tab_scenario,
    tab_factors,
    tab_compliance,
    tab_export,
) = st.tabs(
    [
        "Dashboard",
        "Manual Input",
        "Excel Upload",
        "LCA Module",
        "Scenario Planner",
        "Factor Library",
        "Data & Compliance",
        "Export Center",
    ]
)

with tab_dashboard:
    st.subheader("Overview")

    if st.session_state.emissions_result is None and st.session_state.lca_result is None:
        st.info("No analysis has been run yet. Use Manual Input, Excel Upload, or LCA Module.")

    if st.session_state.emissions_result is not None:
        st.markdown("### Carbon Accounting Results")
        _render_emissions(st.session_state.emissions_result)

        if st.session_state.emissions_flowchart is not None:
            st.plotly_chart(st.session_state.emissions_flowchart, use_container_width=True)

        if st.session_state.data_quality is not None:
            st.markdown("### Data Quality")
            st.metric("Quality Score", f"{st.session_state.data_quality['score']:,.2f}/100")
            st.dataframe(st.session_state.data_quality["issues_df"], use_container_width=True)

        _render_intensity_panel()

    if st.session_state.lca_result is not None:
        st.markdown("### LCA Results")
        st.metric("Total LCA Emissions", f"{st.session_state.lca_result['total_emissions']:,.3f} tCO2e")
        st.dataframe(st.session_state.lca_result["summary_df"], use_container_width=True)

        stage_fig = px.bar(
            st.session_state.lca_result["summary_df"],
            x="stage",
            y="total_emissions",
            title="LCA Emissions by Stage",
        )
        st.plotly_chart(stage_fig, use_container_width=True)

        sens = st.session_state.lca_result.get("sensitivity", {})
        if sens:
            st.caption(
                f"Sensitivity (+/- {sens.get('pct', 0):.1f}% EF): "
                f"{sens.get('low_total', 0):,.3f} to {sens.get('high_total', 0):,.3f} tCO2e"
            )

        if st.session_state.lca_flowchart is not None:
            st.plotly_chart(st.session_state.lca_flowchart, use_container_width=True)

    st.markdown("### Historical Runs")
    runs_df = list_runs(DB_FILE, limit=200)
    if runs_df.empty:
        st.info("No historical runs saved yet.")
    else:
        st.dataframe(runs_df, use_container_width=True)

        col_load, col_compare = st.columns(2)
        with col_load:
            load_id = st.selectbox("Load Run", runs_df["id"].tolist(), index=0)
            if st.button("Load Selected Run"):
                run_obj = load_run(DB_FILE, int(load_id))
                try:
                    _load_emissions_run_payload(run_obj["payload"], active_factor_df, context_region, context_year)
                    st.success(f"Run {load_id} loaded into current dashboard context.")
                except Exception as exc:
                    st.error(str(exc))

        with col_compare:
            a_id = st.selectbox("Compare Run A", runs_df["id"].tolist(), index=0)
            b_id = st.selectbox("Compare Run B", runs_df["id"].tolist(), index=min(1, len(runs_df) - 1))
            if st.button("Compare Runs"):
                cmp = compare_runs(DB_FILE, int(a_id), int(b_id))
                st.write(
                    {
                        "run_a": cmp["run_a"]["run_name"],
                        "run_b": cmp["run_b"]["run_name"],
                        "delta_tCO2e": round(cmp["delta"], 3),
                        "delta_pct": round(cmp["delta_pct"], 2) if cmp["delta_pct"] is not None else None,
                    }
                )

with tab_manual:
    st.subheader("Manual Carbon Input")

    run_name = st.text_input("Run Name", value=f"manual-{datetime.now().strftime('%Y%m%d-%H%M%S')}")

    with st.form("manual-input-form"):
        left, right = st.columns(2)

        with left:
            fuel_amount = st.number_input("Fuel Consumption", min_value=0.0, value=0.0, step=1.0)
            fuel_unit = st.selectbox("Fuel Unit", ["L", "kg", "MJ", "g", "tonne"])
            fuel_ef = st.number_input("Fuel Emission Factor", min_value=0.0, value=2.68, step=0.01)

            electricity_kwh = st.number_input("Electricity Consumption (kWh)", min_value=0.0, value=0.0, step=1.0)
            electricity_ef = st.number_input(
                "Electricity Emission Factor", min_value=0.0, value=0.40, step=0.01
            )

        with right:
            renewable_fraction_percent = st.slider("Renewable Fraction (%)", 0, 100, 0, 5)
            transport_km = st.number_input("Transportation Distance", min_value=0.0, value=0.0, step=1.0)
            transport_ef = st.number_input("Transportation Emission Factor", min_value=0.0, value=0.12, step=0.01)
            waste_kg = st.number_input("Waste Weight", min_value=0.0, value=0.0, step=1.0)
            waste_ef = st.number_input("Waste Emission Factor", min_value=0.0, value=0.45, step=0.01)

        submitted = st.form_submit_button("Calculate Emissions")

    if submitted:
        entries = {
            "fuel": float(fuel_amount),
            "electricity": float(electricity_kwh),
            "transport": float(transport_km),
            "waste": float(waste_kg),
        }
        valid, message = validate_manual_inputs(entries)

        if not valid:
            st.error(message)
        else:
            manual_df = build_manual_dataframe(
                fuel_amount=float(fuel_amount),
                fuel_unit=fuel_unit,
                electricity_kwh=float(electricity_kwh),
                renewable_fraction=renewable_fraction_percent / 100.0,
                transport_km=float(transport_km),
                waste_kg=float(waste_kg),
                fuel_ef=float(fuel_ef),
                electricity_ef=float(electricity_ef),
                transport_ef=float(transport_ef),
                waste_ef=float(waste_ef),
            )

            result = calculate_emissions(
                manual_df,
                factors_df=active_factor_df,
                region=context_region,
                year=context_year,
            )

            st.session_state.raw_df = manual_df
            st.session_state.emissions_result = result
            st.session_state.emissions_flowchart = generate_flowchart(result["summary_df"])
            st.session_state.data_quality = assess_data_quality(manual_df)
            st.session_state.pdf_bytes = None

            _save_emissions_run(
                run_name=run_name,
                run_type="manual",
                metadata={"region": context_region, "year": context_year},
            )
            st.success("Manual carbon calculation completed and saved.")

    if st.session_state.emissions_result is not None:
        _render_emissions(st.session_state.emissions_result)

with tab_upload:
    st.subheader("Excel Upload")
    run_name = st.text_input("Run Name", value=f"upload-{datetime.now().strftime('%Y%m%d-%H%M%S')}", key="upload_run_name")
    uploaded_file = st.file_uploader("Upload activity workbook (.xlsx)", type=["xlsx"])

    if st.button("Parse and Calculate", disabled=uploaded_file is None):
        try:
            raw_df = parse_excel(uploaded_file)
            result = calculate_emissions(
                raw_df,
                factors_df=active_factor_df,
                region=context_region,
                year=context_year,
            )

            st.session_state.raw_df = raw_df
            st.session_state.emissions_result = result
            st.session_state.emissions_flowchart = generate_flowchart(result["summary_df"])
            st.session_state.data_quality = assess_data_quality(raw_df)
            st.session_state.pdf_bytes = None

            _save_emissions_run(
                run_name=run_name,
                run_type="excel_upload",
                metadata={"region": context_region, "year": context_year},
            )
            st.success("Excel parsing and emissions calculations completed and saved.")
        except Exception as exc:
            st.error(str(exc))

    if st.session_state.raw_df is not None:
        st.markdown("### Parsed Raw Data")
        st.dataframe(st.session_state.raw_df.head(200), use_container_width=True)

    if st.session_state.emissions_result is not None:
        st.markdown("### Emissions Output")
        _render_emissions(st.session_state.emissions_result)

with tab_lca:
    st.subheader("Lifecycle Analysis")

    input_mode = st.radio("Inventory Input Mode", ["Table", "JSON"], horizontal=True)
    boundary = st.selectbox("System Boundary", ["cradle-to-grave", "cradle-to-gate", "gate-to-gate"])
    allocation_method = st.selectbox("Allocation Method", ["none", "mass", "economic", "energy"])
    default_allocation_factor = st.slider("Default Allocation Factor", min_value=0.0, max_value=1.0, value=1.0, step=0.05)
    sensitivity_pct = st.slider("Sensitivity (+/- % EF)", min_value=0.0, max_value=50.0, value=10.0, step=1.0)

    default_inventory = pd.DataFrame(
        {
            "stage": ["Materials", "Transport", "Processing", "Distribution", "End-of-life"],
            "amount": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emission_factor": [0.0, 0.0, 0.0, 0.0, 0.0],
            "allocation_factor": [default_allocation_factor] * 5,
        }
    )

    lca_input = None
    stage_alloc = {}
    if input_mode == "Table":
        lca_input = st.data_editor(default_inventory, use_container_width=True, num_rows="dynamic")
        if "allocation_factor" in lca_input.columns:
            stage_alloc = {
                str(row["stage"]): float(row["allocation_factor"])
                for _, row in lca_input.dropna(subset=["stage", "allocation_factor"]).iterrows()
            }
    else:
        st.caption("JSON format: {\"inventory\": [{\"stage\": \"Materials\", \"amount\": 10, \"emission_factor\": 1.8}]}")
        json_text = st.text_area("LCA JSON", height=200)
        if json_text.strip():
            try:
                lca_input = json.loads(json_text)
            except json.JSONDecodeError as exc:
                st.error(f"Invalid JSON: {exc}")

    lca_run_name = st.text_input("LCA Run Name", value=f"lca-{datetime.now().strftime('%Y%m%d-%H%M%S')}")
    if st.button("Run LCA"):
        try:
            result = run_lca(
                lca_input,
                boundary=boundary,
                allocation_method=allocation_method,
                default_allocation_factor=default_allocation_factor,
                stage_allocation=stage_alloc,
                sensitivity_pct=sensitivity_pct,
            )
            st.session_state.lca_result = result
            st.session_state.lca_flowchart = generate_sankey_from_edges(
                result["sankey_df"], title="LCA Stage Flowchart"
            )

            save_run(
                DB_FILE,
                run_name=lca_run_name,
                run_type="lca",
                payload={"inventory": lca_input.to_dict(orient="records") if isinstance(lca_input, pd.DataFrame) else lca_input},
                metadata={
                    "boundary": boundary,
                    "allocation_method": allocation_method,
                    "sensitivity_pct": sensitivity_pct,
                },
                total_co2e=float(result["total_emissions"]),
            )
            st.success("LCA analysis completed and saved.")
        except Exception as exc:
            st.error(str(exc))

    if st.session_state.lca_result is not None:
        st.metric("Total LCA Emissions", f"{st.session_state.lca_result['total_emissions']:,.3f} tCO2e")
        st.dataframe(st.session_state.lca_result["summary_df"], use_container_width=True)

        hotspots = st.session_state.lca_result["hotspot_categories"]
        if hotspots:
            st.markdown("### Hotspot Categories")
            st.dataframe(pd.DataFrame(hotspots), use_container_width=True)

        sensitivity = st.session_state.lca_result.get("sensitivity", {})
        if sensitivity:
            st.caption(
                f"Sensitivity range: {sensitivity.get('low_total', 0):,.3f} - {sensitivity.get('high_total', 0):,.3f} tCO2e"
            )

        if st.session_state.lca_flowchart is not None:
            st.plotly_chart(st.session_state.lca_flowchart, use_container_width=True)

with tab_scenario:
    st.subheader("Scenario + Target Planner")

    if st.session_state.raw_df is None or st.session_state.emissions_result is None:
        st.info("Run Manual Input or Excel Upload first.")
    else:
        c1, c2, c3 = st.columns(3)
        reduction_scope1 = c1.slider("Scope1 Reduction (%)", 0, 100, 0)
        reduction_scope2 = c2.slider("Scope2 Reduction (%)", 0, 100, 0)
        reduction_scope3 = c3.slider("Scope3 Reduction (%)", 0, 100, 0)

        activities = sorted(st.session_state.raw_df["activity"].astype(str).str.strip().unique().tolist())
        chosen_activity = st.selectbox("Optional Activity-Level Reduction", [""] + activities)
        activity_reduction = st.slider("Selected Activity Reduction (%)", 0, 100, 0)

        target_total = st.number_input("Target Total Emissions (tCO2e)", min_value=0.0, value=0.0, step=1.0)
        has_target = st.checkbox("Enable Target Check", value=False)

        if st.button("Run Scenario"):
            activity_map = {}
            if chosen_activity:
                activity_map[chosen_activity] = activity_reduction

            scenario_result = run_reduction_scenario(
                st.session_state.raw_df,
                factors_df=active_factor_df,
                scope_reduction_pct={
                    "scope1": reduction_scope1,
                    "scope2": reduction_scope2,
                    "scope3": reduction_scope3,
                },
                activity_reduction_pct=activity_map,
                target_total=float(target_total) if has_target else None,
            )

            st.session_state.scenario_result = scenario_result
            st.success("Scenario analysis completed.")

            save_run(
                DB_FILE,
                run_name=f"scenario-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
                run_type="scenario",
                payload={"raw_records": st.session_state.raw_df.to_dict(orient="records")},
                metadata={
                    "scope_reduction": {
                        "scope1": reduction_scope1,
                        "scope2": reduction_scope2,
                        "scope3": reduction_scope3,
                    },
                    "activity_reduction": activity_map,
                    "target_total": float(target_total) if has_target else None,
                },
                total_co2e=float(scenario_result["scenario_total"]),
            )

        if st.session_state.scenario_result is not None:
            sr = st.session_state.scenario_result
            m1, m2, m3 = st.columns(3)
            m1.metric("Baseline", f"{sr['baseline_total']:,.3f}")
            m2.metric("Scenario", f"{sr['scenario_total']:,.3f}")
            m3.metric("Abatement", f"{sr['abatement']:,.3f}")
            st.caption(f"Abatement percent: {sr['abatement_pct']:.2f}%")
            if sr["meets_target"] is not None:
                st.caption(f"Target met: {sr['meets_target']}")

            compare_fig = px.bar(sr["comparison_df"], x="metric", y="tCO2e", title="Scenario Comparison")
            st.plotly_chart(compare_fig, use_container_width=True)

with tab_factors:
    st.subheader("Emission Factor Library")

    factor_df = list_factors(DB_FILE, active_only=False)
    scope_categories_df = list_scope_categories(DB_FILE)
    if factor_df.empty:
        st.info("No factors in library.")
    else:
        st.dataframe(factor_df, use_container_width=True)

    st.markdown("### Add or Update Factor")
    with st.form("factor_form"):
        factor_id_text = st.text_input("Factor ID (leave blank for new)")
        fcol1, fcol2, fcol3 = st.columns(3)
        activity = fcol1.text_input("Activity", value="")
        unit = fcol2.text_input("Unit", value="")
        emission_factor = fcol3.number_input("Emission Factor", min_value=0.0, value=0.0, step=0.001)

        gcol1, gcol2, gcol3 = st.columns(3)
        scope = gcol1.selectbox("Scope", ["", "scope1", "scope2", "scope3"])
        scope_category_options = [""]
        if scope_categories_df is not None and not scope_categories_df.empty:
            filtered_categories = scope_categories_df
            if scope:
                filtered_categories = scope_categories_df[
                    scope_categories_df["scope"].astype(str).str.strip().str.lower() == scope
                ]
            scope_category_options.extend(sorted(filtered_categories["category_code"].astype(str).unique().tolist()))
        scope_category = gcol2.selectbox("Scope Category", scope_category_options)
        region = gcol3.text_input("Region", value="global")

        hcol1, hcol2, hcol3, hcol4 = st.columns(4)
        year = hcol1.number_input("Year", min_value=1990, max_value=2100, value=2025)
        source = hcol2.text_input("Source", value="custom")
        version = hcol3.text_input("Version", value="v1")
        active = hcol4.checkbox("Active", value=True)

        upsert_submitted = st.form_submit_button("Save Factor")

    if upsert_submitted:
        try:
            upsert_factor(
                DB_FILE,
                {
                    "id": int(factor_id_text) if factor_id_text.strip() else None,
                    "activity": activity,
                    "unit": unit,
                    "emission_factor": emission_factor,
                    "scope": scope,
                    "scope_category": scope_category,
                    "region": region,
                    "year": int(year),
                    "source": source,
                    "version": version,
                    "active": int(active),
                },
            )
            st.success("Factor saved.")
        except Exception as exc:
            st.error(str(exc))

    st.markdown("### Delete Factor")
    delete_id = st.number_input("Factor ID to Delete", min_value=1, value=1, step=1)
    if st.button("Delete Factor"):
        try:
            delete_factor(DB_FILE, int(delete_id))
            st.success(f"Factor {delete_id} deleted.")
        except Exception as exc:
            st.error(str(exc))

    if not factor_df.empty:
        st.download_button(
            "Download Factor Library CSV",
            data=factor_df.to_csv(index=False).encode("utf-8"),
            file_name="factor_library.csv",
            mime="text/csv",
        )

with tab_compliance:
    st.subheader("Data Quality + Compliance Pack")

    if st.session_state.emissions_result is None:
        st.info("Run emissions first to build compliance outputs.")
    else:
        meta_col1, meta_col2, meta_col3 = st.columns(3)
        organization = meta_col1.text_input(
            "Organization", value=st.session_state.report_metadata.get("organization", "")
        )
        reporting_year = meta_col2.number_input(
            "Reporting Year",
            min_value=1990,
            max_value=2100,
            value=int(st.session_state.report_metadata.get("reporting_year", datetime.now().year)),
        )
        reporting_standard = meta_col3.text_input(
            "Reporting Standard",
            value=st.session_state.report_metadata.get("reporting_standard", "GHG Protocol"),
        )

        prepared_by = st.text_input(
            "Prepared By", value=st.session_state.report_metadata.get("prepared_by", "")
        )

        assumptions_text = st.text_area(
            "Assumptions (one per line)",
            value="\n".join(st.session_state.assumptions),
            height=120,
        )
        changes_text = st.text_area(
            "Change Log (one per line)",
            value="\n".join(st.session_state.change_log),
            height=120,
        )

        if st.session_state.data_quality is not None:
            st.metric("Current Data Quality Score", f"{st.session_state.data_quality['score']:,.2f}/100")
            st.dataframe(st.session_state.data_quality["issues_df"], use_container_width=True)

        if st.button("Generate Compliance Pack"):
            st.session_state.report_metadata = {
                "organization": organization,
                "reporting_year": int(reporting_year),
                "reporting_standard": reporting_standard,
                "prepared_by": prepared_by,
            }
            st.session_state.assumptions = [line.strip() for line in assumptions_text.splitlines() if line.strip()]
            st.session_state.change_log = [line.strip() for line in changes_text.splitlines() if line.strip()]

            compliance_tables = build_compliance_tables(
                emissions_result=st.session_state.emissions_result,
                factors_df=active_factor_df,
                metadata=st.session_state.report_metadata,
                assumptions=st.session_state.assumptions,
                change_log=st.session_state.change_log,
                intensity_metrics=st.session_state.intensity_metrics,
                data_quality=st.session_state.data_quality,
            )
            st.session_state.compliance_tables = compliance_tables
            st.success("Compliance pack generated.")

        if st.session_state.compliance_tables:
            for name, df in st.session_state.compliance_tables.items():
                if isinstance(df, pd.DataFrame) and not df.empty:
                    st.markdown(f"### {name}")
                    st.dataframe(df, use_container_width=True)

with tab_export:
    st.subheader("Export Center")

    if st.session_state.emissions_result is None or st.session_state.raw_df is None:
        st.info("Run Manual Input or Excel Upload first to enable exports.")
    else:
        compliance_tables = st.session_state.compliance_tables or build_compliance_tables(
            emissions_result=st.session_state.emissions_result,
            factors_df=active_factor_df,
            metadata=st.session_state.report_metadata,
            assumptions=st.session_state.assumptions,
            change_log=st.session_state.change_log,
            intensity_metrics=st.session_state.intensity_metrics,
            data_quality=st.session_state.data_quality,
        )

        excel_buffer = export_excel(
            summary_df=st.session_state.emissions_result["summary_df"],
            raw_df=st.session_state.raw_df,
            scope_df=st.session_state.emissions_result["scope_df"],
            compliance_tables=compliance_tables,
        )
        st.download_button(
            label="Download Excel Report",
            data=excel_buffer.getvalue(),
            file_name="carbonsuite_report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        if st.button("Prepare PDF Report"):
            bar_fig = px.bar(
                st.session_state.emissions_result["summary_df"],
                x="activity",
                y="total_co2e",
                color="category",
                title="Emissions by Activity",
            )
            chart_png = _figure_to_png_bytes(bar_fig)
            flowchart_png = _figure_to_png_bytes(st.session_state.emissions_flowchart)

            pdf_buffer = export_pdf(
                summary=st.session_state.emissions_result,
                charts=[item for item in [chart_png] if item is not None],
                flowchart=flowchart_png,
                metadata=st.session_state.report_metadata,
                intensity_metrics=st.session_state.intensity_metrics,
                data_quality=st.session_state.data_quality,
                assumptions=st.session_state.assumptions,
                change_log=st.session_state.change_log,
            )
            st.session_state.pdf_bytes = pdf_buffer.getvalue()
            st.success("PDF report generated.")

        if st.session_state.pdf_bytes:
            st.download_button(
                label="Download PDF Report",
                data=st.session_state.pdf_bytes,
                file_name="carbonsuite_report.pdf",
                mime="application/pdf",
            )
