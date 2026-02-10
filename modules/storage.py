from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

import pandas as pd


FACTOR_COLUMNS = [
    "id",
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
    "created_at",
    "updated_at",
]

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

DEFAULT_SCOPE_CATEGORIES = [
    ("scope1", "stationary_combustion", "Stationary Combustion", "Fuel burned in owned/controlled equipment."),
    ("scope1", "mobile_combustion", "Mobile Combustion", "Fuel burned in owned/controlled vehicles."),
    ("scope1", "process_emissions", "Process Emissions", "Direct process emissions from industrial operations."),
    ("scope1", "fugitive_emissions", "Fugitive Emissions", "Leakage of refrigerants and other gases."),
    ("scope2", "purchased_electricity", "Purchased Electricity", "Indirect emissions from purchased electricity."),
    ("scope2", "purchased_steam", "Purchased Steam", "Indirect emissions from imported steam."),
    ("scope2", "purchased_heating", "Purchased Heating", "Indirect emissions from district heating."),
    ("scope2", "purchased_cooling", "Purchased Cooling", "Indirect emissions from district cooling."),
    ("scope3", "cat1_purchased_goods_services", "Category 1 Purchased Goods and Services", "Upstream emissions from purchased goods/services."),
    ("scope3", "cat2_capital_goods", "Category 2 Capital Goods", "Upstream emissions from capital goods."),
    ("scope3", "cat3_fuel_energy_related", "Category 3 Fuel and Energy Related", "Fuel and energy activities not included in Scope 1/2."),
    ("scope3", "cat4_upstream_transport", "Category 4 Upstream Transportation", "Upstream transport and distribution."),
    ("scope3", "cat5_waste_generated", "Category 5 Waste Generated", "Upstream waste treatment from operations."),
    ("scope3", "cat6_business_travel", "Category 6 Business Travel", "Business travel in non-owned assets."),
    ("scope3", "cat7_employee_commuting", "Category 7 Employee Commuting", "Commuting and remote work emissions."),
    ("scope3", "cat8_upstream_leased_assets", "Category 8 Upstream Leased Assets", "Leased assets not in Scope 1/2."),
    ("scope3", "cat9_downstream_transport", "Category 9 Downstream Transportation", "Downstream transport and distribution."),
    ("scope3", "cat10_processing_sold_products", "Category 10 Processing of Sold Products", "Processing of intermediate sold products."),
    ("scope3", "cat11_use_sold_products", "Category 11 Use of Sold Products", "Use-phase emissions of sold products."),
    ("scope3", "cat12_end_of_life", "Category 12 End-of-Life Treatment", "End-of-life treatment of sold products."),
    ("scope3", "cat13_downstream_leased_assets", "Category 13 Downstream Leased Assets", "Downstream leased assets."),
    ("scope3", "cat14_franchises", "Category 14 Franchises", "Franchise operation emissions."),
    ("scope3", "cat15_investments", "Category 15 Investments", "Financed and investment-related emissions."),
]


def _connect(db_path: str | Path) -> sqlite3.Connection:
    return sqlite3.connect(str(db_path), check_same_thread=False)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]).strip().lower() for row in rows}


def _normalize_scope(scope_value: object) -> Optional[str]:
    raw = str(scope_value).strip().lower()
    if not raw or raw == "nan":
        return None
    return SCOPE_MAP.get(raw, raw)


def _normalize_scope_category(scope_category: object) -> Optional[str]:
    raw = str(scope_category).strip().lower().replace(" ", "_")
    if not raw or raw == "nan":
        return None
    return raw


def _as_optional_text(value: object, *, lowercase: bool = False) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return text.lower() if lowercase else text


def _as_int_or_none(value: object) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return int(value)


def _ensure_factor_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS factor_library (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            activity TEXT NOT NULL,
            unit TEXT NOT NULL,
            emission_factor REAL NOT NULL,
            scope TEXT,
            scope_category TEXT,
            region TEXT,
            year INTEGER,
            source TEXT,
            version TEXT,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    existing_cols = _table_columns(conn, "factor_library")
    if "scope_category" not in existing_cols:
        conn.execute("ALTER TABLE factor_library ADD COLUMN scope_category TEXT")


def _ensure_scope_categories_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scope_categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scope TEXT NOT NULL,
            category_code TEXT NOT NULL,
            category_name TEXT NOT NULL,
            description TEXT,
            source TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(scope, category_code)
        )
        """
    )


def _seed_scope_categories(conn: sqlite3.Connection, source: str = "GHG_Protocol_Default") -> None:
    now = _now_iso()
    for scope, code, name, description in DEFAULT_SCOPE_CATEGORIES:
        conn.execute(
            """
            INSERT INTO scope_categories
            (scope, category_code, category_name, description, source, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(scope, category_code)
            DO UPDATE SET
                category_name = excluded.category_name,
                description = excluded.description,
                source = excluded.source,
                updated_at = excluded.updated_at
            """,
            (scope, code, name, description, source, now, now),
        )


def _upsert_factor_with_conn(
    conn: sqlite3.Connection,
    factor: Dict[str, object],
    *,
    match_on_natural_key: bool = True,
) -> int:
    activity = str(factor.get("activity", "")).strip().lower()
    unit = str(factor.get("unit", "")).strip().lower()
    emission_factor = float(factor.get("emission_factor", 0.0))

    if not activity or not unit:
        raise ValueError("activity and unit are required for factor records.")

    scope = _normalize_scope(factor.get("scope", ""))
    scope_category = _normalize_scope_category(factor.get("scope_category", ""))
    region = _as_optional_text(factor.get("region", "global"), lowercase=True) or "global"
    year = _as_int_or_none(factor.get("year"))
    source = _as_optional_text(factor.get("source", "unspecified")) or "unspecified"
    version = _as_optional_text(factor.get("version", "v1")) or "v1"
    active = int(bool(factor.get("active", 1)))
    now = _now_iso()
    record_id = factor.get("id")

    if record_id is None and match_on_natural_key:
        row = conn.execute(
            """
            SELECT id
            FROM factor_library
            WHERE activity = ?
              AND unit = ?
              AND COALESCE(scope, '') = ?
              AND COALESCE(region, 'global') = ?
              AND COALESCE(year, -1) = ?
              AND COALESCE(source, '') = ?
              AND COALESCE(version, '') = ?
            LIMIT 1
            """,
            (
                activity,
                unit,
                scope or "",
                region,
                year if year is not None else -1,
                source,
                version,
            ),
        ).fetchone()
        if row is not None:
            record_id = int(row[0])

    if record_id is None:
        cursor = conn.execute(
            """
            INSERT INTO factor_library
            (activity, unit, emission_factor, scope, scope_category, region, year, source, version, active, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                activity,
                unit,
                emission_factor,
                scope,
                scope_category,
                region,
                year,
                source,
                version,
                active,
                now,
                now,
            ),
        )
        return int(cursor.lastrowid)

    conn.execute(
        """
        UPDATE factor_library
        SET activity = ?, unit = ?, emission_factor = ?, scope = ?, scope_category = ?, region = ?, year = ?,
            source = ?, version = ?, active = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            activity,
            unit,
            emission_factor,
            scope,
            scope_category,
            region,
            year,
            source,
            version,
            active,
            now,
            int(record_id),
        ),
    )
    return int(record_id)


def init_database(db_path: str | Path, seed_factors_csv: Optional[str | Path] = None) -> None:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with _connect(db_path) as conn:
        _ensure_factor_schema(conn)
        _ensure_scope_categories_schema(conn)
        _seed_scope_categories(conn)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS analysis_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_name TEXT NOT NULL,
                run_type TEXT NOT NULL,
                run_timestamp TEXT NOT NULL,
                total_co2e REAL,
                payload_json TEXT NOT NULL,
                metadata_json TEXT
            )
            """
        )

        if seed_factors_csv and Path(seed_factors_csv).exists():
            seed_df = pd.read_csv(seed_factors_csv)
            for _, row in seed_df.iterrows():
                _upsert_factor_with_conn(
                    conn,
                    {
                        "activity": row.get("activity", ""),
                        "unit": row.get("unit", ""),
                        "emission_factor": row.get("emission_factor", 0.0),
                        "scope": row.get("scope", ""),
                        "scope_category": row.get("scope_category", ""),
                        "region": row.get("region", "global"),
                        "year": row.get("year", None),
                        "source": row.get("source", "seed_csv"),
                        "version": row.get("version", "v1"),
                        "active": row.get("active", 1),
                    },
                )


def list_factors(db_path: str | Path, active_only: bool = False) -> pd.DataFrame:
    query = "SELECT * FROM factor_library"
    if active_only:
        query += " WHERE active = 1"
    query += " ORDER BY activity, unit, year"

    with _connect(db_path) as conn:
        return pd.read_sql_query(query, conn)


def list_scope_categories(db_path: str | Path) -> pd.DataFrame:
    with _connect(db_path) as conn:
        return pd.read_sql_query(
            """
            SELECT scope, category_code, category_name, description, source
            FROM scope_categories
            ORDER BY scope, category_code
            """,
            conn,
        )


def upsert_factor(db_path: str | Path, factor: Dict[str, object]) -> int:
    with _connect(db_path) as conn:
        _ensure_factor_schema(conn)
        return _upsert_factor_with_conn(conn, factor, match_on_natural_key=True)


def delete_factor(db_path: str | Path, factor_id: int) -> None:
    with _connect(db_path) as conn:
        conn.execute("DELETE FROM factor_library WHERE id = ?", (int(factor_id),))


def factors_for_calculation(
    db_path: str | Path,
    region: Optional[str] = None,
    year: Optional[int] = None,
) -> pd.DataFrame:
    query = "SELECT * FROM factor_library WHERE active = 1"
    params = []
    if region:
        query += " AND (region = ? OR region = 'global')"
        params.append(region.strip().lower())
    if year:
        query += " AND (year = ? OR year IS NULL)"
        params.append(int(year))

    with _connect(db_path) as conn:
        df = pd.read_sql_query(query, conn, params=params)

    if df.empty:
        return df

    working = df.copy()
    working["year_num"] = pd.to_numeric(working["year"], errors="coerce").fillna(-1)
    source_is_ipcc = working.get("source", pd.Series("", index=working.index)).astype(str).str.lower().str.contains(
        "ipcc", regex=False
    )
    working["source_priority"] = (~source_is_ipcc).astype(int)

    deduped = working.sort_values(["activity", "unit", "year_num", "source_priority", "updated_at"]).drop_duplicates(
        subset=["activity", "unit"], keep="last"
    )
    return deduped.drop(columns=["year_num", "source_priority"], errors="ignore")


def save_run(
    db_path: str | Path,
    run_name: str,
    run_type: str,
    payload: Dict[str, object],
    metadata: Optional[Dict[str, object]] = None,
    total_co2e: Optional[float] = None,
) -> int:
    payload_json = json.dumps(payload, default=str)
    metadata_json = json.dumps(metadata or {}, default=str)

    with _connect(db_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO analysis_runs (run_name, run_type, run_timestamp, total_co2e, payload_json, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                run_name.strip() or "Unnamed Run",
                run_type.strip() or "general",
                _now_iso(),
                float(total_co2e) if total_co2e is not None else None,
                payload_json,
                metadata_json,
            ),
        )
        return int(cursor.lastrowid)


def list_runs(db_path: str | Path, limit: int = 200) -> pd.DataFrame:
    with _connect(db_path) as conn:
        return pd.read_sql_query(
            """
            SELECT id, run_name, run_type, run_timestamp, total_co2e
            FROM analysis_runs
            ORDER BY id DESC
            LIMIT ?
            """,
            conn,
            params=[int(limit)],
        )


def load_run(db_path: str | Path, run_id: int) -> Dict[str, object]:
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT id, run_name, run_type, run_timestamp, total_co2e, payload_json, metadata_json FROM analysis_runs WHERE id = ?",
            (int(run_id),),
        ).fetchone()

    if row is None:
        raise ValueError(f"Run id {run_id} not found.")

    return {
        "id": row[0],
        "run_name": row[1],
        "run_type": row[2],
        "run_timestamp": row[3],
        "total_co2e": row[4],
        "payload": json.loads(row[5]),
        "metadata": json.loads(row[6]) if row[6] else {},
    }


def compare_runs(db_path: str | Path, run_id_a: int, run_id_b: int) -> Dict[str, object]:
    run_a = load_run(db_path, run_id_a)
    run_b = load_run(db_path, run_id_b)

    total_a = float(run_a.get("total_co2e") or 0.0)
    total_b = float(run_b.get("total_co2e") or 0.0)
    delta = total_b - total_a
    delta_pct = (delta / total_a * 100.0) if total_a else None

    return {
        "run_a": run_a,
        "run_b": run_b,
        "delta": delta,
        "delta_pct": delta_pct,
    }
