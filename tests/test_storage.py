from __future__ import annotations

from pathlib import Path

import pandas as pd

from modules.storage import (
    compare_runs,
    init_database,
    list_factors,
    list_runs,
    list_scope_categories,
    load_run,
    save_run,
    upsert_factor,
)


def test_storage_factor_seed_and_upsert(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    csv_path = tmp_path / "seed.csv"
    pd.DataFrame(
        [
            {
                "activity": "diesel use",
                "unit": "l",
                "emission_factor": 2.68,
                "scope": "scope1",
                "scope_category": "mobile_combustion",
                "region": "global",
                "year": 2025,
                "source": "seed",
                "version": "v1",
                "active": 1,
            }
        ]
    ).to_csv(csv_path, index=False)

    init_database(db_path, seed_factors_csv=csv_path)
    init_database(db_path, seed_factors_csv=csv_path)
    factors = list_factors(db_path)
    assert len(factors) == 1
    assert "scope_category" in factors.columns
    assert factors.iloc[0]["scope_category"] == "mobile_combustion"

    scope_categories = list_scope_categories(db_path)
    assert not scope_categories.empty
    assert {"scope1", "scope2", "scope3"}.issubset(set(scope_categories["scope"]))

    new_id = upsert_factor(
        db_path,
        {
            "activity": "waste",
            "unit": "kg",
            "emission_factor": 0.4,
            "scope": "scope3",
            "scope_category": "cat5_waste_generated",
            "region": "global",
            "year": 2025,
            "source": "custom",
            "version": "v1",
            "active": 1,
        },
    )
    assert new_id > 0


def test_storage_runs_save_load_compare(tmp_path: Path) -> None:
    db_path = tmp_path / "runs.db"
    init_database(db_path)

    run_a = save_run(
        db_path,
        run_name="a",
        run_type="manual",
        payload={"raw_records": [{"activity": "x"}]},
        total_co2e=100.0,
    )
    run_b = save_run(
        db_path,
        run_name="b",
        run_type="manual",
        payload={"raw_records": [{"activity": "x"}]},
        total_co2e=80.0,
    )

    runs = list_runs(db_path)
    assert len(runs) == 2

    loaded = load_run(db_path, run_a)
    assert loaded["run_name"] == "a"

    cmp = compare_runs(db_path, run_a, run_b)
    assert cmp["delta"] == -20.0
