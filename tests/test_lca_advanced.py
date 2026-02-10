from __future__ import annotations

import pandas as pd
import pytest

from modules.lca import run_lca


def _inventory_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "stage": ["Materials", "Transport", "Processing", "Distribution", "End-of-life"],
            "amount": [10, 10, 10, 10, 10],
            "emission_factor": [1, 1, 1, 1, 1],
            "allocation_factor": [1, 1, 1, 1, 1],
        }
    )


def test_lca_boundary_filters_stages() -> None:
    result = run_lca(_inventory_df(), boundary="cradle-to-gate")

    assert result["total_emissions"] == pytest.approx(30.0)
    assert set(result["summary_df"]["stage"].astype(str).tolist()) == {"Materials", "Transport", "Processing"}


def test_lca_allocation_reduces_total() -> None:
    result = run_lca(
        _inventory_df(),
        allocation_method="economic",
        default_allocation_factor=0.5,
        stage_allocation={"Materials": 0.8},
    )

    assert result["total_emissions"] < 50.0


def test_lca_sensitivity_bounds() -> None:
    result = run_lca(_inventory_df(), sensitivity_pct=20)

    assert result["sensitivity"]["low_total"] == pytest.approx(40.0)
    assert result["sensitivity"]["high_total"] == pytest.approx(60.0)
