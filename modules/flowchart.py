from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go


def generate_flowchart(summary_df: pd.DataFrame) -> go.Figure:
    """Create a Sankey diagram from emissions summary by scope and activity."""
    if summary_df is None or summary_df.empty:
        return go.Figure()

    required = {"category", "activity", "total_co2e"}
    missing = required - set(summary_df.columns)
    if missing:
        raise ValueError(f"Summary dataframe missing columns: {', '.join(sorted(missing))}")

    working = summary_df.copy()
    working["scope"] = working["category"].astype(str)
    working["activity_node"] = working["scope"] + " | " + working["activity"].astype(str)

    scope_nodes = sorted(working["scope"].unique())
    activity_nodes = working["activity_node"].tolist()
    labels = scope_nodes + activity_nodes

    scope_index = {label: idx for idx, label in enumerate(scope_nodes)}

    sources = [scope_index[row["scope"]] for _, row in working.iterrows()]
    targets = [len(scope_nodes) + i for i in range(len(working))]
    values = working["total_co2e"].astype(float).tolist()

    fig = go.Figure(
        data=[
            go.Sankey(
                arrangement="snap",
                node={"label": labels, "pad": 18, "thickness": 16},
                link={"source": sources, "target": targets, "value": values},
            )
        ]
    )
    fig.update_layout(title_text="Emission Source Flowchart", font_size=11)
    return fig


def generate_sankey_from_edges(edges_df: pd.DataFrame, title: str = "Flowchart") -> go.Figure:
    """Create a Sankey diagram from source-target-value edges."""
    if edges_df is None or edges_df.empty:
        return go.Figure()

    required = {"source", "target", "value"}
    missing = required - set(edges_df.columns)
    if missing:
        raise ValueError(f"Edges dataframe missing columns: {', '.join(sorted(missing))}")

    nodes = pd.Index(edges_df["source"].tolist() + edges_df["target"].tolist()).unique().tolist()
    node_map = {node: i for i, node in enumerate(nodes)}

    fig = go.Figure(
        data=[
            go.Sankey(
                node={"label": nodes, "pad": 18, "thickness": 16},
                link={
                    "source": edges_df["source"].map(node_map).tolist(),
                    "target": edges_df["target"].map(node_map).tolist(),
                    "value": edges_df["value"].astype(float).tolist(),
                },
            )
        ]
    )
    fig.update_layout(title_text=title, font_size=11)
    return fig
