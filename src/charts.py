"""
Chart creation functions for the Trade Balance Dashboard.
"""

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

from src.config import SECTORES_SITC, SECTOR_A_GRUPO, SUNBURST_BASE_COLORS
from src.utils import (
    format_currency,
    format_partner_name,
    format_value_short,
    lighten_color,
    darken_color,
)


def create_evolution_chart(df_total):
    """Create dual-axis chart with lines for trade flows and bars for balance.

    Args:
        df_total: DataFrame with fecha, exportaciones, importaciones columns.

    Returns:
        Plotly Figure object with combined line and bar chart.
    """
    df_evol = (
        df_total.groupby("fecha")[["exportaciones", "importaciones"]]
        .sum()
        .reset_index()
    )
    df_evol["balance"] = df_evol["exportaciones"] - df_evol["importaciones"]

    # Format values for hover
    exp_formatted = [format_currency(val) for val in df_evol["exportaciones"]]
    imp_formatted = [format_currency(val) for val in df_evol["importaciones"]]
    bal_formatted = [format_currency(val) for val in df_evol["balance"]]

    fig_evol = go.Figure()

    # Exportaciones line
    fig_evol.add_trace(
        go.Scatter(
            x=df_evol["fecha"],
            y=df_evol["exportaciones"],
            name="Exportaciones",
            line=dict(color="#00CC96", width=3),
            mode="lines+markers",
            yaxis="y",
            customdata=exp_formatted,
            hovertemplate="<b>Exportaciones</b><br>%{customdata}<extra></extra>",
        )
    )

    # Importaciones line
    fig_evol.add_trace(
        go.Scatter(
            x=df_evol["fecha"],
            y=df_evol["importaciones"],
            name="Importaciones",
            line=dict(color="#EF553B", width=3),
            mode="lines+markers",
            yaxis="y",
            customdata=imp_formatted,
            hovertemplate="<b>Importaciones</b><br>%{customdata}<extra></extra>",
        )
    )

    # Balance bars
    fig_evol.add_trace(
        go.Bar(
            x=df_evol["fecha"],
            y=df_evol["balance"],
            name="Balance Comercial",
            marker_color=[
                "#00CC96" if v >= 0 else "#EF553B" for v in df_evol["balance"]
            ],
            opacity=0.6,
            yaxis="y2",
            customdata=bal_formatted,
            hovertemplate="<b>Balance</b><br>%{customdata}<extra></extra>",
        )
    )

    fig_evol.update_layout(
        height=220,
        margin=dict(l=40, r=40, t=30, b=30),
        legend=dict(orientation="h", y=1.12, x=0, font=dict(size=10)),
        yaxis=dict(title="Comercio Total (€)", tickfont=dict(size=10), showgrid=True),
        yaxis2=dict(
            title="Balance Comercial (€)",
            overlaying="y",
            side="right",
            showgrid=False,
            tickfont=dict(size=10),
        ),
        xaxis=dict(tickfont=dict(size=10)),
        hovermode="x unified",
        bargap=0.3,
    )

    return fig_evol


def create_bump_chart(partners_data, flow_type, fecha_inicio, fecha_fin):
    """Create bump chart showing rank evolution of trading partners.

    Args:
        partners_data: Dictionary with 'imports' and 'exports' DataFrames.
        flow_type: 'Exportaciones' or 'Importaciones'.
        fecha_inicio: Start date for filtering.
        fecha_fin: End date for filtering.

    Returns:
        Plotly Figure with bump chart, or None if no data.
    """
    if partners_data is None:
        return None

    df_src = partners_data[
        "exports" if flow_type == "Exportaciones" else "imports"
    ].copy()
    df_src = df_src[(df_src["fecha"] >= fecha_inicio) & (df_src["fecha"] <= fecha_fin)]

    df_bump = df_src.groupby(["partner", "fecha"])["OBS_VALUE"].sum().reset_index()
    df_bump["rank"] = df_bump.groupby("fecha")["OBS_VALUE"].rank(
        ascending=False, method="min"
    )

    # Only show positions 1-10
    df_top10 = df_bump[df_bump["rank"] <= 10].copy()

    # All partners that were ever in top 10
    all_dates = sorted(df_bump["fecha"].unique())

    # Sort by total volume for consistent colors
    partner_totals = (
        df_top10.groupby("partner")["OBS_VALUE"].sum().sort_values(ascending=False)
    )
    partners_ordered = partner_totals.index.tolist()

    colors = px.colors.qualitative.Set1 + px.colors.qualitative.Set2

    fig_bump = go.Figure()

    for i, partner in enumerate(partners_ordered):
        df_p = df_top10[df_top10["partner"] == partner].set_index("fecha")

        # Reindex to all dates (creates NaN where no data)
        df_p_full = df_p.reindex(all_dates)

        color = colors[i % len(colors)]
        label = format_partner_name(partner)

        # Line (with gaps where NaN)
        fig_bump.add_trace(
            go.Scatter(
                x=all_dates,
                y=df_p_full["rank"],
                mode="lines",
                name=label,
                line=dict(color=color, width=2),
                hoverinfo="skip",
                showlegend=True,
                connectgaps=False,
            )
        )

        # Markers with numbers (only where data exists)
        df_p_valid = df_p.reset_index()
        fechas_fmt = [d.strftime("%b %Y") for d in df_p_valid["fecha"]]
        fig_bump.add_trace(
            go.Scatter(
                x=df_p_valid["fecha"],
                y=df_p_valid["rank"],
                mode="markers+text",
                marker=dict(size=16, color=color),
                text=[str(int(r)) for r in df_p_valid["rank"]],
                textposition="middle center",
                textfont=dict(size=8, color="white"),
                hovertemplate=f"<b>{label}</b><br>%{{customdata[1]}}<br>Rank: %{{y}}<br>€%{{customdata[0]:,.0f}}<extra></extra>",
                customdata=list(zip(df_p_valid["OBS_VALUE"], fechas_fmt)),
                showlegend=False,
            )
        )

    fig_bump.update_layout(
        height=380,
        margin=dict(l=30, r=10, t=40, b=70),
        yaxis=dict(
            range=[10.7, 0.3],  # Inverted: 1 at top, 10 at bottom
            dtick=1,
            tickvals=list(range(1, 11)),
            title="",
            tickfont=dict(size=10),
        ),
        xaxis=dict(tickfont=dict(size=9), tickangle=-45),
        legend=dict(
            orientation="h",
            y=-0.22,
            x=0,
            font=dict(size=8),
        ),
        hovermode="closest",
    )

    return fig_bump


def create_sunburst_chart(df_sectores, flow_type="importaciones"):
    """Create enhanced sunburst chart with hierarchical colors and formatting.

    Args:
        df_sectores: DataFrame with sector and trade flow columns.
        flow_type: 'importaciones' or 'exportaciones'.

    Returns:
        Plotly Figure with sunburst chart, or None if no data.
    """
    # Map sector name to SITC code
    NOMBRE_A_SITC = {v: k for k, v in SECTORES_SITC.items() if k != "TOTAL"}

    df_grp = df_sectores.groupby("sector")[[flow_type]].sum().reset_index()
    df_grp["sitc"] = df_grp["sector"].map(NOMBRE_A_SITC)
    df_grp = df_grp.dropna(subset=["sitc"])
    df_grp = df_grp[df_grp[flow_type] > 0]

    if df_grp.empty:
        return None

    # Build hierarchical structure
    ids = []
    labels = []
    parents = []
    values = []

    # Group categories
    categories = {}
    for _, row in df_grp.iterrows():
        sitc_code = row["sitc"]
        grupo = SECTOR_A_GRUPO.get(sitc_code, "Otros")
        if grupo not in categories:
            categories[grupo] = 0
        categories[grupo] += row[flow_type]

    grand_total = sum(categories.values())

    # Add category level
    for category, category_value in categories.items():
        ids.append(category)
        labels.append(category)
        parents.append("")
        values.append(category_value)

    # Add sector level
    for _, row in df_grp.iterrows():
        sitc_code = row["sitc"]
        grupo = SECTOR_A_GRUPO.get(sitc_code, "Otros")
        sector_id = f"{grupo}_{row['sector']}"
        ids.append(sector_id)
        labels.append(row["sector"])
        parents.append(grupo)
        values.append(row[flow_type])

    # Format values for hover
    formatted_values = [format_currency(val) for val in values]
    percentages = [
        (val / grand_total * 100) if grand_total > 0 else 0 for val in values
    ]
    customdata = [
        [formatted_values[i], f"{percentages[i]:.1f}%"] for i in range(len(values))
    ]

    # Assign colors
    segment_colors = []
    category_subcategory_count = {}

    for i, parent in enumerate(parents):
        if parent == "":  # Main categories
            color = SUNBURST_BASE_COLORS.get(labels[i], "#8B8C89")
            segment_colors.append(color)
            category_subcategory_count[labels[i]] = 0
        else:  # Subcategories
            parent_color = SUNBURST_BASE_COLORS.get(parent, "#8B8C89")
            count = category_subcategory_count.get(parent, 0)
            category_subcategory_count[parent] = count + 1
            if count % 2 == 0:
                segment_colors.append(
                    lighten_color(parent_color, 0.25 + (count // 2) * 0.1)
                )
            else:
                segment_colors.append(
                    darken_color(parent_color, 0.15 + (count // 2) * 0.05)
                )

    # Create text labels
    text_labels = []
    for i, (label, val, pct, parent) in enumerate(
        zip(labels, values, percentages, parents)
    ):
        if parent == "":
            text_labels.append(f"{label}<br>{pct:.1f}%<br>{format_value_short(val)}")
        else:
            text_labels.append(f"{label}")

    fig = go.Figure(
        go.Sunburst(
            ids=ids,
            labels=labels,
            parents=parents,
            values=values,
            branchvalues="total",
            customdata=customdata,
            text=text_labels,
            textinfo="text",
            textfont=dict(size=9, family="Arial, sans-serif", color="white"),
            hovertemplate="<b>%{label}</b><br>Valor: %{customdata[0]}<br>Porcentaje: %{customdata[1]}<br><extra></extra>",
            insidetextorientation="radial",
            marker=dict(colors=segment_colors, line=dict(color="white", width=2)),
        )
    )

    fig.update_layout(
        height=300,
        margin=dict(l=5, r=5, t=5, b=5),
        uniformtext=dict(minsize=7),
    )

    return fig
