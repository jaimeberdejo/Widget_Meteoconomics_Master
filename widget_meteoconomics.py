import warnings
import sys
import logging

warnings.filterwarnings("ignore")
logging.getLogger("plotly").setLevel(logging.ERROR)

import io

sys.stderr = io.StringIO()

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime

sys.stderr = sys.__stderr__

# Configuración de rutas por país
from pathlib import Path

DATA_FOLDERS = {
    "eu": Path("data/eu"),  # DE, ES, FR, IT
    "us": Path("data/us"),  # US
    "uk": Path("data/uk"),  # GB
    "jp": Path("data/jp"),  # JP
    "ca": Path("data/ca"),  # CA
}


def update_data_if_needed():
    return False


# --- CONFIGURACION DE PAGINA ---
st.set_page_config(page_title="Balanza Comercial", page_icon="🌍", layout="wide")

# --- CSS COMPACTO ---
st.markdown(
    """
<style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    .block-container {
        padding: 1rem 2rem !important;
        max-width: 1100px !important;
    }
    [data-testid="stMetric"] {
        background: transparent;
        padding: 0.2rem 0.5rem !important;
    }
    [data-testid="stMetricLabel"] { font-size: 0.8rem !important; }
    [data-testid="stMetricValue"] { font-size: 1.2rem !important; }
    h3 { font-size: 1rem !important; margin: 0.3rem 0 !important; }
    h5 { font-size: 0.9rem !important; margin: 0.5rem 0 0.3rem 0 !important; }
    .element-container { margin-bottom: 0.2rem !important; }
</style>
""",
    unsafe_allow_html=True,
)

# --- DATOS ---
# Países disponibles para selección (EU + nuevos países)
PAISES_V1 = {
    # EU (Eurostat)
    "ES": "España",
    "FR": "Francia",
    "DE": "Alemania",
    "IT": "Italia",
    # Non-EU (nuevas fuentes)
    "US": "Estados Unidos",
    "GB": "Reino Unido",
    "JP": "Japón",
    "CA": "Canadá",
}

BANDERAS = {
    "ES": "🇪🇸",
    "FR": "🇫🇷",
    "DE": "🇩🇪",
    "IT": "🇮🇹",
    "GB": "🇬🇧",
    "AT": "🇦🇹",
    "BE": "🇧🇪",
    "BG": "🇧🇬",
    "HR": "🇭🇷",
    "CY": "🇨🇾",
    "CZ": "🇨🇿",
    "DK": "🇩🇰",
    "EE": "🇪🇪",
    "FI": "🇫🇮",
    "GR": "🇬🇷",
    "HU": "🇭🇺",
    "IE": "🇮🇪",
    "LV": "🇱🇻",
    "LT": "🇱🇹",
    "LU": "🇱🇺",
    "MT": "🇲🇹",
    "NL": "🇳🇱",
    "PL": "🇵🇱",
    "PT": "🇵🇹",
    "RO": "🇷🇴",
    "SK": "🇸🇰",
    "SI": "🇸🇮",
    "SE": "🇸🇪",
    "NO": "🇳🇴",
    "CH": "🇨🇭",
    "CN": "🇨🇳",
    "US": "🇺🇸",
    "TR": "🇹🇷",
    "RU": "🇷🇺",
    "JP": "🇯🇵",
    "IN": "🇮🇳",
    "KR": "🇰🇷",
    "BR": "🇧🇷",
    "MX": "🇲🇽",
    "CA": "🇨🇦",
    "AU": "🇦🇺",
    "SA": "🇸🇦",
    "SG": "🇸🇬",
    "VN": "🇻🇳",
    "UA": "🇺🇦",
    "TW": "🇹🇼",
    "CL": "🇨🇱",
    "UK": "🇬🇧",
}

PAISES_NOMBRE = {
    "AT": "Austria",
    "BE": "Bélgica",
    "BG": "Bulgaria",
    "HR": "Croacia",
    "CZ": "Rep. Checa",
    "DK": "Dinamarca",
    "FI": "Finlandia",
    "FR": "Francia",
    "DE": "Alemania",
    "GR": "Grecia",
    "HU": "Hungría",
    "IE": "Irlanda",
    "IT": "Italia",
    "NL": "P. Bajos",
    "PL": "Polonia",
    "PT": "Portugal",
    "RO": "Rumanía",
    "SK": "Eslovaquia",
    "SI": "Eslovenia",
    "ES": "España",
    "SE": "Suecia",
    "GB": "R. Unido",
    "UK": "R. Unido",
    "CH": "Suiza",
    "NO": "Noruega",
    "CN": "China",
    "US": "EE.UU.",
    "TR": "Turquía",
    "RU": "Rusia",
    "JP": "Japón",
    "IN": "India",
    "KR": "Corea S.",
    "BR": "Brasil",
    "MX": "México",
    "CA": "Canadá",
    "AU": "Australia",
    "SA": "A. Saudita",
    "SG": "Singapur",
    "VN": "Vietnam",
    "UA": "Ucrania",
    "TW": "Taiwán",
    "CL": "Chile",
}

# Sectores SITC - DEBEN COINCIDIR con etl_data.py
SECTORES_SITC = {
    "TOTAL": "Total Comercio",
    "0": "Alimentos y animales vivos",
    "1": "Bebidas y tabaco",
    "2": "Materiales crudos",
    "3": "Combustibles minerales",
    "4": "Aceites y grasas",
    "5": "Productos químicos",
    "6": "Manufacturas por material",
    "7": "Maquinaria y transporte",
    "8": "Manufacturas diversas",
    "9": "Otros",
}

GRUPOS_SUNBURST = {
    "Agro y Alimentos": ["0", "1", "4"],
    "Minería y Energía": ["2", "3"],
    "Químicos": ["5"],
    "Manufacturas": ["6", "7", "8"],
    "Otros": ["9"],
}

SECTOR_A_GRUPO = {}
for grupo, sectores in GRUPOS_SUNBURST.items():
    for s in sectores:
        SECTOR_A_GRUPO[s] = grupo

COLOR_MAP = {
    "Agro y Alimentos": "#2ca02c",
    "Minería y Energía": "#8B4513",
    "Químicos": "#1f77b4",
    "Manufacturas": "#17becf",
    "Otros": "#7f7f7f",
}


# --- FUNCIONES ---
def format_currency(value):
    if abs(value) >= 1_000_000_000_000:
        return f"€{value / 1_000_000_000_000:.2f} T"
    elif abs(value) >= 1_000_000_000:
        return f"€{value / 1_000_000_000:.2f} B"
    elif abs(value) >= 1_000_000:
        return f"€{value / 1_000_000:.0f} M"
    return f"€{value:,.0f}"


def format_partner_name(code):
    nombre = PAISES_NOMBRE.get(code, code)
    bandera = BANDERAS.get(code, "")
    return f"{bandera} {nombre}"


@st.cache_data(ttl=3600)
def load_goods_data():
    """Carga datos de bienes de todas las carpetas de países."""
    all_dfs = []

    for folder_name, folder_path in DATA_FOLDERS.items():
        file_path = folder_path / "bienes_agregado.csv"
        if file_path.exists():
            try:
                df = pd.read_csv(file_path)
                df["fecha"] = pd.to_datetime(df["fecha"])
                all_dfs.append(df)
            except Exception:
                pass

    if not all_dfs:
        st.error("No hay datos. Ejecuta los ETLs primero.")
        st.stop()

    return pd.concat(all_dfs, ignore_index=True)


@st.cache_data(ttl=3600)
def load_partners_data(country_code):
    """Carga datos de socios comerciales para un país."""
    # Determinar qué carpeta usar
    if country_code in ["DE", "ES", "FR", "IT"]:
        folder = DATA_FOLDERS["eu"]
    elif country_code == "US":
        folder = DATA_FOLDERS["us"]
    elif country_code == "GB":
        folder = DATA_FOLDERS["uk"]
    elif country_code == "JP":
        folder = DATA_FOLDERS["jp"]
    elif country_code == "CA":
        folder = DATA_FOLDERS["ca"]
    else:
        return None

    file_path = folder / "comercio_socios.csv"
    if not file_path.exists():
        return None

    try:
        df = pd.read_csv(file_path)
        df["fecha"] = pd.to_datetime(df["fecha"])
        df_c = df[df["pais_code"] == country_code]
        if df_c.empty:
            return None
        return {
            "imports": df_c[["fecha", "socio_code", "importaciones"]].rename(
                columns={"socio_code": "partner", "importaciones": "OBS_VALUE"}
            ),
            "exports": df_c[["fecha", "socio_code", "exportaciones"]].rename(
                columns={"socio_code": "partner", "exportaciones": "OBS_VALUE"}
            ),
        }
    except Exception:
        return None


# --- CARGA DE DATOS ---
df_goods = load_goods_data()
NOMBRE_A_CODIGO = {v: k for k, v in PAISES_V1.items()}
paises_disponibles = [p for p in PAISES_V1.values() if p in df_goods["pais"].unique()]

if not paises_disponibles:
    st.error("Sin datos. Ejecuta ETL.")
    st.stop()

# ============================================================
# LAYOUT
# ============================================================

st.title("🌍 Balanza Comercial")

# --- CONFIGURATION ROW ---
col_country, col_date1, col_date2, col_download = st.columns([1.8, 1.2, 1.2, 1.3])

with col_country:
    pais_sel = st.selectbox(
        "📍 País",
        paises_disponibles,
        index=paises_disponibles.index("España")
        if "España" in paises_disponibles
        else 0,
    )

country_code = NOMBRE_A_CODIGO.get(pais_sel)
df_pais = df_goods[df_goods["pais"] == pais_sel].sort_values("fecha")

# Get date range from data
fecha_min_data = df_pais["fecha"].min().date()
fecha_max_data = df_pais["fecha"].max().date()
fecha_default_inicio = (df_pais["fecha"].max() - pd.DateOffset(months=11)).date()
if fecha_default_inicio < fecha_min_data:
    fecha_default_inicio = fecha_min_data

with col_date1:
    fecha_inicio = st.date_input(
        "📅 Desde",
        value=fecha_default_inicio,
        min_value=fecha_min_data,
        max_value=fecha_max_data,
        format="DD/MM/YYYY",
    )

with col_date2:
    fecha_fin = st.date_input(
        "Hasta",
        value=fecha_max_data,
        min_value=fecha_min_data,
        max_value=fecha_max_data,
        format="DD/MM/YYYY",
    )

# Convert to datetime for filtering
fecha_inicio = pd.to_datetime(fecha_inicio)
fecha_fin = pd.to_datetime(fecha_fin)

# Filtrar por rango
df_rango = df_pais[(df_pais["fecha"] >= fecha_inicio) & (df_pais["fecha"] <= fecha_fin)]

# Prepare CSV download
df_download = df_rango[df_rango["sector"] != "Total Comercio"][
    ["fecha", "pais", "sector", "exportaciones", "importaciones"]
]
df_download["fecha"] = df_download["fecha"].dt.strftime("%Y-%m")
csv_data = df_download.to_csv(index=False).encode("utf-8")

with col_download:
    st.write("")  # Vertical spacing to align with inputs
    st.download_button(
        "📥 CSV",
        csv_data,
        file_name=f"balanza_{country_code}.csv",
        mime="text/csv",
        help="Descargar datos del período",
        use_container_width=True,
    )

st.markdown("---")

# --- KPIs ROW ---
# Calcular KPIs
df_total = df_rango[df_rango["sector"] == "Total Comercio"]
if df_total.empty:
    df_total = df_rango
tot_exp = df_total["exportaciones"].sum()
tot_imp = df_total["importaciones"].sum()
tot_bal = tot_exp - tot_imp
cobertura = (tot_exp / tot_imp * 100) if tot_imp > 0 else 0

col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns(4)

with col_kpi1:
    st.metric(
        "💰 Exportaciones",
        format_currency(tot_exp),
        help="Total de bienes vendidos al exterior",
    )
with col_kpi2:
    st.metric(
        "🛒 Importaciones",
        format_currency(tot_imp),
        help="Total de bienes comprados del exterior",
    )
with col_kpi3:
    st.metric(
        "⚖️ Balanza Comercial",
        format_currency(tot_bal),
        help="Exportaciones - Importaciones. Positivo = superávit, Negativo = déficit",
    )
with col_kpi4:
    st.metric(
        "📊 Tasa de Cobertura",
        f"{cobertura:.1f}%",
        help="Exportaciones ÷ Importaciones. >100% = superávit, <100% = déficit",
    )

# --- CONTENIDO: 2 columnas ---
col_left, col_right = st.columns([2.5, 2])

# === COLUMNA IZQUIERDA ===
with col_left:
    # --- Evolucion Mensual ---
    st.markdown("##### Evolución Mensual")

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
    st.plotly_chart(
        fig_evol, use_container_width=True, config={"displayModeBar": False}
    )

    # --- Bump Chart Socios ---
    st.markdown("##### Evolución Top Socios Comerciales")

    bump_flow = st.radio(
        "Flujo",
        ["Exportaciones", "Importaciones"],
        horizontal=True,
        key="bump",
        label_visibility="collapsed",
    )

    partners_data = load_partners_data(country_code)

    if partners_data:
        df_src = partners_data[
            "exports" if bump_flow == "Exportaciones" else "imports"
        ].copy()
        df_src = df_src[
            (df_src["fecha"] >= fecha_inicio) & (df_src["fecha"] <= fecha_fin)
        ]

        df_bump = df_src.groupby(["partner", "fecha"])["OBS_VALUE"].sum().reset_index()
        df_bump["rank"] = df_bump.groupby("fecha")["OBS_VALUE"].rank(
            ascending=False, method="min"
        )

        # Solo mostrar posiciones 1-10 (filtrar ranks > 10)
        df_top10 = df_bump[df_bump["rank"] <= 10].copy()

        # Todos los socios que alguna vez estuvieron en top 10
        partners_in_top10 = df_top10["partner"].unique().tolist()

        # Crear serie temporal completa para cada partner (con NaN donde no esta en top 10)
        all_dates = sorted(df_bump["fecha"].unique())

        # Ordenar por volumen total para colores consistentes
        partner_totals = (
            df_top10.groupby("partner")["OBS_VALUE"].sum().sort_values(ascending=False)
        )
        partners_ordered = partner_totals.index.tolist()

        colors = px.colors.qualitative.Set1 + px.colors.qualitative.Set2

        fig_bump = go.Figure()

        for i, partner in enumerate(partners_ordered):
            df_p = df_top10[df_top10["partner"] == partner].set_index("fecha")

            # Reindexar a todas las fechas (crea NaN donde no hay datos)
            df_p_full = df_p.reindex(all_dates)

            color = colors[i % len(colors)]
            label = format_partner_name(partner)

            # Linea (con gaps donde hay NaN)
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

            # Markers con numero (solo donde hay datos)
            df_p_valid = df_p.reset_index()
            # Formatear fechas para hover
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
                range=[10.7, 0.3],  # Invertido: 1 arriba, 10 abajo, más espacio
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
        st.plotly_chart(
            fig_bump, use_container_width=True, config={"displayModeBar": False}
        )
    else:
        st.info("Sin datos de socios")

# === COLUMNA DERECHA: Sunbursts ===
with col_right:
    df_sectores = df_rango[df_rango["sector"] != "Total Comercio"].copy()

    # Mapeo sector nombre -> codigo SITC
    NOMBRE_A_SITC = {v: k for k, v in SECTORES_SITC.items() if k != "TOTAL"}

    def create_enhanced_sunburst(df_data, flow_type="importaciones"):
        """Create enhanced sunburst with better colors and formatting."""
        df_grp = df_data.groupby("sector")[[flow_type]].sum().reset_index()
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

        # Enhanced color scheme
        base_colors = {
            "Agro y Alimentos": "#2E86AB",
            "Minería y Energía": "#F18F01",
            "Químicos": "#C73E1D",
            "Manufacturas": "#6A994E",
            "Otros": "#8B8C89",
        }

        def lighten_color(hex_color, factor=0.3):
            hex_color = hex_color.lstrip("#")
            r, g, b = (
                int(hex_color[0:2], 16),
                int(hex_color[2:4], 16),
                int(hex_color[4:6], 16),
            )
            r = int(r + (255 - r) * factor)
            g = int(g + (255 - g) * factor)
            b = int(b + (255 - b) * factor)
            return f"#{r:02x}{g:02x}{b:02x}"

        def darken_color(hex_color, factor=0.2):
            hex_color = hex_color.lstrip("#")
            r, g, b = (
                int(hex_color[0:2], 16),
                int(hex_color[2:4], 16),
                int(hex_color[4:6], 16),
            )
            r = int(r * (1 - factor))
            g = int(g * (1 - factor))
            b = int(b * (1 - factor))
            return f"#{r:02x}{g:02x}{b:02x}"

        # Assign colors
        segment_colors = []
        category_subcategory_count = {}

        for i, (id_val, parent) in enumerate(zip(ids, parents)):
            if parent == "":  # Main categories
                color = base_colors.get(labels[i], "#8B8C89")
                segment_colors.append(color)
                category_subcategory_count[labels[i]] = 0
            else:  # Subcategories
                parent_color = base_colors.get(parent, "#8B8C89")
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
        def format_value_short(val):
            if val >= 1e9:
                return f"€{val / 1e9:.1f}B"
            elif val >= 1e6:
                return f"€{val / 1e6:.0f}M"
            elif val >= 1e3:
                return f"€{val / 1e3:.0f}K"
            else:
                return f"€{val:.0f}"

        text_labels = []
        for i, (label, val, pct, parent) in enumerate(
            zip(labels, values, percentages, parents)
        ):
            if parent == "":
                text_labels.append(
                    f"{label}<br>{pct:.1f}%<br>{format_value_short(val)}"
                )
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

    # --- Sunburst Importaciones ---
    st.markdown("##### Importaciones")
    fig_imp = create_enhanced_sunburst(df_sectores, "importaciones")
    if fig_imp:
        st.plotly_chart(
            fig_imp, use_container_width=True, config={"displayModeBar": False}
        )
    else:
        st.warning("Sin datos de importaciones por sector")

    # --- Sunburst Exportaciones ---
    st.markdown("##### Exportaciones")
    fig_exp = create_enhanced_sunburst(df_sectores, "exportaciones")
    if fig_exp:
        st.plotly_chart(
            fig_exp, use_container_width=True, config={"displayModeBar": False}
        )
    else:
        st.warning("Sin datos de exportaciones por sector")
