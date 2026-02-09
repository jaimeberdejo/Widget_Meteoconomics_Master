import warnings
import sys
import logging

warnings.filterwarnings('ignore')
logging.getLogger('plotly').setLevel(logging.ERROR)

import io
sys.stderr = io.StringIO()

import streamlit as st
import pandas as pd

sys.stderr = sys.__stderr__

from src.config import PAISES_V1, MONEDA_PAIS, CUSTOM_CSS
from src.utils import format_currency
from src.data_loader import load_goods_data, load_partners_data
from src.charts import create_evolution_chart, create_bump_chart, create_sunburst_chart

# --- CONFIGURACION DE PAGINA ---
st.set_page_config(page_title="Balanza Comercial", page_icon="🌍", layout="wide")

st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

# --- CARGA DE DATOS ---
df_goods = load_goods_data()
NOMBRE_A_CODIGO = {v: k for k, v in PAISES_V1.items()}
paises_disponibles = [p for p in PAISES_V1.values() if p in df_goods['pais'].unique()]

if not paises_disponibles:
    st.error("Sin datos. Ejecuta ETL.")
    st.stop()

# ============================================================
# LAYOUT
# ============================================================

st.title("🌍 Balanza Comercial")

# --- Fila de configuración: País | Desde | Hasta | CSV ---
col_country, col_date1, col_date2, col_download = st.columns([1.8, 1.2, 1.2, 1.3])

with col_country:
    pais_sel = st.selectbox(
        "📍 País",
        paises_disponibles,
        index=paises_disponibles.index('España') if 'España' in paises_disponibles else 0,
    )

country_code = NOMBRE_A_CODIGO.get(pais_sel)
currency_symbol = MONEDA_PAIS.get(country_code, '€')
df_pais = df_goods[df_goods['pais'] == pais_sel].sort_values('fecha')

# --- Rango de fechas ---
fecha_min_data = df_pais['fecha'].min().date()
fecha_max_data = df_pais['fecha'].max().date()
fecha_default_inicio = (df_pais['fecha'].max() - pd.DateOffset(months=11)).date()
if fecha_default_inicio < fecha_min_data:
    fecha_default_inicio = fecha_min_data

with col_date1:
    fecha_inicio = st.date_input(
        "📅 Desde",
        value=fecha_default_inicio,
        min_value=fecha_min_data, max_value=fecha_max_data,
        format="MM/YYYY",
    )

with col_date2:
    fecha_fin = st.date_input(
        "Hasta",
        value=fecha_max_data,
        min_value=fecha_min_data, max_value=fecha_max_data,
        format="MM/YYYY",
    )

# Convertir a datetime para filtrar
fecha_inicio = pd.to_datetime(fecha_inicio)
fecha_fin = pd.to_datetime(fecha_fin)

# Filtrar por rango
df_rango = df_pais[(df_pais['fecha'] >= fecha_inicio) & (df_pais['fecha'] <= fecha_fin)]

# Preparar CSV download
df_download = df_rango[df_rango['sector'] != 'Total Comercio'][
    ['fecha', 'pais', 'sector', 'exportaciones', 'importaciones']
].copy()
df_download['fecha'] = df_download['fecha'].dt.strftime('%Y-%m')
csv_data = df_download.to_csv(index=False).encode('utf-8')

with col_download:
    st.write("")
    st.download_button(
        "📥 CSV", csv_data,
        file_name=f"balanza_{country_code}.csv",
        mime="text/csv",
        help="Descargar datos del período",
        use_container_width=True,
    )

# --- KPIs ---
df_total = df_rango[df_rango['sector'] == 'Total Comercio']
if df_total.empty:
    df_total = df_rango
tot_exp = df_total['exportaciones'].sum()
tot_imp = df_total['importaciones'].sum()
tot_bal = tot_exp - tot_imp
cobertura = (tot_exp / tot_imp * 100) if tot_imp > 0 else 0

col_k1, col_k2, col_k3, col_k4 = st.columns(4)

with col_k1:
    st.metric(
        "💰 Exportaciones", format_currency(tot_exp, currency_symbol),
        help="Total de bienes vendidos al exterior"
    )
with col_k2:
    st.metric(
        "🛒 Importaciones", format_currency(tot_imp, currency_symbol),
        help="Total de bienes comprados del exterior"
    )
with col_k3:
    st.metric(
        "⚖️ Balanza Comercial", format_currency(tot_bal, currency_symbol),
        help="Exportaciones - Importaciones. Positivo = superávit, Negativo = déficit"
    )
with col_k4:
    st.metric(
        "📊 Tasa de Cobertura", f"{cobertura:.1f}%",
        help="Exportaciones ÷ Importaciones. >100% = superávit, <100% = déficit"
    )

st.markdown("---")

# --- CONTENIDO: 2 columnas ---
col_left, col_right = st.columns([2.5, 2])

# === COLUMNA IZQUIERDA ===
with col_left:
    st.markdown("##### Evolución Mensual")
    fig_evol = create_evolution_chart(df_total, currency_symbol)
    st.plotly_chart(fig_evol, use_container_width=True, config={"displayModeBar": False})

    # --- Bump Chart Socios ---
    st.markdown("##### Evolución Top Socios Comerciales")
    bump_flow = st.radio(
        "Flujo", ["Exportaciones", "Importaciones"],
        horizontal=True, key="bump", label_visibility="collapsed"
    )

    partners_data = load_partners_data(country_code)
    fig_bump = create_bump_chart(
        partners_data, bump_flow, fecha_inicio, fecha_fin, currency_symbol
    )

    if fig_bump:
        st.plotly_chart(fig_bump, use_container_width=True, config={"displayModeBar": False})
    else:
        st.info("Sin datos de socios")

# === COLUMNA DERECHA: Sunbursts ===
with col_right:
    df_sectores = df_rango[df_rango['sector'] != 'Total Comercio'].copy()

    st.markdown("##### Importaciones")
    fig_imp = create_sunburst_chart(df_sectores, 'importaciones', currency_symbol)
    if fig_imp:
        st.plotly_chart(fig_imp, use_container_width=True, config={"displayModeBar": False})
    else:
        st.warning("Sin datos de importaciones por sector")

    st.markdown("##### Exportaciones")
    fig_exp = create_sunburst_chart(df_sectores, 'exportaciones', currency_symbol)
    if fig_exp:
        st.plotly_chart(fig_exp, use_container_width=True, config={"displayModeBar": False})
    else:
        st.warning("Sin datos de exportaciones por sector")
