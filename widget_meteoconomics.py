import warnings
import sys
import logging

# Suprimir warnings de deprecacion
warnings.filterwarnings('ignore')
logging.getLogger('plotly').setLevel(logging.ERROR)

import io
sys.stderr = io.StringIO()

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import os
from datetime import datetime

sys.stderr = sys.__stderr__

# Importar desde etl_data
try:
    from etl_data import update_data_if_needed, FILE_BIENES_AGREGADO, FILE_COMERCIO_SOCIOS
except ImportError:
    from pathlib import Path
    FILE_BIENES_AGREGADO = Path('data/bienes_agregado.csv')
    FILE_COMERCIO_SOCIOS = Path('data/comercio_socios.csv')
    def update_data_if_needed(): return False

# --- CONFIGURACION DE PAGINA ---
st.set_page_config(
    page_title="Meteoconomics - Balanza Comercial",
    page_icon="🌍",
    layout="wide"
)

# --- PAISES V1 (solo europeos Eurostat) ---
PAISES_V1 = {
    'ES': 'España',
    'FR': 'Francia',
    'DE': 'Alemania',
    'IT': 'Italia',
}

BANDERAS = {
    'ES': '🇪🇸', 'FR': '🇫🇷', 'DE': '🇩🇪', 'IT': '🇮🇹', 'GB': '🇬🇧',
    'AT': '🇦🇹', 'BE': '🇧🇪', 'BG': '🇧🇬', 'HR': '🇭🇷', 'CY': '🇨🇾',
    'CZ': '🇨🇿', 'DK': '🇩🇰', 'EE': '🇪🇪', 'FI': '🇫🇮', 'GR': '🇬🇷',
    'HU': '🇭🇺', 'IE': '🇮🇪', 'LV': '🇱🇻', 'LT': '🇱🇹', 'LU': '🇱🇺',
    'MT': '🇲🇹', 'NL': '🇳🇱', 'PL': '🇵🇱', 'PT': '🇵🇹', 'RO': '🇷🇴',
    'SK': '🇸🇰', 'SI': '🇸🇮', 'SE': '🇸🇪', 'NO': '🇳🇴', 'CH': '🇨🇭',
    'IS': '🇮🇸', 'CN': '🇨🇳', 'US': '🇺🇸', 'TR': '🇹🇷', 'RU': '🇷🇺',
    'JP': '🇯🇵', 'IN': '🇮🇳', 'KR': '🇰🇷', 'BR': '🇧🇷', 'MX': '🇲🇽',
    'CA': '🇨🇦', 'AU': '🇦🇺', 'SA': '🇸🇦', 'AE': '🇦🇪', 'ZA': '🇿🇦',
    'SG': '🇸🇬', 'TH': '🇹🇭', 'MY': '🇲🇾', 'ID': '🇮🇩', 'VN': '🇻🇳',
    'PH': '🇵🇭', 'AR': '🇦🇷', 'CL': '🇨🇱', 'CO': '🇨🇴', 'PE': '🇵🇪',
    'EG': '🇪🇬', 'NG': '🇳🇬', 'KE': '🇰🇪', 'MA': '🇲🇦', 'DZ': '🇩🇿',
    'TN': '🇹🇳', 'IL': '🇮🇱', 'IQ': '🇮🇶', 'IR': '🇮🇷', 'PK': '🇵🇰',
    'BD': '🇧🇩', 'UA': '🇺🇦', 'BY': '🇧🇾', 'KZ': '🇰🇿', 'NZ': '🇳🇿',
    'UK': '🇬🇧', 'TW': '🇹🇼',
}

PAISES_NOMBRE = {
    'AT': 'Austria', 'BE': 'Bélgica', 'BG': 'Bulgaria', 'HR': 'Croacia',
    'CY': 'Chipre', 'CZ': 'República Checa', 'DK': 'Dinamarca', 'EE': 'Estonia',
    'FI': 'Finlandia', 'FR': 'Francia', 'DE': 'Alemania', 'GR': 'Grecia',
    'HU': 'Hungría', 'IE': 'Irlanda', 'IT': 'Italia', 'LV': 'Letonia',
    'LT': 'Lituania', 'LU': 'Luxemburgo', 'MT': 'Malta', 'NL': 'Países Bajos',
    'PL': 'Polonia', 'PT': 'Portugal', 'RO': 'Rumanía', 'SK': 'Eslovaquia',
    'SI': 'Eslovenia', 'ES': 'España', 'SE': 'Suecia',
    'GB': 'Reino Unido', 'CH': 'Suiza', 'NO': 'Noruega', 'IS': 'Islandia',
    'CN': 'China', 'US': 'Estados Unidos', 'TR': 'Turquía', 'RU': 'Rusia',
    'JP': 'Japón', 'IN': 'India', 'KR': 'Corea del Sur', 'BR': 'Brasil',
    'MX': 'México', 'CA': 'Canadá', 'AU': 'Australia', 'SA': 'Arabia Saudita',
    'AE': 'EAU', 'ZA': 'Sudáfrica', 'SG': 'Singapur', 'TH': 'Tailandia',
    'MY': 'Malasia', 'ID': 'Indonesia', 'VN': 'Vietnam', 'PH': 'Filipinas',
    'AR': 'Argentina', 'CL': 'Chile', 'CO': 'Colombia', 'PE': 'Perú',
    'EG': 'Egipto', 'NG': 'Nigeria', 'KE': 'Kenia', 'MA': 'Marruecos',
    'DZ': 'Argelia', 'TN': 'Túnez', 'IL': 'Israel', 'IQ': 'Irak',
    'IR': 'Irán', 'PK': 'Pakistán', 'BD': 'Bangladesh', 'UA': 'Ucrania',
    'BY': 'Bielorrusia', 'KZ': 'Kazajistán', 'NZ': 'Nueva Zelanda',
    'UK': 'Reino Unido', 'TW': 'Taiwán',
}

SECTORES_SITC = {
    'TOTAL': 'Total Comercio',
    '0': 'Alimentos y animales vivos',
    '1': 'Bebidas y tabaco',
    '2': 'Materiales crudos',
    '3': 'Combustibles minerales',
    '4': 'Aceites y grasas',
    '5': 'Productos químicos',
    '6': 'Manufacturas por material',
    '7': 'Maquinaria y transporte',
    '8': 'Manufacturas diversas',
    '9': 'Otros'
}

# Agrupacion para sunburst
GRUPOS_SUNBURST = {
    'Agro y Alimentos': ['0', '1', '4'],
    'Materias Primas y Energía': ['2', '3'],
    'Químicos': ['5'],
    'Manufacturas': ['6', '7', '8'],
    'Otros': ['9'],
}

# Mapeo inverso: sector -> grupo
SECTOR_A_GRUPO = {}
for grupo, sectores in GRUPOS_SUNBURST.items():
    for s in sectores:
        SECTOR_A_GRUPO[s] = grupo


# --- FUNCIONES AUXILIARES ---
def format_currency(value):
    """Convierte numeros grandes a formato legible (Billones/Millones)"""
    if abs(value) >= 1_000_000_000:
        return f"€{value/1_000_000_000:.2f} B"
    elif abs(value) >= 1_000_000:
        return f"€{value/1_000_000:.0f} M"
    else:
        return f"€{value:,.0f}"


def format_partner_name(code):
    """Retorna nombre del pais con bandera"""
    nombre = PAISES_NOMBRE.get(code, code)
    bandera = BANDERAS.get(code, '🏳️')
    return f"{bandera} {nombre}"


# --- CARGA DE DATOS ---
@st.cache_data(ttl=3600)
def load_goods_data():
    """Carga datos de bienes (CSV pre-procesado por etl_data.py)"""
    if not FILE_BIENES_AGREGADO.exists():
        st.error("Ejecuta primero 'python3 etl_data.py' para generar los datos.")
        st.stop()
    df = pd.read_csv(FILE_BIENES_AGREGADO)
    df['fecha'] = pd.to_datetime(df['fecha'])
    df['tipo'] = 'Bienes'
    return df


@st.cache_data(ttl=3600)
def load_partners_data(country_code):
    """Carga datos de socios comerciales bilaterales (bienes+servicios) para un pais"""
    if not FILE_COMERCIO_SOCIOS.exists():
        return None
    try:
        df = pd.read_csv(FILE_COMERCIO_SOCIOS)
        df['fecha'] = pd.to_datetime(df['fecha'])
        df_c = df[df['pais_code'] == country_code]
        if df_c.empty:
            return None
        return {
            'imports': df_c[['fecha', 'socio_code', 'importaciones']].rename(
                columns={'socio_code': 'partner', 'importaciones': 'OBS_VALUE'}),
            'exports': df_c[['fecha', 'socio_code', 'exportaciones']].rename(
                columns={'socio_code': 'partner', 'exportaciones': 'OBS_VALUE'}),
        }
    except Exception as e:
        st.warning(f"Error cargando datos de socios para {country_code}: {e}")
        return None





# --- CARGA DE DATOS ---
try:
    df_goods = load_goods_data()
except Exception as e:
    st.error(f"Error cargando datos: {e}")
    st.stop()

# --- NOMBRES DISPONIBLES (filtrar a v1) ---
# Mapeo nombre -> codigo
NOMBRE_A_CODIGO = {v: k for k, v in PAISES_V1.items()}
paises_nombres_v1 = list(PAISES_V1.values())

# Verificar cuales de nuestros 4 paises tienen datos
paises_disponibles = [p for p in paises_nombres_v1 if p in df_goods['pais'].unique()]

if not paises_disponibles:
    st.error("No hay datos disponibles para ninguno de los 4 paises. Ejecuta los ETLs primero.")
    st.stop()

# ============================================================
# LAYOUT PRINCIPAL (sin sidebar)
# ============================================================

# --- HEADER + DROPDOWN ---
st.title("🌍 Balanza Comercial")

col_sel1, col_sel2 = st.columns([1, 2])

with col_sel1:
    pais_sel = st.selectbox(
        "Selecciona un país",
        paises_disponibles,
        index=paises_disponibles.index('España') if 'España' in paises_disponibles else 0,
        label_visibility="collapsed"
    )

country_code = NOMBRE_A_CODIGO.get(pais_sel)

# Filtrar datos del pais seleccionado
df_pais = df_goods[df_goods['pais'] == pais_sel].sort_values('fecha')

if df_pais.empty:
    st.warning(f"No hay datos disponibles para {pais_sel}")
    st.stop()

# --- Rango temporal configurable ---
fechas_disponibles = sorted(df_pais['fecha'].unique())
fecha_max = pd.Timestamp(fechas_disponibles[-1])
fecha_min = pd.Timestamp(fechas_disponibles[0])
fecha_default_inicio = fecha_max - pd.DateOffset(months=11)
if fecha_default_inicio < fecha_min:
    fecha_default_inicio = fecha_min

with col_sel2:
    rango_fechas = st.slider(
        "Rango temporal",
        min_value=fecha_min.to_pydatetime(),
        max_value=fecha_max.to_pydatetime(),
        value=(fecha_default_inicio.to_pydatetime(), fecha_max.to_pydatetime()),
        format="MMM YYYY",
    )

fecha_inicio_sel = pd.Timestamp(rango_fechas[0])
fecha_fin_sel = pd.Timestamp(rango_fechas[1])

# --- KPIs: usar datos de bienes ---
df_pais_rango = df_pais[(df_pais['fecha'] >= fecha_inicio_sel) & (df_pais['fecha'] <= fecha_fin_sel)]

df_total = df_pais_rango[df_pais_rango['sector'] == 'Total Comercio']
df_agrupado_kpi = df_total.groupby('fecha')[['exportaciones', 'importaciones', 'balance']].sum().reset_index() if not df_total.empty else df_pais_rango.groupby('fecha')[['exportaciones', 'importaciones', 'balance']].sum().reset_index()
tot_exp = df_agrupado_kpi['exportaciones'].sum()
tot_imp = df_agrupado_kpi['importaciones'].sum()
tot_bal = df_agrupado_kpi['balance'].sum()

cobertura = (tot_exp / tot_imp * 100) if tot_imp > 0 else 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Exportaciones", format_currency(tot_exp), border=True)
c2.metric("Importaciones", format_currency(tot_imp), border=True)
c3.metric("Balanza Comercial", format_currency(tot_bal),
          delta=format_currency(tot_bal), delta_color="normal", border=True)
c4.metric("Tasa Cobertura", f"{cobertura:.1f}%", border=True)

st.caption("Fuente: Solo bienes (DS-059331)")
st.markdown("---")

# ============================================================
# GRAFICOS EN GRID 2x2
# ============================================================
col_left, col_right = st.columns(2)

# --- GRAFICO 1: Evolucion Mensual (izquierda arriba) ---
with col_left:
    st.subheader("📈 Evolución Mensual")

    # Usar datos de bienes
    df_total_r = df_pais_rango[df_pais_rango['sector'] == 'Total Comercio']
    df_evol = df_total_r.groupby('fecha')[['exportaciones', 'importaciones', 'balance']].sum().reset_index() if not df_total_r.empty else df_pais_rango.groupby('fecha')[['exportaciones', 'importaciones', 'balance']].sum().reset_index()

    fig_line = go.Figure()

    fig_line.add_trace(go.Scatter(
        x=df_evol['fecha'],
        y=df_evol['exportaciones'],
        name='Exportaciones',
        line=dict(color='#00CC96', width=2),
        yaxis='y'
    ))
    fig_line.add_trace(go.Scatter(
        x=df_evol['fecha'],
        y=df_evol['importaciones'],
        name='Importaciones',
        line=dict(color='#008B8B', width=2),
        yaxis='y'
    ))

    fig_line.add_trace(go.Bar(
        x=df_evol['fecha'],
        y=df_evol['balance'],
        name='Balance',
        marker_color=['#00CC96' if val >= 0 else '#EF553B' for val in df_evol['balance']],
        opacity=0.4,
        yaxis='y2'
    ))

    fig_line.update_layout(
        height=400,
        hovermode="x unified",
        legend=dict(orientation="h", y=1.12),
        yaxis=dict(title="Comercio (€)", side='left'),
        yaxis2=dict(title="Balance (€)", side='right', overlaying='y', showgrid=False),
        margin=dict(t=50, b=30)
    )
    st.plotly_chart(fig_line, use_container_width=True, config={"displayModeBar": False})


# --- GRAFICO 2: Sunburst Importaciones (derecha arriba) ---
with col_right:
    st.subheader("📊 Importaciones por Sector")

    df_sectores = df_pais_rango[(df_pais_rango['sector'] != 'Total Comercio')]

    # Mapeo sector nombre -> codigo SITC
    NOMBRE_A_SITC = {v: k for k, v in SECTORES_SITC.items() if k != 'TOTAL'}

    color_map = {
        'Agro y Alimentos': '#2ca02c',
        'Materias Primas y Energía': '#d62728',
        'Químicos': '#9467bd',
        'Manufacturas': '#1f77b4',
        'Otros': '#7f7f7f',
    }

    df_sec_agrupado = df_sectores.groupby('sector')[['importaciones']].sum().reset_index()
    df_sec_agrupado['sitc_code'] = df_sec_agrupado['sector'].map(NOMBRE_A_SITC)
    df_sec_agrupado = df_sec_agrupado.dropna(subset=['sitc_code'])
    df_sec_agrupado['grupo'] = df_sec_agrupado['sitc_code'].map(SECTOR_A_GRUPO)
    df_sec_agrupado = df_sec_agrupado[df_sec_agrupado['importaciones'] > 0]

    if not df_sec_agrupado.empty:
        fig_sun_imp = px.sunburst(
            df_sec_agrupado,
            path=['grupo', 'sector'],
            values='importaciones',
            color='grupo',
            color_discrete_map=color_map,
        )
        fig_sun_imp.update_traces(
            hovertemplate='<b>%{label}</b><br>Valor: €%{value:,.0f}<extra></extra>',
            maxdepth=2,
        )
        fig_sun_imp.update_layout(
            height=400,
            margin=dict(t=10, b=10, l=10, r=10),
        )
        st.plotly_chart(fig_sun_imp, use_container_width=True, config={"displayModeBar": False})
    else:
        st.info("Sin datos de importaciones por sector para este rango.")


# --- SEGUNDA FILA ---
col_left2, col_right2 = st.columns(2)

# --- GRAFICO 3: Bump Chart - Top 10 Socios (izquierda abajo) ---
with col_left2:
    st.subheader("🏆 Top 10 Socios Comerciales")

    # Toggle Exportaciones / Importaciones
    bump_flow = st.radio(
        "Flujo",
        ["Exportaciones", "Importaciones"],
        horizontal=True,
        key="bump_flow"
    )

    partners_data = load_partners_data(country_code)

    if partners_data is not None:
        if bump_flow == "Exportaciones":
            df_bump_src = partners_data['exports'].copy()
        else:
            df_bump_src = partners_data['imports'].copy()

        # Filtrar por rango temporal
        df_bump_src = df_bump_src[(df_bump_src['fecha'] >= fecha_inicio_sel) & (df_bump_src['fecha'] <= fecha_fin_sel)]

        # Sumar todos los sectores por partner y fecha
        df_bump = df_bump_src.groupby(['partner', 'fecha'])['OBS_VALUE'].sum().reset_index()

        # Para cada fecha, calcular ranking
        df_bump['rank'] = df_bump.groupby('fecha')['OBS_VALUE'].rank(ascending=False, method='min')

        # Determinar top 10 por volumen total
        total_por_partner = df_bump.groupby('partner')['OBS_VALUE'].sum()
        top10_partners = total_por_partner.nlargest(10).index.tolist()

        df_bump_top = df_bump[df_bump['partner'].isin(top10_partners)].copy()

        # Solo considerar ranking entre los top 10 entre si
        df_bump_top['rank'] = df_bump_top.groupby('fecha')['OBS_VALUE'].rank(ascending=False, method='min')

        # Colores para cada partner
        colors_palette = px.colors.qualitative.Set3[:10]

        fig_bump = go.Figure()

        for i, partner in enumerate(top10_partners):
            df_p = df_bump_top[df_bump_top['partner'] == partner].sort_values('fecha')
            partner_label = format_partner_name(partner)
            color = colors_palette[i % len(colors_palette)]

            # Linea
            fig_bump.add_trace(go.Scatter(
                x=df_p['fecha'],
                y=df_p['rank'],
                mode='lines',
                name=partner_label,
                line=dict(color=color, width=2),
                hoverinfo='skip',
                showlegend=True,
            ))

            # Markers solo al inicio y final
            if len(df_p) > 0:
                first = df_p.iloc[0]
                last = df_p.iloc[-1]

                for point in [first, last]:
                    fig_bump.add_trace(go.Scatter(
                        x=[point['fecha']],
                        y=[point['rank']],
                        mode='markers+text',
                        marker=dict(color=color, size=10),
                        text=[str(int(point['rank']))],
                        textposition='middle center',
                        textfont=dict(size=8, color='white'),
                        hovertemplate=f"<b>{partner_label}</b><br>"
                                      f"Ranking: {int(point['rank'])}<br>"
                                      f"Valor: €{point['OBS_VALUE']:,.0f}<extra></extra>",
                        showlegend=False,
                    ))

        fig_bump.update_layout(
            height=400,
            yaxis=dict(
                title="Ranking",
                autorange='reversed',
                dtick=1,
                range=[0.5, 10.5],
            ),
            xaxis=dict(title=""),
            legend=dict(
                orientation="v",
                yanchor="top",
                y=1,
                xanchor="left",
                x=1.02,
                font=dict(size=9),
            ),
            hovermode='x unified',
            margin=dict(t=30, b=30, r=180),
        )
        st.plotly_chart(fig_bump, use_container_width=True, config={"displayModeBar": False})
    else:
        st.info("Datos de socios no disponibles. Ejecuta: `python3 etl_data.py`")


# --- GRAFICO 4: Sunburst Exportaciones (derecha abajo) ---
with col_right2:
    st.subheader("📊 Exportaciones por Sector")

    df_sec_exp = df_sectores.groupby('sector')[['exportaciones']].sum().reset_index()
    df_sec_exp['sitc_code'] = df_sec_exp['sector'].map(NOMBRE_A_SITC)
    df_sec_exp = df_sec_exp.dropna(subset=['sitc_code'])
    df_sec_exp['grupo'] = df_sec_exp['sitc_code'].map(SECTOR_A_GRUPO)
    df_sec_exp = df_sec_exp[df_sec_exp['exportaciones'] > 0]

    if not df_sec_exp.empty:
        fig_sun_exp = px.sunburst(
            df_sec_exp,
            path=['grupo', 'sector'],
            values='exportaciones',
            color='grupo',
            color_discrete_map=color_map,
        )
        fig_sun_exp.update_traces(
            hovertemplate='<b>%{label}</b><br>Valor: €%{value:,.0f}<extra></extra>',
            maxdepth=2,
        )
        fig_sun_exp.update_layout(
            height=400,
            margin=dict(t=10, b=10, l=10, r=10),
        )
        st.plotly_chart(fig_sun_exp, use_container_width=True, config={"displayModeBar": False})
    else:
        st.info("Sin datos de exportaciones por sector para este rango.")


# ============================================================
# DESCARGA CSV
# ============================================================
st.markdown("---")

# Preparar datos para descarga
df_download = df_pais_rango[df_pais_rango['sector'] != 'Total Comercio'].copy()
df_download = df_download[['fecha', 'pais', 'sector', 'exportaciones', 'importaciones', 'balance']]
df_download['fecha'] = df_download['fecha'].dt.strftime('%Y-%m')

csv_data = df_download.to_csv(index=False).encode('utf-8')
st.download_button(
    label="📥 Descargar CSV",
    data=csv_data,
    file_name=f"balanza_comercial_{country_code}.csv",
    mime="text/csv"
)

# Footer
st.markdown("---")
if FILE_BIENES_AGREGADO.exists():
    last_update = datetime.fromtimestamp(FILE_BIENES_AGREGADO.stat().st_mtime).strftime('%Y-%m-%d %H:%M')
    st.caption(f"Fuente: Eurostat DS-059331 (Solo Bienes) | Ultima actualizacion: {last_update}")
st.caption("Meteoconomics - Datos reales desde la API oficial de Eurostat")
