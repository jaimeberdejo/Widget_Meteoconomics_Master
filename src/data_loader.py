import streamlit as st
import pandas as pd

from src.config import DATA_FOLDERS


@st.cache_data(ttl=3600)
def load_goods_data():
    """Carga datos de bienes de todas las carpetas de países."""
    all_dfs = []

    for folder_name, folder_path in DATA_FOLDERS.items():
        file_path = folder_path / 'bienes_agregado.csv'
        if file_path.exists():
            try:
                df = pd.read_csv(file_path)
                df['fecha'] = pd.to_datetime(df['fecha'])
                all_dfs.append(df)
            except Exception as e:
                st.warning(f"Error cargando {file_path}: {e}")

    if not all_dfs:
        st.error("No hay datos. Ejecuta los ETLs primero.")
        st.stop()

    return pd.concat(all_dfs, ignore_index=True)


@st.cache_data(ttl=3600)
def load_partners_data(country_code):
    """Carga datos de socios comerciales para un país."""
    if country_code in ['DE', 'ES', 'FR', 'IT']:
        folder = DATA_FOLDERS['eu']
    elif country_code == 'US':
        folder = DATA_FOLDERS['us']
    elif country_code == 'GB':
        folder = DATA_FOLDERS['gb']
    elif country_code == 'JP':
        folder = DATA_FOLDERS['jp']
    elif country_code == 'CA':
        folder = DATA_FOLDERS['ca']
    else:
        return None

    file_path = folder / 'comercio_socios.csv'
    if not file_path.exists():
        return None

    try:
        df = pd.read_csv(file_path)
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
    except Exception:
        return None
