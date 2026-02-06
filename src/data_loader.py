"""
Data loading and caching functions for trade balance data.
"""

import pandas as pd
import streamlit as st

from src.config import DATA_FOLDERS


@st.cache_data(ttl=3600)
def load_goods_data():
    """Load goods/trade data from all country folders.

    Searches through all DATA_FOLDERS and loads bienes_agregado.csv files.
    Caches results for 1 hour.

    Returns:
        DataFrame with combined trade data from all countries.

    Raises:
        Stops execution if no data files found.
    """
    all_dfs = []

    for folder_path in DATA_FOLDERS.values():
        file_path = folder_path / "bienes_agregado.csv"
        if file_path.exists():
            df = pd.read_csv(file_path)
            df["fecha"] = pd.to_datetime(df["fecha"])
            all_dfs.append(df)

    if not all_dfs:
        st.error("No hay datos. Ejecuta los ETLs primero.")
        st.stop()

    return pd.concat(all_dfs, ignore_index=True)


@st.cache_data(ttl=3600)
def load_partners_data(country_code):
    """Load bilateral trade partner data for a specific country.

    Determines correct data folder based on country code and loads
    partner-level trade data. Caches results for 1 hour.

    Args:
        country_code: ISO country code (e.g., 'ES', 'US', 'GB').

    Returns:
        Dictionary with 'imports' and 'exports' DataFrames, or None if unavailable.
        Each DataFrame contains columns: fecha, partner, OBS_VALUE.
    """
    # Determine which folder to use
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
