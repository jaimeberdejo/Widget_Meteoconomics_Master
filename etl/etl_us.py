"""
ETL US Census Bureau - Comercio Internacional de Estados Unidos
API: https://api.census.gov/data/timeseries/intltrade/

Endpoints:
  - /exports/sitc - Exportaciones por SITC (ALL_VAL_MO)
  - /imports/sitc - Importaciones por SITC (GEN_VAL_MO)

API Key: https://api.census.gov/data/key_signup.html (gratuita)

Ejecutar: python3 etl_us.py [--force]
"""

import argparse
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
import os
import time

# ============================================================
# CONSTANTES
# ============================================================

# API Key - configurar como variable de entorno
CENSUS_API_KEY = os.environ.get('CENSUS_API_KEY', '')

# Base URLs
BASE_URL_EXPORTS = "https://api.census.gov/data/timeseries/intltrade/exports/sitc"
BASE_URL_IMPORTS = "https://api.census.gov/data/timeseries/intltrade/imports/sitc"

# Reporter
REPORTER = 'US'
REPORTER_NOMBRE = 'Estados Unidos'

# Códigos de país Census Bureau
# Fuente: https://www.census.gov/foreign-trade/schedules/c/countrycode.html
PARTNER_CODES = {
    'DE': '4280',   # Germany
    'ES': '4790',   # Spain
    'FR': '4279',   # France
    'IT': '4759',   # Italy
    'GB': '4120',   # United Kingdom
    'JP': '5880',   # Japan
    'CN': '5700',   # China
    'CA': '1220',   # Canada
    'MX': '2010',   # Mexico
    'KR': '5800',   # South Korea
    'IN': '5330',   # India
    'BR': '3510',   # Brazil
    'AU': '6021',   # Australia
    'NL': '4210',   # Netherlands
    'CH': '2940',   # Switzerland
    'BE': '4231',   # Belgium
    'AT': '4330',   # Austria
    'SE': '4010',   # Sweden
    'NO': '4090',   # Norway
    'PL': '4550',   # Poland
    'CZ': '4351',   # Czech Republic
    'PT': '4710',   # Portugal
    'IE': '4190',   # Ireland
    'RU': '4621',   # Russia
    'SA': '5170',   # Saudi Arabia
    'SG': '5590',   # Singapore
    'TW': '5830',   # Taiwan
    'VN': '5520',   # Vietnam
    'UA': '4623',   # Ukraine
    'CL': '3370',   # Chile
}

# Nombres de socios
SOCIOS_NOMBRES = {
    'AT': 'Austria', 'AU': 'Australia', 'BE': 'Belgica', 'BR': 'Brasil',
    'CA': 'Canada', 'CH': 'Suiza', 'CL': 'Chile', 'CN': 'China',
    'CZ': 'Republica Checa', 'DE': 'Alemania', 'ES': 'Espana',
    'FR': 'Francia', 'GB': 'Reino Unido', 'IE': 'Irlanda', 'IN': 'India',
    'IT': 'Italia', 'JP': 'Japon', 'KR': 'Corea del Sur', 'MX': 'Mexico',
    'NL': 'Paises Bajos', 'NO': 'Noruega', 'PL': 'Polonia', 'PT': 'Portugal',
    'RU': 'Rusia', 'SA': 'Arabia Saudita', 'SE': 'Suecia', 'SG': 'Singapur',
    'TW': 'Taiwan', 'UA': 'Ucrania', 'VN': 'Vietnam',
}

# Sectores SITC (1 dígito)
SECTORES_SITC = {
    '0': 'Alimentos y animales vivos',
    '1': 'Bebidas y tabaco',
    '2': 'Materiales crudos',
    '3': 'Combustibles minerales',
    '4': 'Aceites y grasas',
    '5': 'Productos químicos',
    '6': 'Manufacturas por material',
    '7': 'Maquinaria y transporte',
    '8': 'Manufacturas diversas',
    '9': 'Otros',
    'TOTAL': 'Total Comercio',
}

# Archivos de salida
DATA_DIR = Path(__file__).parent.parent / 'data' / 'us'
FILE_US_BIENES = DATA_DIR / 'bienes_agregado.csv'
FILE_US_SOCIOS = DATA_DIR / 'comercio_socios.csv'

HTTP_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json',
}

# Census data has ~2 month lag
CURRENT_YEAR = datetime.now().year
CURRENT_MONTH = datetime.now().month - 2
if CURRENT_MONTH <= 0:
    CURRENT_MONTH += 12
    CURRENT_YEAR -= 1
START_YEAR = 2010  # SITC data available from 2010


# ============================================================
# FUNCIONES API
# ============================================================

def _call_census_api(url, description, timeout=120):
    """Llama a la API de Census y retorna lista de listas."""
    print(f"  {description}...", end=" ", flush=True)

    try:
        response = requests.get(url, headers=HTTP_HEADERS, timeout=timeout)

        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list) and len(data) > 1:
                print(f"OK ({len(data)-1} registros)")
                return data
            else:
                print("vacío")
                return None
        elif response.status_code == 204:
            print("sin datos")
            return None
        else:
            print(f"ERROR {response.status_code}")
            return None

    except Exception as e:
        print(f"ERROR: {e}")
        return None


def _api_to_dataframe(data):
    """Convierte respuesta API a DataFrame."""
    if not data or len(data) < 2:
        return pd.DataFrame()
    headers = data[0]
    rows = data[1:]
    return pd.DataFrame(rows, columns=headers)


# ============================================================
# DESCARGA: Bienes agregados por sector SITC
# ============================================================

def download_us_bienes_agregado():
    """
    Descarga exportaciones e importaciones de US por sector SITC.
    - Exportaciones: ALL_VAL_MO
    - Importaciones: GEN_VAL_MO
    - SITC="-" indica TOTAL
    - Agrega por primer dígito SITC
    """
    print("\n" + "="*70)
    print("DESCARGANDO BIENES AGREGADOS US (SITC)")
    print("="*70)

    if not CENSUS_API_KEY:
        print("  ERROR: Se requiere CENSUS_API_KEY")
        print("  Obtener en: https://api.census.gov/data/key_signup.html")
        print("  Configurar: export CENSUS_API_KEY='tu_key'")
        return None

    all_exports = []
    all_imports = []

    # Descargar por año-mes
    for year in range(START_YEAR, CURRENT_YEAR + 1):
        max_month = CURRENT_MONTH if year == CURRENT_YEAR else 12

        for month in range(1, max_month + 1):
            time_period = f"{year}-{month:02d}"

            # --- EXPORTACIONES ---
            url_exp = (f"{BASE_URL_EXPORTS}?get=ALL_VAL_MO,SITC&time={time_period}"
                      f"&key={CENSUS_API_KEY}")
            data_exp = _call_census_api(url_exp, f"Exp {time_period}")

            if data_exp:
                df = _api_to_dataframe(data_exp)
                df['time'] = time_period
                df['flow'] = 'exportaciones'
                df['value'] = pd.to_numeric(df['ALL_VAL_MO'], errors='coerce')
                all_exports.append(df)

            # --- IMPORTACIONES ---
            url_imp = (f"{BASE_URL_IMPORTS}?get=GEN_VAL_MO,SITC&time={time_period}"
                      f"&key={CENSUS_API_KEY}")
            data_imp = _call_census_api(url_imp, f"Imp {time_period}")

            if data_imp:
                df = _api_to_dataframe(data_imp)
                df['time'] = time_period
                df['flow'] = 'importaciones'
                df['value'] = pd.to_numeric(df['GEN_VAL_MO'], errors='coerce')
                all_imports.append(df)

            # Rate limiting
            time.sleep(0.2)

    if not all_exports and not all_imports:
        print("  ERROR: No se descargaron datos")
        return None

    # Combinar y procesar
    df_exp = pd.concat(all_exports, ignore_index=True) if all_exports else pd.DataFrame()
    df_imp = pd.concat(all_imports, ignore_index=True) if all_imports else pd.DataFrame()

    # Procesar exportaciones
    result_exp = _process_sitc_data(df_exp, 'exportaciones')
    result_imp = _process_sitc_data(df_imp, 'importaciones')

    # Merge exportaciones e importaciones
    if result_exp.empty or result_imp.empty:
        print("  ERROR: Datos incompletos")
        return None

    df_merged = result_exp.merge(
        result_imp[['fecha', 'sector_code', 'importaciones']],
        on=['fecha', 'sector_code'],
        how='outer'
    ).fillna(0)

    df_merged['balance'] = df_merged['exportaciones'] - df_merged['importaciones']
    df_merged['pais'] = REPORTER_NOMBRE
    df_merged['pais_code'] = REPORTER
    df_merged['sector'] = df_merged['sector_code'].map(SECTORES_SITC)
    df_merged['moneda_original'] = 'USD'

    # Reordenar
    cols = ['fecha', 'pais', 'pais_code', 'sector', 'sector_code',
            'exportaciones', 'importaciones', 'balance', 'moneda_original']
    df_final = df_merged[[c for c in cols if c in df_merged.columns]]
    df_final = df_final.sort_values(['sector_code', 'fecha'])

    # Guardar
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df_final.to_csv(FILE_US_BIENES, index=False)
    print(f"\n  Guardado: {FILE_US_BIENES} ({len(df_final):,} filas)")

    return df_final


def _process_sitc_data(df, flow_name):
    """Procesa datos SITC: agrupa por primer dígito + extrae TOTAL."""
    if df.empty:
        return pd.DataFrame()

    # Separar TOTAL (SITC="-") y detalle
    df_total = df[df['SITC'] == '-'].copy()
    df_detail = df[df['SITC'] != '-'].copy()

    # Agregar detalle por primer dígito SITC
    df_detail['sector_code'] = df_detail['SITC'].astype(str).str[0]
    df_detail = df_detail[df_detail['sector_code'].isin(list('0123456789'))]

    df_by_sector = df_detail.groupby(['time', 'sector_code'])['value'].sum().reset_index()
    df_by_sector.columns = ['fecha', 'sector_code', flow_name]

    # Agregar TOTAL
    df_total_agg = df_total.groupby('time')['value'].sum().reset_index()
    df_total_agg.columns = ['fecha', flow_name]
    df_total_agg['sector_code'] = 'TOTAL'

    # Combinar
    result = pd.concat([df_by_sector, df_total_agg], ignore_index=True)
    return result


# ============================================================
# DESCARGA: Comercio bilateral con socios
# ============================================================

def download_us_socios():
    """
    Descarga comercio bilateral de US con principales socios.
    Solo TOTAL (sin desglose por SITC) para simplificar.
    """
    print("\n" + "="*70)
    print("DESCARGANDO COMERCIO BILATERAL US")
    print("="*70)

    if not CENSUS_API_KEY:
        print("  ERROR: Se requiere CENSUS_API_KEY")
        return None

    all_data = []

    # Por cada mes
    for year in range(START_YEAR, CURRENT_YEAR + 1):
        max_month = CURRENT_MONTH if year == CURRENT_YEAR else 12

        for month in range(1, max_month + 1):
            time_period = f"{year}-{month:02d}"

            # Descargar todos los países de una vez (más eficiente)
            # --- EXPORTACIONES ---
            url_exp = (f"{BASE_URL_EXPORTS}?get=ALL_VAL_MO,CTY_CODE&time={time_period}"
                      f"&SITC=-&key={CENSUS_API_KEY}")
            data_exp = _call_census_api(url_exp, f"Exp socios {time_period}")

            if data_exp:
                df = _api_to_dataframe(data_exp)
                df['fecha'] = time_period
                df['exportaciones'] = pd.to_numeric(df['ALL_VAL_MO'], errors='coerce')
                all_data.append(df[['fecha', 'CTY_CODE', 'exportaciones']])

            # --- IMPORTACIONES ---
            url_imp = (f"{BASE_URL_IMPORTS}?get=GEN_VAL_MO,CTY_CODE&time={time_period}"
                      f"&SITC=-&key={CENSUS_API_KEY}")
            data_imp = _call_census_api(url_imp, f"Imp socios {time_period}")

            if data_imp:
                df = _api_to_dataframe(data_imp)
                df['fecha'] = time_period
                df['importaciones'] = pd.to_numeric(df['GEN_VAL_MO'], errors='coerce')

                # Merge con exportaciones del mismo período
                if all_data and 'exportaciones' in all_data[-1].columns:
                    # Ya tenemos exp, agregar imp
                    pass

                all_data.append(df[['fecha', 'CTY_CODE', 'importaciones']])

            time.sleep(0.2)

    if not all_data:
        print("  ERROR: No se descargaron datos")
        return None

    # Combinar todos
    df_all = pd.concat(all_data, ignore_index=True)

    # Agrupar por fecha y país
    df_exp = df_all[df_all['exportaciones'].notna()].groupby(['fecha', 'CTY_CODE'])['exportaciones'].sum().reset_index()
    df_imp = df_all[df_all['importaciones'].notna()].groupby(['fecha', 'CTY_CODE'])['importaciones'].sum().reset_index()

    df_merged = df_exp.merge(df_imp, on=['fecha', 'CTY_CODE'], how='outer').fillna(0)

    # Filtrar solo nuestros socios de interés
    census_to_iso = {v: k for k, v in PARTNER_CODES.items()}
    df_merged['socio_code'] = df_merged['CTY_CODE'].map(census_to_iso)
    df_merged = df_merged.dropna(subset=['socio_code'])

    # Añadir info
    df_merged['pais'] = REPORTER_NOMBRE
    df_merged['pais_code'] = REPORTER
    df_merged['socio'] = df_merged['socio_code'].map(SOCIOS_NOMBRES)
    df_merged['moneda_original'] = 'USD'

    # Reordenar
    cols = ['fecha', 'pais', 'pais_code', 'socio', 'socio_code',
            'exportaciones', 'importaciones', 'moneda_original']
    df_final = df_merged[[c for c in cols if c in df_merged.columns]]
    df_final = df_final.sort_values(['socio_code', 'fecha'])

    # Guardar
    df_final.to_csv(FILE_US_SOCIOS, index=False)
    print(f"\n  Guardado: {FILE_US_SOCIOS} ({len(df_final):,} filas)")

    return df_final


# ============================================================
# MAIN
# ============================================================

def main(force=False):
    """Ejecuta las descargas de datos de comercio de US."""
    print("=" * 70)
    print("ETL US CENSUS BUREAU - COMERCIO INTERNACIONAL")
    print(f"Reporter: {REPORTER} ({REPORTER_NOMBRE})")
    print(f"Periodo: {START_YEAR}-01 a {CURRENT_YEAR}-{CURRENT_MONTH:02d}")
    print(f"API Key: {'Configurada' if CENSUS_API_KEY else 'NO CONFIGURADA'}")
    print("=" * 70)

    if not CENSUS_API_KEY:
        print("\n  ERROR: Se requiere API key del Census Bureau")
        print("  1. Obtener gratis en: https://api.census.gov/data/key_signup.html")
        print("  2. Configurar: export CENSUS_API_KEY='tu_key'")
        print("  3. Ejecutar de nuevo: python3 etl_us.py")
        return False

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Descargar bienes agregados
    df_bienes = download_us_bienes_agregado()

    # Descargar comercio bilateral
    df_socios = download_us_socios()

    # Resumen
    print(f"\n{'='*70}")
    print("RESUMEN US")
    print(f"{'='*70}")
    for f in [FILE_US_BIENES, FILE_US_SOCIOS]:
        if f.exists():
            size_kb = f.stat().st_size / 1024
            print(f"  {f.name}: {size_kb:.1f} KB")
        else:
            print(f"  {f.name}: NO GENERADO")

    return df_bienes is not None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ETL US Census Bureau')
    parser.add_argument('--force', action='store_true', help='Forzar descarga')
    args = parser.parse_args()

    if args.force:
        for f in [FILE_US_BIENES, FILE_US_SOCIOS]:
            if f.exists():
                f.unlink()
                print(f"Eliminado: {f}")

    main(force=args.force)
