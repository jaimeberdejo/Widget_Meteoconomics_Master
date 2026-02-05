"""
ETL Unificado - Balanza Comercial (Solo Bienes)
Descarga y procesa datos de Eurostat en 2 API calls:
  1. DS-059331: Bienes por sector SITC, partner=WORLD (sunbursts + evolucion)
  2. DS-059331: Bienes bilaterales por socio (bump chart bienes)

Ejecutar: python3 etl_data.py [--force]
"""

import argparse
import io
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode

# ============================================================
# CONSTANTES
# ============================================================

REPORTERS = ['DE', 'ES', 'FR', 'IT']
REPORTER_NOMBRES = {'DE': 'Alemania', 'ES': 'España', 'FR': 'Francia', 'IT': 'Italia'}

PARTNERS = [
    'AT', 'AU', 'BE', 'BR', 'CA', 'CH', 'CL', 'CN', 'CZ', 'DE',
    'ES', 'FR', 'GB', 'IE', 'IN', 'IT', 'JP', 'KR', 'MX', 'NL',
    'NO', 'PL', 'PT', 'RU', 'SA', 'SE', 'SG', 'TW', 'UA', 'US', 'VN',
]

SECTORES_SITC = {
    '0': 'Alimentos y animales vivos', '1': 'Bebidas y tabaco',
    '2': 'Materiales crudos', '3': 'Combustibles minerales',
    '4': 'Aceites y grasas', '5': 'Productos químicos',
    '6': 'Manufacturas por material', '7': 'Maquinaria y transporte',
    '8': 'Manufacturas diversas', '9': 'Otros', 'TOTAL': 'Total Comercio',
}

# Archivos de salida
DATA_DIR = Path('data')
FILE_BIENES_AGREGADO = DATA_DIR / 'bienes_agregado.csv'
FILE_COMERCIO_SOCIOS = DATA_DIR / 'comercio_socios.csv'

# Mapeo codigos Eurostat BOP <-> ISO
BOP_TO_ISO = {'EL': 'GR', 'UK': 'GB', 'CN_X_HK': 'CN'}
ISO_TO_BOP = {v: k for k, v in BOP_TO_ISO.items()}

# Headers HTTP comunes
HTTP_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'text/csv, */*',
    'Accept-Language': 'en-US,en;q=0.9',
}

CURRENT_YEAR = datetime.now().year


# ============================================================
# FUNCIONES AUXILIARES
# ============================================================

def _download(url, description, timeout=300):
    """Descarga una URL y retorna el contenido como string."""
    print(f"\n{'='*70}")
    print(f"  {description}")
    print(f"{'='*70}")
    print(f"  URL: {url[:120]}...")

    try:
        response = requests.get(url, headers=HTTP_HEADERS, timeout=timeout)
        print(f"  Status: {response.status_code} | Size: {len(response.content):,} bytes")
        if response.status_code == 200:
            return response.text
        print(f"  ERROR: {response.text[:300]}")
        return None
    except requests.exceptions.Timeout:
        print(f"  ERROR: Timeout ({timeout}s)")
        return None
    except requests.exceptions.RequestException as e:
        print(f"  ERROR: {e}")
        return None


def _read_csv(csv_text):
    """Lee CSV text a DataFrame, filtrando filas sin OBS_VALUE."""
    df = pd.read_csv(io.StringIO(csv_text))
    if 'OBS_VALUE' in df.columns:
        df['OBS_VALUE'] = pd.to_numeric(df['OBS_VALUE'], errors='coerce')
        df = df.dropna(subset=['OBS_VALUE'])
    return df


# ============================================================
# CALL 1: Bienes agregado (sunbursts + fallback evolucion)
# ============================================================

def download_bienes_agregado():
    """
    DS-059331: Bienes por sector SITC, partner=WORLD, 4 reporters.
    formatVersion=2.0, labels=name -> columnas pareadas (codigo + nombre).
    """
    base = "https://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/3.0/data/dataflow/ESTAT/ds-059331/1.0/*.*.*.*.*.*"
    products = ','.join(SECTORES_SITC.keys())

    params = {
        'c[freq]': 'M',
        'c[reporter]': ','.join(REPORTERS),
        'c[partner]': 'WORLD',
        'c[product]': products,
        'c[flow]': '1,2',
        'c[indicators]': 'VALUE_EUR',
        'c[TIME_PERIOD]': f'ge:2002-01+le:{CURRENT_YEAR}-12',
        'compress': 'false',
        'format': 'csvdata',
        'formatVersion': '2.0',
        'lang': 'en',
        'labels': 'name',
    }

    url = f"{base}?{urlencode(params, safe=':,+[]')}"
    csv_text = _download(url, "CALL 1: Bienes agregado (DS-059331, WORLD, 4 reporters)")
    if csv_text is None:
        return None

    df = _read_csv(csv_text)
    if df.empty:
        print("  ERROR: DataFrame vacio tras leer CSV")
        return None

    # Identificar columnas-codigo (formatVersion=2.0 produce pares: codigo, nombre)
    cols = df.columns.tolist()
    # Buscar columna de reporter por codigo (e.g. 'reporter' o similar)
    reporter_col = _find_code_col(cols, 'reporter')
    product_col = _find_code_col(cols, 'product')
    flow_col = _find_code_col(cols, 'flow')

    if not all([reporter_col, product_col, flow_col]):
        print(f"  ERROR: No se encontraron columnas requeridas. Columnas: {cols}")
        return None

    # Extraer columnas necesarias
    df_work = df[[reporter_col, product_col, flow_col, 'TIME_PERIOD', 'OBS_VALUE']].copy()
    df_work.columns = ['reporter_code', 'product_code', 'flow_code', 'fecha', 'valor']

    # Filtrar solo nuestros 4 reporters y productos conocidos
    df_work = df_work[df_work['reporter_code'].isin(REPORTERS)]
    df_work = df_work[df_work['product_code'].isin(SECTORES_SITC.keys())]
    df_work = df_work[df_work['valor'] > 0]

    # Mapear nombres
    df_work['pais'] = df_work['reporter_code'].map(REPORTER_NOMBRES)
    df_work['pais_code'] = df_work['reporter_code']
    df_work['sector'] = df_work['product_code'].map(SECTORES_SITC)
    df_work['sector_code'] = df_work['product_code']

    # Pivotar flow: 1=importaciones, 2=exportaciones
    df_work['flow_type'] = df_work['flow_code'].astype(str).map({'1': 'importaciones', '2': 'exportaciones'})
    df_work = df_work.dropna(subset=['flow_type'])

    df_pivot = df_work.pivot_table(
        index=['fecha', 'pais', 'pais_code', 'sector', 'sector_code'],
        columns='flow_type',
        values='valor',
        aggfunc='sum',
        fill_value=0
    ).reset_index()
    df_pivot.columns.name = None

    for col in ['exportaciones', 'importaciones']:
        if col not in df_pivot.columns:
            df_pivot[col] = 0

    df_pivot['balance'] = df_pivot['exportaciones'] - df_pivot['importaciones']

    # Guardar
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df_pivot = df_pivot.sort_values(['pais_code', 'sector_code', 'fecha'])
    df_pivot.to_csv(FILE_BIENES_AGREGADO, index=False)

    print(f"  Guardado: {FILE_BIENES_AGREGADO} ({len(df_pivot):,} filas, {FILE_BIENES_AGREGADO.stat().st_size / 1024 / 1024:.1f} MB)")
    return df_pivot


# ============================================================
# CALL 2: Bienes socios (bilaterales)
# ============================================================

def download_bienes_socios():
    """
    DS-059331: Bienes bilaterales, TOTAL product, 31 partners, 4 reporters.
    """
    base = "https://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/3.0/data/dataflow/ESTAT/ds-059331/1.0/*.*.*.*.*.*"

    params = {
        'c[freq]': 'M',
        'c[reporter]': ','.join(REPORTERS),
        'c[partner]': ','.join(PARTNERS),
        'c[product]': 'TOTAL',
        'c[flow]': '1,2',
        'c[indicators]': 'VALUE_EUR',
        'c[TIME_PERIOD]': f'ge:2002-01+le:{CURRENT_YEAR}-12',
        'compress': 'false',
        'format': 'csvdata',
        'formatVersion': '2.0',
        'lang': 'en',
        'labels': 'name',
    }

    url = f"{base}?{urlencode(params, safe=':,+[]')}"
    csv_text = _download(url, "CALL 2: Bienes socios (DS-059331, 31 partners, TOTAL)")
    if csv_text is None:
        return None

    df = _read_csv(csv_text)
    if df.empty:
        return None

    cols = df.columns.tolist()
    reporter_col = _find_code_col(cols, 'reporter')
    partner_col = _find_code_col(cols, 'partner')
    flow_col = _find_code_col(cols, 'flow')

    if not all([reporter_col, partner_col, flow_col]):
        print(f"  ERROR: Columnas no encontradas. Cols: {cols}")
        return None

    df_work = df[[reporter_col, partner_col, flow_col, 'TIME_PERIOD', 'OBS_VALUE']].copy()
    df_work.columns = ['reporter_code', 'partner_code', 'flow_code', 'fecha', 'valor']

    df_work = df_work[df_work['reporter_code'].isin(REPORTERS)]
    df_work = df_work[df_work['partner_code'].isin(PARTNERS)]
    df_work = df_work[df_work['valor'] > 0]

    # Pivotar flow
    df_work['flow_type'] = df_work['flow_code'].astype(str).map({'1': 'imp_bienes', '2': 'exp_bienes'})
    df_work = df_work.dropna(subset=['flow_type'])

    df_pivot = df_work.pivot_table(
        index=['fecha', 'reporter_code', 'partner_code'],
        columns='flow_type',
        values='valor',
        aggfunc='sum',
        fill_value=0
    ).reset_index()
    df_pivot.columns.name = None

    for col in ['exp_bienes', 'imp_bienes']:
        if col not in df_pivot.columns:
            df_pivot[col] = 0

    print(f"  Bienes socios: {len(df_pivot):,} filas")
    return df_pivot





# ============================================================
# PREPARE: comercio_socios.csv (solo bienes)
# ============================================================

def prepare_comercio_socios(df_goods):
    """
    Prepara datos de bienes bilaterales (call 2) para el bump chart.
    """
    if df_goods is None:
        print("  ERROR: No hay datos de bienes socios")
        return None

    df_merged = df_goods.copy()
    df_merged['exportaciones'] = df_merged['exp_bienes']
    df_merged['importaciones'] = df_merged['imp_bienes']

    # Mapear nombres
    df_merged['pais'] = df_merged['reporter_code'].map(REPORTER_NOMBRES)
    df_merged['pais_code'] = df_merged['reporter_code']
    df_merged['socio_code'] = df_merged['partner_code']

    # Nombre socios (mapa basico para los 31)
    SOCIOS_NOMBRES = {
        'AT': 'Austria', 'AU': 'Australia', 'BE': 'Belgica', 'BR': 'Brasil',
        'CA': 'Canada', 'CH': 'Suiza', 'CL': 'Chile', 'CN': 'China',
        'CZ': 'Republica Checa', 'DE': 'Alemania', 'ES': 'Espana',
        'FR': 'Francia', 'GB': 'Reino Unido', 'IE': 'Irlanda', 'IN': 'India',
        'IT': 'Italia', 'JP': 'Japon', 'KR': 'Corea del Sur', 'MX': 'Mexico',
        'NL': 'Paises Bajos', 'NO': 'Noruega', 'PL': 'Polonia', 'PT': 'Portugal',
        'RU': 'Rusia', 'SA': 'Arabia Saudita', 'SE': 'Suecia', 'SG': 'Singapur',
        'TW': 'Taiwan', 'UA': 'Ucrania', 'US': 'Estados Unidos', 'VN': 'Vietnam',
    }
    df_merged['socio'] = df_merged['socio_code'].map(SOCIOS_NOMBRES).fillna(df_merged['socio_code'])

    # Columnas finales
    df_final = df_merged[['fecha', 'pais', 'pais_code', 'socio', 'socio_code', 'exportaciones', 'importaciones']].copy()
    df_final = df_final.sort_values(['pais_code', 'socio_code', 'fecha'])

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df_final.to_csv(FILE_COMERCIO_SOCIOS, index=False)
    print(f"  Guardado: {FILE_COMERCIO_SOCIOS} ({len(df_final):,} filas, {FILE_COMERCIO_SOCIOS.stat().st_size / 1024 / 1024:.1f} MB)")
    return df_final


# ============================================================
# UTILIDADES
# ============================================================

def _find_code_col(columns, keyword):
    """
    Busca la columna de codigo para un campo dado.
    formatVersion=2.0 con labels=name produce pares:
      reporter, Reporter Name  (o similar)
    La columna de codigo es la que coincide exactamente o es la primera del par.
    """
    keyword_lower = keyword.lower()
    candidates = []
    for col in columns:
        col_lower = col.lower().strip()
        if col_lower == keyword_lower:
            return col
        if keyword_lower in col_lower:
            candidates.append(col)

    # Si hay candidatos, preferir el mas corto (codigo vs nombre)
    if candidates:
        candidates.sort(key=len)
        return candidates[0]
    return None


def update_data_if_needed():
    """
    Actualiza datos si no existen o tienen mas de 7 dias.
    Retorna True si se actualizaron.
    """
    files = [FILE_BIENES_AGREGADO, FILE_COMERCIO_SOCIOS]

    # Verificar existencia
    if not all(f.exists() for f in files):
        print("Archivos no encontrados, descargando...")
        main(force=False)
        return True

    # Verificar tamano minimo
    for f in files:
        if f.stat().st_size < 1024:
            print(f"Archivo corrupto ({f.name}), re-descargando...")
            main(force=False)
            return True

    # Verificar antiguedad
    oldest = min(datetime.fromtimestamp(f.stat().st_mtime) for f in files)
    age = datetime.now() - oldest
    if age > timedelta(days=7):
        print(f"Cache antiguo ({age.days} dias), actualizando...")
        main(force=False)
        return True

    print(f"Cache valido ({age.days} dias)")
    return False


# ============================================================
# MAIN
# ============================================================

def main(force=False):
    """Ejecuta las 2 descargas y genera los 2 CSVs (solo bienes)."""
    print("=" * 70)
    print("ETL UNIFICADO - BALANZA COMERCIAL (SOLO BIENES)")
    print(f"Reporters: {', '.join(REPORTERS)} | Partners: {len(PARTNERS)}")
    print(f"Periodo: 2002-01 a {CURRENT_YEAR}-12")
    print("=" * 70)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Call 1: Bienes agregado
    df_bienes = download_bienes_agregado()
    if df_bienes is None:
        print("\nERROR: Fallo descarga bienes agregado")
        return False

    # Call 2: Bienes socios
    df_goods_bilateral = download_bienes_socios()

    # Preparar comercio_socios.csv (solo bienes)
    df_socios = prepare_comercio_socios(df_goods_bilateral)

    # Resumen
    print(f"\n{'='*70}")
    print("RESUMEN")
    print(f"{'='*70}")
    for f in [FILE_BIENES_AGREGADO, FILE_COMERCIO_SOCIOS]:
        if f.exists():
            size_mb = f.stat().st_size / 1024 / 1024
            print(f"  {f.name}: {size_mb:.1f} MB")
        else:
            print(f"  {f.name}: NO GENERADO")

    print(f"\nEjecuta el widget con: streamlit run widget_meteoconomics.py")
    print("=" * 70)
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ETL Unificado - Balanza Comercial')
    parser.add_argument('--force', action='store_true', help='Forzar descarga ignorando cache')
    args = parser.parse_args()

    if args.force:
        for f in [FILE_BIENES_AGREGADO, FILE_COMERCIO_SOCIOS]:
            if f.exists():
                f.unlink()
                print(f"Eliminado: {f}")

    main(force=args.force)
