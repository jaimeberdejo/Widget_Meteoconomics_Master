"""
ETL Currency - Tasas de cambio históricas para conversión a EUR
Fuente: Banco Central Europeo (ECB) Statistical Data Warehouse

API: https://data.ecb.europa.eu/data/data-browser/EXR
Formato: SDMX REST API

Monedas soportadas:
  - USD (Dólar estadounidense)
  - GBP (Libra esterlina)
  - JPY (Yen japonés)
  - CAD (Dólar canadiense)

Ejecutar: python3 etl_currency.py [--force]
"""

import argparse
import io
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# ============================================================
# CONSTANTES
# ============================================================

# Monedas a descargar (código ISO 4217)
CURRENCIES = ['USD', 'GBP', 'JPY', 'CAD']

# ECB Statistical Data Warehouse API
# Endpoint para tasas de cambio diarias
ECB_BASE_URL = "https://data-api.ecb.europa.eu/service/data/EXR"

# Archivos de salida
DATA_DIR = Path(__file__).parent.parent / 'data'
FILE_EXCHANGE_RATES = DATA_DIR / 'exchange_rates.csv'

# Headers HTTP
HTTP_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'text/csv',
}

CURRENT_YEAR = datetime.now().year
START_YEAR = 2002  # Para coincidir con Eurostat


# ============================================================
# FUNCIONES AUXILIARES
# ============================================================

def _download_ecb(url, description, timeout=120):
    """Descarga datos del ECB API."""
    print(f"\n{'='*70}")
    print(f"  {description}")
    print(f"{'='*70}")
    print(f"  URL: {url[:100]}...")

    try:
        response = requests.get(url, headers=HTTP_HEADERS, timeout=timeout)
        print(f"  Status: {response.status_code} | Size: {len(response.content):,} bytes")

        if response.status_code == 200:
            return response.text
        else:
            print(f"  ERROR: {response.text[:300]}")
            return None

    except requests.exceptions.Timeout:
        print(f"  ERROR: Timeout ({timeout}s)")
        return None
    except requests.exceptions.RequestException as e:
        print(f"  ERROR: {e}")
        return None


def download_exchange_rates():
    """
    Descarga tasas de cambio mensuales del ECB.
    Formato: 1 EUR = X unidades de moneda extranjera
    Para convertir a EUR: valor_local / tasa
    """
    print("\n" + "="*70)
    print("DESCARGANDO TASAS DE CAMBIO ECB")
    print("="*70)

    all_rates = []

    for currency in CURRENCIES:
        # Construir URL para SDMX REST API
        # Frecuencia mensual (M), tipo SP00 (spot), serie A (average)
        # Formato: EXR/M.{currency}.EUR.SP00.A
        flow_ref = f"M.{currency}.EUR.SP00.A"
        url = f"{ECB_BASE_URL}/{flow_ref}"

        params = {
            'format': 'csvdata',
            'startPeriod': f'{START_YEAR}-01',
            'endPeriod': f'{CURRENT_YEAR}-12',
        }

        full_url = url + "?" + "&".join(f"{k}={v}" for k, v in params.items())
        csv_text = _download_ecb(full_url, f"Tasa EUR/{currency}")

        if csv_text:
            try:
                df = pd.read_csv(io.StringIO(csv_text))

                # ECB devuelve columnas como TIME_PERIOD, OBS_VALUE
                if 'TIME_PERIOD' in df.columns and 'OBS_VALUE' in df.columns:
                    df_clean = df[['TIME_PERIOD', 'OBS_VALUE']].copy()
                    df_clean.columns = ['fecha', 'tasa']
                    df_clean['moneda'] = currency
                    df_clean['tasa'] = pd.to_numeric(df_clean['tasa'], errors='coerce')
                    df_clean = df_clean.dropna(subset=['tasa'])
                    all_rates.append(df_clean)
                    print(f"    {currency}: {len(df_clean)} registros")
                else:
                    print(f"    {currency}: Columnas no encontradas: {df.columns.tolist()}")

            except Exception as e:
                print(f"    ERROR procesando {currency}: {e}")

    if not all_rates:
        print("\n  No se descargaron tasas. Usando tasas de respaldo...")
        return download_fallback_rates()

    # Combinar todos
    df_all = pd.concat(all_rates, ignore_index=True)

    # Pivotar: una columna por moneda
    df_pivot = df_all.pivot_table(
        index='fecha',
        columns='moneda',
        values='tasa',
        aggfunc='mean'
    ).reset_index()
    df_pivot.columns.name = None

    # Añadir EUR = 1 para referencia
    df_pivot['EUR'] = 1.0

    # Ordenar por fecha
    df_pivot = df_pivot.sort_values('fecha')

    # Guardar
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df_pivot.to_csv(FILE_EXCHANGE_RATES, index=False)
    print(f"\n  Guardado: {FILE_EXCHANGE_RATES} ({len(df_pivot)} registros)")

    return df_pivot


def download_fallback_rates():
    """
    Tasas de respaldo aproximadas si ECB no está disponible.
    Estas son promedios históricos recientes.
    """
    print("\n  Generando tasas de respaldo...")

    # Tasas promedio aproximadas (2020-2024)
    fallback_rates = {
        'USD': 1.10,   # 1 EUR = 1.10 USD
        'GBP': 0.86,   # 1 EUR = 0.86 GBP
        'JPY': 140.0,  # 1 EUR = 140 JPY
        'CAD': 1.45,   # 1 EUR = 1.45 CAD
    }

    periods = []
    for year in range(START_YEAR, CURRENT_YEAR + 1):
        for month in range(1, 13):
            if year == CURRENT_YEAR and month > datetime.now().month:
                break
            periods.append(f"{year}-{month:02d}")

    data = []
    for period in periods:
        row = {'fecha': period, 'EUR': 1.0}
        for currency, rate in fallback_rates.items():
            row[currency] = rate
        data.append(row)

    df = pd.DataFrame(data)
    df = df.sort_values('fecha')

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(FILE_EXCHANGE_RATES, index=False)
    print(f"\n  Guardado (fallback): {FILE_EXCHANGE_RATES} ({len(df)} registros)")

    return df


def load_exchange_rates():
    """Carga tasas de cambio del archivo CSV."""
    if not FILE_EXCHANGE_RATES.exists():
        print("  Tasas de cambio no encontradas, descargando...")
        return download_exchange_rates()

    df = pd.read_csv(FILE_EXCHANGE_RATES)
    return df


def convert_to_eur(df, value_cols, currency_col='moneda_original', rates_df=None):
    """
    Convierte valores de moneda local a EUR.

    Args:
        df: DataFrame con valores a convertir
        value_cols: Lista de columnas con valores monetarios
        currency_col: Columna que indica la moneda original
        rates_df: DataFrame con tasas (opcional, se carga si no se proporciona)

    Returns:
        DataFrame con columnas convertidas a EUR
    """
    if rates_df is None:
        rates_df = load_exchange_rates()

    df_result = df.copy()

    # Asegurar que fecha tiene formato compatible
    if 'fecha' in df_result.columns:
        # Normalizar formato de fecha a YYYY-MM
        df_result['fecha_mes'] = pd.to_datetime(df_result['fecha']).dt.strftime('%Y-%m')
    else:
        print("  ERROR: No hay columna 'fecha'")
        return df_result

    # Merge con tasas
    rates_df['fecha_mes'] = rates_df['fecha'].astype(str).str[:7]
    df_merged = df_result.merge(rates_df, on='fecha_mes', how='left', suffixes=('', '_rate'))

    # Convertir cada columna de valor
    for col in value_cols:
        if col in df_merged.columns and currency_col in df_merged.columns:
            # Para cada moneda, aplicar la tasa correspondiente
            for currency in CURRENCIES:
                if currency in df_merged.columns:
                    mask = df_merged[currency_col] == currency
                    if mask.any():
                        # valor_eur = valor_local / tasa
                        df_merged.loc[mask, col] = df_merged.loc[mask, col] / df_merged.loc[mask, currency]

    # Limpiar columnas temporales
    cols_to_drop = ['fecha_mes'] + [c for c in df_merged.columns if c.endswith('_rate')]
    df_merged = df_merged.drop(columns=[c for c in cols_to_drop if c in df_merged.columns], errors='ignore')

    # Eliminar columnas de tasas individuales
    for curr in CURRENCIES + ['EUR']:
        if curr in df_merged.columns and curr != currency_col:
            df_merged = df_merged.drop(columns=[curr], errors='ignore')

    return df_merged


# ============================================================
# MAIN
# ============================================================

def main(force=False):
    """Descarga tasas de cambio del ECB."""
    print("=" * 70)
    print("ETL TASAS DE CAMBIO - ECB")
    print(f"Monedas: {', '.join(CURRENCIES)}")
    print(f"Periodo: {START_YEAR}-01 a {CURRENT_YEAR}-12")
    print("=" * 70)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    df = download_exchange_rates()

    if df is not None and not df.empty:
        print(f"\n{'='*70}")
        print("RESUMEN TASAS")
        print(f"{'='*70}")
        print(f"  Registros: {len(df)}")
        print(f"  Período: {df['fecha'].min()} a {df['fecha'].max()}")

        # Mostrar últimas tasas
        print(f"\n  Últimas tasas (1 EUR = X moneda):")
        last_row = df.iloc[-1]
        for curr in CURRENCIES:
            if curr in df.columns:
                print(f"    {curr}: {last_row[curr]:.4f}")

        return True
    else:
        print("\n  ERROR: No se pudieron obtener tasas de cambio")
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ETL Tasas de Cambio ECB')
    parser.add_argument('--force', action='store_true', help='Forzar descarga')
    args = parser.parse_args()

    if args.force and FILE_EXCHANGE_RATES.exists():
        FILE_EXCHANGE_RATES.unlink()
        print(f"Eliminado: {FILE_EXCHANGE_RATES}")

    main(force=args.force)
