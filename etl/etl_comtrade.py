"""
ETL UN Comtrade - Comercio Internacional (UK, Japón, Canadá, China)
API: https://comtradeapi.un.org/

Descarga datos HS y los mapea a sectores SITC para:
  - Reino Unido (GB) - Código M49: 826
  - Japón (JP) - Código M49: 392
  - Canadá (CA) - Código M49: 124
  - China (CN) - Código M49: 156

Usa llamadas batch para minimizar peticiones API (límite 500/día).

Ejecutar: python3 etl_comtrade.py [--force] [--country GB|JP|CA|CN]
"""

import argparse
import pandas as pd
import requests
from datetime import datetime
from pathlib import Path
import os
import time

from etl import SECTORES_SITC, SOCIOS_NOMBRES

# ============================================================
# CONSTANTES
# ============================================================

COMTRADE_KEY = os.environ.get('COMTRADE_API_KEY', '')
BASE_URL = "https://comtradeapi.un.org/data/v1/get/C"

REPORTERS = {
    'GB': {'code': 826, 'name': 'Reino Unido'},
    'JP': {'code': 392, 'name': 'Japón'},
    'CA': {'code': 124, 'name': 'Canadá'},
    'CN': {'code': 156, 'name': 'China'},
}

# Mapeo HS 2-dígitos a sectores SITC
HS_TO_SITC = {
    '01': '0', '02': '0', '03': '0', '04': '0', '05': '0',
    '06': '0', '07': '0', '08': '0', '09': '0', '10': '0',
    '11': '0', '12': '0', '13': '0', '14': '0', '15': '0',
    '16': '0', '17': '0', '18': '0', '19': '0', '20': '0',
    '21': '0', '22': '1', '23': '0', '24': '1',
    '25': '2', '26': '2',
    '27': '3',
    '28': '5', '29': '5', '30': '5', '31': '5', '32': '5',
    '33': '5', '34': '5', '35': '5', '36': '5', '37': '5', '38': '5',
    '39': '6', '40': '6', '41': '6', '42': '6', '43': '6',
    '44': '6', '45': '6', '46': '6', '47': '6', '48': '6', '49': '6',
    '50': '6', '51': '6', '52': '6', '53': '6', '54': '6',
    '55': '6', '56': '6', '57': '6', '58': '6', '59': '6',
    '60': '6', '61': '6', '62': '6', '63': '6',
    '68': '6', '69': '6', '70': '6', '71': '6', '72': '6',
    '73': '6', '74': '6', '75': '6', '76': '6', '78': '6',
    '79': '6', '80': '6', '81': '6', '82': '6', '83': '6',
    '84': '7', '85': '7', '86': '7', '87': '7', '88': '7', '89': '7',
    '64': '8', '65': '8', '66': '8', '67': '8',
    '90': '8', '91': '8', '92': '8', '93': '8', '94': '8',
    '95': '8', '96': '8',
    '97': '9', '98': '9', '99': '9', '77': '7',
}

# Socios comerciales: M49 -> ISO
TOP_PARTNERS = {
    276: 'DE', 251: 'FR', 380: 'IT', 724: 'ES',
    156: 'CN', 842: 'US', 392: 'JP', 124: 'CA',
    528: 'NL', 372: 'IE', 56: 'BE', 756: 'CH',
    410: 'KR', 356: 'IN', 36: 'AU', 826: 'GB',
    484: 'MX', 76: 'BR', 702: 'SG', 158: 'TW', 752: 'SE',
}

BASE_DATA_DIR = Path(__file__).parent.parent / 'data'
CURRENT_YEAR = datetime.now().year
CURRENT_MONTH = datetime.now().month - 2
if CURRENT_MONTH <= 0:
    CURRENT_MONTH += 12
    CURRENT_YEAR -= 1
START_YEAR = 2010

# Contador global de llamadas API
api_call_count = 0


# ============================================================
# FUNCIONES API
# ============================================================

def _call_api(reporter_code, period, flow_code, freq='M',
              cmd_code='TOTAL', partner_code=None):
    """
    Llama a la API de UN Comtrade con soporte batch.
    period, flow_code y partner_code pueden ser valores separados por comas.
    """
    global api_call_count
    api_call_count += 1

    url = f"{BASE_URL}/{freq}/HS"
    params = {
        'reporterCode': str(reporter_code),
        'period': str(period),
        'flowCode': flow_code,
        'cmdCode': cmd_code,
        'subscription-key': COMTRADE_KEY,
    }
    if partner_code is not None:
        params['partnerCode'] = str(partner_code)

    try:
        response = requests.get(url, params=params, timeout=120)
        if response.status_code == 200:
            data = response.json()
            return data.get('data', [])
        elif response.status_code == 429:
            print(f"\n    RATE LIMIT! Esperando 60s...")
            time.sleep(60)
            return _call_api(reporter_code, period, flow_code, freq,
                             cmd_code, partner_code)
        else:
            print(f"\n    ERROR HTTP {response.status_code}: {response.text[:200]}")
            return None
    except Exception as e:
        print(f"\n    ERROR: {e}")
        return None


def _get_data_dir(country_code):
    return BASE_DATA_DIR / country_code.lower()


def _get_last_date_from_file(file_path):
    if not file_path.exists():
        return None
    try:
        df = pd.read_csv(file_path)
        df['fecha'] = pd.to_datetime(df['fecha'])
        return df['fecha'].max()
    except Exception:
        return None


# ============================================================
# DESCARGA: Bienes agregados por sector (BATCH por año)
# ============================================================

def _fetch_bienes_batch(reporter_m49, months_list):
    """
    Descarga AG2 para una lista de meses. Si la API trunca a 100K registros,
    divide automáticamente en batches más pequeños.
    """
    if not months_list:
        return []

    periods_str = ','.join(months_list)
    records = _call_api(
        reporter_m49, periods_str, 'X,M',
        freq='M', cmd_code='AG2', partner_code=0
    )

    if records is None:
        return []

    # Si hay 100K registros, probablemente está truncado -> dividir
    if len(records) >= 99000 and len(months_list) > 1:
        mid = len(months_list) // 2
        left = _fetch_bienes_batch(reporter_m49, months_list[:mid])
        time.sleep(1)
        right = _fetch_bienes_batch(reporter_m49, months_list[mid:])
        return left + right

    return records


def download_bienes_agregado(country_code, incremental=True):
    """
    Descarga exp/imp por sector SITC. Usa llamadas batch por año
    con subdivisión automática si la API trunca resultados.
    """
    if country_code not in REPORTERS:
        print(f"  ERROR: País no soportado: {country_code}")
        return None

    reporter = REPORTERS[country_code]
    reporter_m49 = reporter['code']
    reporter_name = reporter['name']

    data_dir = _get_data_dir(country_code)
    file_path = data_dir / 'bienes_agregado.csv'

    print(f"\n{'='*70}")
    print(f"BIENES {country_code} ({reporter_name})")
    print(f"{'='*70}")

    existing_df = None
    prepend_years = []
    append_start_year = START_YEAR

    if incremental and file_path.exists():
        existing_df = pd.read_csv(file_path)
        existing_df['fecha'] = pd.to_datetime(existing_df['fecha'])
        first_date = existing_df['fecha'].min()
        last_date = existing_df['fecha'].max()

        # Prepend: if existing data starts after START_YEAR, download the gap
        if first_date.year > START_YEAR:
            prepend_years = list(range(START_YEAR, first_date.year))
            print(f"  Prepend: descargando {START_YEAR}-{first_date.year - 1}")

        # Append: incremental from last date
        append_start_year = last_date.year
        print(f"  Incremental desde {append_start_year}")

    years = prepend_years + list(range(append_start_year, CURRENT_YEAR + 1))
    if not years:
        years = list(range(START_YEAR, CURRENT_YEAR + 1))
    print(f"  Años: {years[0]}-{years[-1]}")

    all_records = []

    for year in years:
        if year == CURRENT_YEAR:
            months = [f"{year}{m:02d}" for m in range(1, CURRENT_MONTH + 1)]
        else:
            months = [f"{year}{m:02d}" for m in range(1, 13)]

        if not months:
            continue

        print(f"  {year} ({len(months)} meses)...", end=" ", flush=True)

        records = _fetch_bienes_batch(reporter_m49, months)

        if not records:
            print("sin datos")
            time.sleep(1)
            continue

        # Procesar: agrupar por (periodo, flujo) y mapear HS->SITC
        period_flow_data = {}

        for rec in records:
            period = str(rec.get('period', ''))
            if len(period) < 6:
                continue
            y, m = int(period[:4]), int(period[4:])
            fecha = f"{y}-{m:02d}"
            flow = rec.get('flowCode', '')
            hs_code = str(rec.get('cmdCode', ''))[:2]
            value = rec.get('primaryValue', 0) or 0
            sitc_code = HS_TO_SITC.get(hs_code, '9')

            key = (fecha, flow)
            if key not in period_flow_data:
                period_flow_data[key] = {}
            period_flow_data[key][sitc_code] = (
                period_flow_data[key].get(sitc_code, 0) + value
            )

        # Pivotar: para cada fecha, combinar X y M
        fechas = sorted(set(k[0] for k in period_flow_data))
        for fecha in fechas:
            exp_data = period_flow_data.get((fecha, 'X'), {})
            imp_data = period_flow_data.get((fecha, 'M'), {})
            all_sectors = set(exp_data.keys()) | set(imp_data.keys())

            total_exp = sum(exp_data.values())
            total_imp = sum(imp_data.values())

            for sector in all_sectors:
                exp_val = exp_data.get(sector, 0)
                imp_val = imp_data.get(sector, 0)
                all_records.append({
                    'fecha': fecha,
                    'pais': reporter_name,
                    'pais_code': country_code,
                    'sector': SECTORES_SITC.get(sector, f'Sector {sector}'),
                    'sector_code': sector,
                    'exportaciones': exp_val,
                    'importaciones': imp_val,
                    'balance': exp_val - imp_val,
                    'moneda_original': 'USD',
                })

            all_records.append({
                'fecha': fecha,
                'pais': reporter_name,
                'pais_code': country_code,
                'sector': 'Total Comercio',
                'sector_code': 'TOTAL',
                'exportaciones': total_exp,
                'importaciones': total_imp,
                'balance': total_exp - total_imp,
                'moneda_original': 'USD',
            })

        print(f"OK ({len(fechas)} meses, {len(records)} registros)")
        time.sleep(1)

    if not all_records:
        print("  Sin datos nuevos")
        return existing_df

    df_new = pd.DataFrame(all_records)

    if existing_df is not None and not df_new.empty:
        df_final = pd.concat([existing_df, df_new], ignore_index=True)
        df_final = df_final.drop_duplicates(
            subset=['fecha', 'sector_code'], keep='last'
        )
    else:
        df_final = df_new

    df_final = df_final.sort_values(['sector_code', 'fecha'])
    data_dir.mkdir(parents=True, exist_ok=True)
    df_final.to_csv(file_path, index=False)
    print(f"  Guardado: {file_path} ({len(df_final):,} filas)")

    return df_final


# ============================================================
# DESCARGA: Comercio bilateral con socios (BATCH)
# ============================================================

def download_comercio_socios(country_code, incremental=True):
    """
    Descarga comercio bilateral con frecuencia mensual.
    Usa 1 llamada batch por año (12 meses × ~20 socios × 2 flujos).
    En modo incremental solo descarga desde el último año existente.
    """
    if country_code not in REPORTERS:
        print(f"  ERROR: País no soportado: {country_code}")
        return None

    reporter = REPORTERS[country_code]
    reporter_m49 = reporter['code']
    reporter_name = reporter['name']

    data_dir = _get_data_dir(country_code)
    file_path = data_dir / 'comercio_socios.csv'

    print(f"\n{'='*70}")
    print(f"SOCIOS {country_code} ({reporter_name}) - MENSUAL")
    print(f"{'='*70}")

    existing_df = None
    start_year = START_YEAR

    if incremental and file_path.exists():
        existing_df = pd.read_csv(file_path)
        existing_df['fecha'] = pd.to_datetime(existing_df['fecha'], format='%Y-%m')
        last_date = existing_df['fecha'].max()
        start_year = last_date.year
        print(f"  Incremental desde {start_year}")

    # Filtrar socios (excluir el propio país)
    partners = {k: v for k, v in TOP_PARTNERS.items() if v != country_code}
    partners_str = ','.join(str(k) for k in partners.keys())

    years = list(range(start_year, CURRENT_YEAR + 1))

    print(f"  Años: {years[0]}-{years[-1]}, Socios: {len(partners)}")
    print(f"  ~{len(years)} llamadas API (1 por año)")

    all_records = []

    for year in years:
        if year == CURRENT_YEAR:
            months = [f"{year}{m:02d}" for m in range(1, CURRENT_MONTH + 1)]
        else:
            months = [f"{year}{m:02d}" for m in range(1, 13)]

        if not months:
            continue

        periods_str = ','.join(months)
        print(f"  {year} ({len(months)} meses)...", end=" ", flush=True)

        records = _call_api(
            reporter_m49, periods_str, 'X,M',
            freq='M', cmd_code='TOTAL', partner_code=partners_str
        )

        if not records:
            print("sin datos")
            time.sleep(1)
            continue

        # Procesar registros: agrupar por (mes, socio)
        month_partner_data = {}

        for rec in records:
            period = str(rec.get('period', ''))
            partner_code_val = rec.get('partnerCode', 0)
            flow = rec.get('flowCode', '')
            value = rec.get('primaryValue', 0) or 0

            partner_iso = TOP_PARTNERS.get(partner_code_val)
            if not partner_iso:
                continue

            if len(period) < 6:
                continue

            y, m = int(period[:4]), int(period[4:])
            fecha = f"{y}-{m:02d}"
            key = (fecha, partner_iso)

            if key not in month_partner_data:
                month_partner_data[key] = {
                    'fecha': fecha,
                    'pais': reporter_name,
                    'pais_code': country_code,
                    'socio': SOCIOS_NOMBRES.get(partner_iso, partner_iso),
                    'socio_code': partner_iso,
                    'exportaciones': 0,
                    'importaciones': 0,
                    'moneda_original': 'USD',
                }

            if flow == 'X':
                month_partner_data[key]['exportaciones'] += value
            elif flow == 'M':
                month_partner_data[key]['importaciones'] += value

        all_records.extend(month_partner_data.values())
        print(f"OK ({len(month_partner_data)} registros)")
        time.sleep(1)

    if not all_records:
        print("  Sin datos nuevos")
        return existing_df

    df_new = pd.DataFrame(all_records)

    if existing_df is not None and not df_new.empty:
        df_final = pd.concat([existing_df, df_new], ignore_index=True)
        df_final['fecha'] = df_final['fecha'].astype(str).str[:7]
        df_final = df_final.drop_duplicates(
            subset=['fecha', 'socio_code'], keep='last'
        )
    else:
        df_final = df_new

    df_final = df_final.sort_values(['socio_code', 'fecha'])
    data_dir.mkdir(parents=True, exist_ok=True)
    df_final.to_csv(file_path, index=False)
    print(f"  Guardado: {file_path} ({len(df_final):,} filas)")

    return df_final


# ============================================================
# MAIN
# ============================================================

def main(force=False, countries=None):
    global api_call_count
    api_call_count = 0
    incremental = not force

    if countries is None:
        countries = list(REPORTERS.keys())

    print("=" * 70)
    print("ETL UN COMTRADE - COMERCIO INTERNACIONAL (BATCH)")
    print(f"Países: {', '.join(countries)}")
    print(f"Modo: {'COMPLETO' if force else 'INCREMENTAL'}")
    print(f"API Key: {'OK' if COMTRADE_KEY else 'NO CONFIGURADA'}")
    print("=" * 70)

    if not COMTRADE_KEY:
        print("\nERROR: Se requiere COMTRADE_API_KEY")
        print("Configurar: export COMTRADE_API_KEY='tu_key'")
        return False

    success = True

    for country in countries:
        data_dir = _get_data_dir(country)
        data_dir.mkdir(parents=True, exist_ok=True)

        df_bienes = download_bienes_agregado(country, incremental=incremental)
        if df_bienes is None:
            success = False

        df_socios = download_comercio_socios(country, incremental=incremental)
        if df_socios is None:
            success = False

    # Resumen
    print(f"\n{'='*70}")
    print(f"RESUMEN (Total llamadas API: {api_call_count})")
    print(f"{'='*70}")

    for country in countries:
        data_dir = _get_data_dir(country)
        for filename in ['bienes_agregado.csv', 'comercio_socios.csv']:
            filepath = data_dir / filename
            if filepath.exists():
                df = pd.read_csv(filepath)
                size_kb = filepath.stat().st_size / 1024
                print(f"  {country}/{filename}: {len(df)} filas, {size_kb:.1f} KB")
            else:
                print(f"  {country}/{filename}: NO GENERADO")

    return success


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ETL UN Comtrade (batch)')
    parser.add_argument('--force', action='store_true',
                        help='Forzar descarga completa')
    parser.add_argument('--country', type=str, choices=['GB', 'JP', 'CA', 'CN'],
                        help='Descargar solo un país')
    args = parser.parse_args()

    countries = [args.country] if args.country else None

    if args.force and countries:
        for country in countries:
            data_dir = _get_data_dir(country)
            for filename in ['bienes_agregado.csv', 'comercio_socios.csv']:
                filepath = data_dir / filename
                if filepath.exists():
                    filepath.unlink()
                    print(f"Eliminado: {filepath}")

    main(force=args.force, countries=countries)
