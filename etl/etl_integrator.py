"""
ETL Integrator - Combina datos de todas las fuentes
Integra: Eurostat (EU), Census Bureau (US), HMRC (UK), e-Stat (JP), StatCan (CA)

Convierte todas las monedas a EUR usando tasas del ECB.
Genera archivos unificados para el widget.

Ejecutar: python3 etl_integrator.py [--force]
"""

import argparse
import pandas as pd
from datetime import datetime
from pathlib import Path

# ============================================================
# CONSTANTES
# ============================================================

DATA_DIR = Path(__file__).parent.parent / "data"

# Archivos de entrada por país
FILES_BIENES = {
    "EU": DATA_DIR / "eu" / "bienes_agregado.csv",  # Eurostat (DE, ES, FR, IT)
    "US": DATA_DIR / "us" / "bienes_agregado.csv",
    "GB": DATA_DIR / "uk" / "bienes_agregado.csv",
    "JP": DATA_DIR / "jp" / "bienes_agregado.csv",
    "CA": DATA_DIR / "ca" / "bienes_agregado.csv",
}

FILES_SOCIOS = {
    "EU": DATA_DIR / "eu" / "comercio_socios.csv",
    "US": DATA_DIR / "us" / "comercio_socios.csv",
    "GB": DATA_DIR / "uk" / "comercio_socios.csv",
    "JP": DATA_DIR / "jp" / "comercio_socios.csv",
    "CA": DATA_DIR / "ca" / "comercio_socios.csv",
}

# Archivo de tasas de cambio
FILE_EXCHANGE_RATES = DATA_DIR / "exchange_rates.csv"

# Archivos de salida unificados
FILE_BIENES_UNIFICADO = DATA_DIR / "bienes_agregado.csv"
FILE_SOCIOS_UNIFICADO = DATA_DIR / "comercio_socios.csv"


# ============================================================
# FUNCIONES
# ============================================================


def load_exchange_rates():
    """Carga tasas de cambio del ECB."""
    if not FILE_EXCHANGE_RATES.exists():
        print("  AVISO: No hay tasas de cambio. Usando tasas fijas.")
        return None

    df = pd.read_csv(FILE_EXCHANGE_RATES)
    # Normalizar fecha
    df["fecha_mes"] = df["fecha"].astype(str).str[:7]
    return df


def convert_to_eur(df, rates_df):
    """
    Convierte valores de moneda original a EUR.
    Tasa ECB: 1 EUR = X moneda_original
    Conversión: valor_eur = valor_original / tasa
    """
    if rates_df is None or "moneda_original" not in df.columns:
        return df

    df_result = df.copy()

    # Crear columna de fecha normalizada
    if "fecha" in df_result.columns:
        df_result["fecha_mes"] = pd.to_datetime(df_result["fecha"]).dt.strftime("%Y-%m")
    else:
        return df_result

    # Merge con tasas
    df_merged = df_result.merge(
        rates_df[["fecha_mes", "USD", "GBP", "JPY", "CAD"]].drop_duplicates(),
        on="fecha_mes",
        how="left",
    )

    # Aplicar conversión por moneda
    value_cols = ["exportaciones", "importaciones"]

    for col in value_cols:
        if col not in df_merged.columns:
            continue

        # USD
        mask_usd = df_merged["moneda_original"] == "USD"
        if mask_usd.any() and "USD" in df_merged.columns:
            df_merged.loc[mask_usd, col] = (
                df_merged.loc[mask_usd, col] / df_merged.loc[mask_usd, "USD"]
            )

        # GBP
        mask_gbp = df_merged["moneda_original"] == "GBP"
        if mask_gbp.any() and "GBP" in df_merged.columns:
            df_merged.loc[mask_gbp, col] = (
                df_merged.loc[mask_gbp, col] / df_merged.loc[mask_gbp, "GBP"]
            )

        # JPY
        mask_jpy = df_merged["moneda_original"] == "JPY"
        if mask_jpy.any() and "JPY" in df_merged.columns:
            df_merged.loc[mask_jpy, col] = (
                df_merged.loc[mask_jpy, col] / df_merged.loc[mask_jpy, "JPY"]
            )

        # CAD
        mask_cad = df_merged["moneda_original"] == "CAD"
        if mask_cad.any() and "CAD" in df_merged.columns:
            df_merged.loc[mask_cad, col] = (
                df_merged.loc[mask_cad, col] / df_merged.loc[mask_cad, "CAD"]
            )

    # Recalcular balance
    if "balance" in df_merged.columns:
        df_merged["balance"] = df_merged["exportaciones"] - df_merged["importaciones"]

    # Limpiar columnas temporales
    cols_to_drop = ["fecha_mes", "USD", "GBP", "JPY", "CAD"]
    df_merged = df_merged.drop(
        columns=[c for c in cols_to_drop if c in df_merged.columns], errors="ignore"
    )

    return df_merged


def integrate_bienes():
    """
    Integra todos los archivos de bienes agregados.
    """
    print("\n" + "=" * 70)
    print("INTEGRANDO BIENES AGREGADOS")
    print("=" * 70)

    rates_df = load_exchange_rates()
    all_dfs = []

    for source, filepath in FILES_BIENES.items():
        if not filepath.exists():
            print(f"  {source}: No encontrado ({filepath.name})")
            continue

        df = pd.read_csv(filepath)
        print(f"  {source}: {len(df):,} filas cargadas")

        # Convertir a EUR si tiene moneda original
        if "moneda_original" in df.columns and rates_df is not None:
            df_converted = convert_to_eur(df, rates_df)
            print(f"       Convertido a EUR")
        else:
            df_converted = df

        all_dfs.append(df_converted)

    if not all_dfs:
        print("  ERROR: No hay datos para integrar")
        return None

    # Combinar todos
    df_unified = pd.concat(all_dfs, ignore_index=True)

    # Columnas estándar (sin las de moneda original)
    std_cols = [
        "fecha",
        "pais",
        "pais_code",
        "sector",
        "sector_code",
        "exportaciones",
        "importaciones",
        "balance",
    ]
    df_final = df_unified[[c for c in std_cols if c in df_unified.columns]].copy()

    # Ordenar
    df_final = df_final.sort_values(["pais_code", "sector_code", "fecha"])

    # Guardar
    df_final.to_csv(FILE_BIENES_UNIFICADO, index=False)
    print(f"\n  Guardado: {FILE_BIENES_UNIFICADO}")
    print(f"  Total: {len(df_final):,} filas")
    print(f"  Países: {df_final['pais_code'].nunique()}")

    return df_final


def integrate_socios():
    """
    Integra todos los archivos de comercio bilateral.
    """
    print("\n" + "=" * 70)
    print("INTEGRANDO COMERCIO BILATERAL")
    print("=" * 70)

    rates_df = load_exchange_rates()
    all_dfs = []

    for source, filepath in FILES_SOCIOS.items():
        if not filepath.exists():
            print(f"  {source}: No encontrado ({filepath.name})")
            continue

        df = pd.read_csv(filepath)
        print(f"  {source}: {len(df):,} filas cargadas")

        # Convertir a EUR si tiene moneda original
        if "moneda_original" in df.columns and rates_df is not None:
            df_converted = convert_to_eur(df, rates_df)
            print(f"       Convertido a EUR")
        else:
            df_converted = df

        all_dfs.append(df_converted)

    if not all_dfs:
        print("  ERROR: No hay datos para integrar")
        return None

    # Combinar todos
    df_unified = pd.concat(all_dfs, ignore_index=True)

    # Columnas estándar
    std_cols = [
        "fecha",
        "pais",
        "pais_code",
        "socio",
        "socio_code",
        "exportaciones",
        "importaciones",
    ]
    df_final = df_unified[[c for c in std_cols if c in df_unified.columns]].copy()

    # Ordenar
    df_final = df_final.sort_values(["pais_code", "socio_code", "fecha"])

    # Guardar
    df_final.to_csv(FILE_SOCIOS_UNIFICADO, index=False)
    print(f"\n  Guardado: {FILE_SOCIOS_UNIFICADO}")
    print(f"  Total: {len(df_final):,} filas")
    print(f"  Países reporteros: {df_final['pais_code'].nunique()}")

    return df_final


# ============================================================
# MAIN
# ============================================================


def main(force=False):
    """Integra todos los datos de comercio."""
    print("=" * 70)
    print("ETL INTEGRADOR - COMERCIO INTERNACIONAL UNIFICADO")
    print(f"Fuentes: Eurostat, US Census, UK HMRC, Japan e-Stat, Canada StatCan")
    print("=" * 70)

    # Integrar bienes
    df_bienes = integrate_bienes()

    # Integrar socios
    df_socios = integrate_socios()

    # Resumen final
    print(f"\n{'=' * 70}")
    print("RESUMEN INTEGRACIÓN")
    print(f"{'=' * 70}")

    for f in [FILE_BIENES_UNIFICADO, FILE_SOCIOS_UNIFICADO]:
        if f.exists():
            size_mb = f.stat().st_size / 1024 / 1024
            print(f"  {f.name}: {size_mb:.2f} MB")
        else:
            print(f"  {f.name}: NO GENERADO")

    if df_bienes is not None:
        print(f"\n  Países disponibles en bienes:")
        for code in sorted(df_bienes["pais_code"].unique()):
            nombre = df_bienes[df_bienes["pais_code"] == code]["pais"].iloc[0]
            count = len(df_bienes[df_bienes["pais_code"] == code])
            print(f"    {code}: {nombre} ({count:,} registros)")

    return df_bienes is not None and df_socios is not None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Integrador")
    parser.add_argument("--force", action="store_true", help="Forzar regeneración")
    args = parser.parse_args()

    main(force=args.force)
