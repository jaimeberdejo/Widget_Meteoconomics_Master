"""
Script maestro para actualizar todos los datos del widget
Ejecuta ETLs de múltiples fuentes:
  - Eurostat (EU: DE, ES, FR, IT)
  - US Census Bureau (US)
  - UN Comtrade (GB, JP, CA)
"""

import subprocess
import sys
import argparse
from pathlib import Path
from datetime import datetime


def run_etl_script(script_name, description, optional=False):
    """Ejecuta un script ETL con logging de tiempo"""
    script_path = Path(script_name)
    if not script_path.exists():
        if optional:
            print(f"\n  {description}: OMITIDO (script no encontrado)")
            return True  # No cuenta como fallo si es opcional
        else:
            print(f"\n  ERROR: Script no encontrado: {script_name}")
            return False

    print(f"\n{'='*80}")
    print(f"  {description}")
    print(f"{'='*80}\n")

    start_time = datetime.now()

    result = subprocess.run(
        ['python3', '-u', script_name],
        stdout=sys.stdout,
        stderr=sys.stderr,
    )

    elapsed = (datetime.now() - start_time).total_seconds()

    if result.returncode == 0:
        print(f"\n  {description} completado en {elapsed:.1f}s")
        return True
    else:
        print(f"\n  {description} fallo (exit code {result.returncode})")
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Actualizar datos del Widget Balanza Comercial',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python3 update_all_data.py              # Actualizar todos los datos
  python3 update_all_data.py --force      # Forzar actualizacion completa
  python3 update_all_data.py --eu-only    # Solo datos de Eurostat (EU)
  python3 update_all_data.py --non-eu     # Solo datos no-EU (US, UK, JP, CA)
        """
    )
    parser.add_argument('--force', action='store_true',
                       help='Forzar actualizacion eliminando cache')
    parser.add_argument('--eu-only', action='store_true',
                       help='Solo actualizar datos de Eurostat (EU)')
    parser.add_argument('--non-eu', action='store_true',
                       help='Solo actualizar datos no-EU (US, UK, JP, CA)')
    args = parser.parse_args()

    print("=" * 80)
    print("ACTUALIZACION MAESTRA - WIDGET BALANZA COMERCIAL")
    print("Fuentes: Eurostat, US Census, UN Comtrade")
    print("=" * 80)

    # Forzar actualizacion
    if args.force:
        print("\n  FORZANDO ACTUALIZACION: Eliminando cache...")
        files_to_clean = [
            # Archivos por país
            'data/eu/bienes_agregado.csv',
            'data/eu/comercio_socios.csv',
            'data/us/bienes_agregado.csv',
            'data/us/comercio_socios.csv',
            'data/gb/bienes_agregado.csv',
            'data/gb/comercio_socios.csv',
            'data/jp/bienes_agregado.csv',
            'data/jp/comercio_socios.csv',
            'data/ca/bienes_agregado.csv',
            'data/ca/comercio_socios.csv',
        ]
        for file_name in files_to_clean:
            file_path = Path(file_name)
            if file_path.exists():
                file_path.unlink()
                print(f"   Eliminado: {file_name}")
        print()

    # Definir ETLs a ejecutar
    etl_scripts = []

    # Eurostat (países EU)
    if not args.non_eu:
        etl_scripts.append(
            ('etl/etl_data.py', 'Eurostat (DE, ES, FR, IT)', False)
        )

    # Países no-EU
    if not args.eu_only:
        # US Census Bureau
        etl_scripts.append(
            ('etl/etl_us.py', 'US Census Bureau (Estados Unidos)', True)
        )
        # UN Comtrade (UK, Japan, Canada)
        etl_scripts.append(
            ('etl/etl_comtrade.py', 'UN Comtrade (GB, JP, CA)', True)
        )

    start_total = datetime.now()
    success_count = 0
    failed_scripts = []

    # Ejecutar ETLs
    for script, description, optional in etl_scripts:
        if run_etl_script(script, description, optional):
            success_count += 1
        else:
            if not optional:
                failed_scripts.append(script)

    # Resumen
    elapsed_total = (datetime.now() - start_total).total_seconds()

    print(f"\n{'='*80}")
    print(f"  RESUMEN DE ACTUALIZACION")
    print(f"{'='*80}")
    print(f"  Scripts exitosos: {success_count}/{len(etl_scripts)}")
    print(f"  Tiempo total: {elapsed_total/60:.1f} minutos")

    if failed_scripts:
        print(f"\n  Scripts fallidos:")
        for script in failed_scripts:
            print(f"   - {script}")

    # Mostrar archivos generados
    print(f"\n  Archivos de datos:")
    data_files = [
        'data/eu/bienes_agregado.csv',
        'data/eu/comercio_socios.csv',
        'data/us/bienes_agregado.csv',
        'data/us/comercio_socios.csv',
        'data/gb/bienes_agregado.csv',
        'data/gb/comercio_socios.csv',
        'data/jp/bienes_agregado.csv',
        'data/jp/comercio_socios.csv',
        'data/ca/bienes_agregado.csv',
        'data/ca/comercio_socios.csv',
    ]
    for file_name in data_files:
        file_path = Path(file_name)
        if file_path.exists():
            size_mb = file_path.stat().st_size / 1024 / 1024
            print(f"    {file_name}: {size_mb:.2f} MB")
        else:
            print(f"    {file_name}: NO EXISTE")

    if not failed_scripts:
        print("\n  ACTUALIZACION COMPLETA - Todos los datos actualizados")
        print("\n  Ejecuta el widget con: streamlit run widget_meteoconomics.py")
        sys.exit(0)
    else:
        print("\n  ACTUALIZACION PARCIAL")
        print("  Algunos scripts fallaron. Verifica logs de error.")
        sys.exit(1)


if __name__ == "__main__":
    main()
