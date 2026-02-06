"""
Script maestro para actualizar todos los datos del widget
Ejecuta el ETL unificado con manejo de errores y logging
"""

import subprocess
import sys
import argparse
from pathlib import Path
from datetime import datetime


def run_etl_script(script_name, description):
    """Ejecuta un script ETL con logging de tiempo"""
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
  python3 update_all_data.py              # Actualizar solo si cache expirado
  python3 update_all_data.py --force      # Forzar actualizacion completa
        """
    )
    parser.add_argument('--force', action='store_true',
                       help='Forzar actualizacion eliminando cache')
    args = parser.parse_args()

    print("=" * 80)
    print("ACTUALIZACION MAESTRA - WIDGET BALANZA COMERCIAL")
    print("=" * 80)

    # Forzar actualizacion
    if args.force:
        print("\n  FORZANDO ACTUALIZACION: Eliminando cache...")
        files_to_clean = [
            'data/bienes_agregado.csv',
            'data/comercio_socios.csv',
        ]
        for file_name in files_to_clean:
            file_path = Path(file_name)
            if file_path.exists():
                file_path.unlink()
                print(f"   Eliminado: {file_name}")
        print()

    # Definir ETLs a ejecutar
    etl_scripts = [
        ('etl_data.py', 'Descarga unificada (bienes agregados + socios bilaterales)'),
    ]

    start_total = datetime.now()
    success_count = 0
    failed_scripts = []

    # Ejecutar ETLs
    for script, description in etl_scripts:
        if run_etl_script(script, description):
            success_count += 1
        else:
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

    if success_count == len(etl_scripts):
        print("\n  ACTUALIZACION COMPLETA - Todos los datos actualizados")
        sys.exit(0)
    else:
        print("\n  ACTUALIZACION FALLIDA")
        print("  Verifica conectividad y logs de error")
        sys.exit(1)


if __name__ == "__main__":
    main()
