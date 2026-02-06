# Widget Meteoconomics - Balanza Comercial

Dashboard interactivo de comercio internacional con datos de Eurostat (UE) y US Census Bureau (Estados Unidos).

**Demo en vivo:** https://widget-meteo.streamlit.app/

## Datos Disponibles

| Pais | Fuente | Periodo | Estado |
|------|--------|---------|--------|
| Alemania (DE) | Eurostat | 2002-2025 | Completo |
| Espana (ES) | Eurostat | 2002-2025 | Completo |
| Francia (FR) | Eurostat | 2002-2025 | Completo |
| Italia (IT) | Eurostat | 2002-2025 | Completo |
| Estados Unidos (US) | Census Bureau | 2010-2025 | Completo |
| Reino Unido (GB) | HMRC | - | Pendiente |
| Japon (JP) | e-Stat | - | Pendiente |
| Canada (CA) | StatCan | - | Pendiente |

## Estructura del Proyecto

```
Widget_Meteoconomics_Master/
├── widget_meteoconomics.py      # Dashboard Streamlit
├── update_all_data.py           # Orquestador de ETLs
├── requirements.txt
├── etl/                         # Scripts de extraccion de datos
│   ├── etl_data.py             # Eurostat (DE, ES, FR, IT)
│   ├── etl_us.py               # US Census Bureau
│   ├── etl_uk.py               # UK HMRC (pendiente)
│   ├── etl_japan.py            # Japan e-Stat (pendiente)
│   ├── etl_canada.py           # Canada StatCan (pendiente)
│   ├── etl_currency.py         # Tasas de cambio ECB
│   └── etl_integrator.py       # Integrador de fuentes
└── data/                        # Datos por pais
    ├── eu/                      # Eurostat (12,595 filas)
    │   ├── bienes_agregado.csv
    │   └── comercio_socios.csv
    ├── us/                      # Census Bureau (2,101 filas)
    │   ├── bienes_agregado.csv
    │   └── comercio_socios.csv
    ├── uk/                      # (vacio)
    ├── jp/                      # (vacio)
    ├── ca/                      # (vacio)
    └── exchange_rates.csv       # Tasas EUR/USD/GBP/JPY/CAD
```

## Instalacion

```bash
# Clonar repositorio
git clone https://github.com/jaimeberdejo/Widget_Meteoconomics_Master.git
cd Widget_Meteoconomics_Master

# Instalar dependencias
pip install -r requirements.txt

# Ejecutar dashboard
streamlit run widget_meteoconomics.py
```

## Actualizacion de Datos

### Modo Incremental (recomendado)

Solo descarga meses nuevos desde la ultima fecha existente:

```bash
# Actualizar datos EU (Eurostat)
python3 etl/etl_data.py

# Actualizar datos US (Census Bureau)
export CENSUS_API_KEY='tu_api_key'
python3 etl/etl_us.py
```

### Descarga Completa

Descarga todo el historico desde el inicio:

```bash
python3 etl/etl_data.py --force
python3 etl/etl_us.py --force
```

### Actualizar Todo

```bash
python3 update_all_data.py           # Incremental
python3 update_all_data.py --force   # Completo
python3 update_all_data.py --eu-only # Solo Eurostat
```

## API Keys

### US Census Bureau (requerida para US)

1. Registrarse gratis en: https://api.census.gov/data/key_signup.html
2. Configurar variable de entorno:
   ```bash
   export CENSUS_API_KEY='tu_api_key'
   ```

### Japan e-Stat (para JP, pendiente)

1. Registrarse en: https://www.e-stat.go.jp/api/
2. Configurar:
   ```bash
   export ESTAT_API_KEY='tu_api_key'
   ```

## Fuentes de Datos

### Eurostat DS-059331 (UE)

- **Paises**: DE, ES, FR, IT
- **Clasificacion**: SITC (Standard International Trade Classification)
- **Moneda**: EUR
- **Periodo**: 2002-presente
- **Frecuencia**: Mensual

### US Census Bureau (US)

- **API**: https://api.census.gov/data/timeseries/intltrade/
- **Clasificacion**: SITC
- **Moneda**: USD
- **Periodo**: 2010-presente
- **Frecuencia**: Mensual

Endpoints:
- `/exports/sitc` - Exportaciones (variable: ALL_VAL_MO)
- `/imports/sitc` - Importaciones (variable: GEN_VAL_MO)

## Estructura de Datos

### bienes_agregado.csv

Comercio por sector SITC (10 sectores + TOTAL):

| Columna | Descripcion |
|---------|-------------|
| fecha | YYYY-MM-DD |
| pais | Nombre del pais |
| pais_code | Codigo ISO (DE, ES, FR, IT, US) |
| sector | Nombre del sector |
| sector_code | Codigo SITC (0-9, TOTAL) |
| exportaciones | Valor en moneda original |
| importaciones | Valor en moneda original |
| balance | Exportaciones - Importaciones |
| moneda_original | EUR o USD |

### comercio_socios.csv

Comercio bilateral con 30 socios principales:

| Columna | Descripcion |
|---------|-------------|
| fecha | YYYY-MM-DD |
| pais | Reporter |
| pais_code | Codigo ISO reporter |
| socio | Partner |
| socio_code | Codigo ISO partner |
| exportaciones | Valor total |
| importaciones | Valor total |
| moneda_original | EUR o USD |

### Sectores SITC

| Codigo | Sector |
|--------|--------|
| 0 | Alimentos y animales vivos |
| 1 | Bebidas y tabaco |
| 2 | Materiales crudos |
| 3 | Combustibles minerales |
| 4 | Aceites y grasas |
| 5 | Productos quimicos |
| 6 | Manufacturas por material |
| 7 | Maquinaria y transporte |
| 8 | Manufacturas diversas |
| 9 | Otros |
| TOTAL | Total comercio |

## Dependencias

```
streamlit
pandas
plotly
requests
```

---

**Meteoconomics** - Datos oficiales de Eurostat y US Census Bureau
