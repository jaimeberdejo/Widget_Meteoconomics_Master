# 🌍 Balanza Comercial — Widget Meteoconomics

Dashboard interactivo de comercio internacional construido con Streamlit y Plotly.

Visualiza exportaciones, importaciones, balanza comercial y socios comerciales de 8 países con datos oficiales de Eurostat, US Census Bureau y UN Comtrade.

**[Demo en vivo](https://widget-meteo.streamlit.app/)**

---

## Funcionalidades

- **KPIs en tiempo real** — Exportaciones, importaciones, balanza comercial y tasa de cobertura con tooltips explicativos
- **Evolución mensual** — Gráfico dual con líneas de comercio y barras de balance
- **Top 10 socios comerciales** — Bump chart interactivo con ranking mensual
- **Distribución sectorial** — Sunbursts jerárquicos agrupados por categoría SITC
- **Selector de fechas** — Rango personalizable con date inputs DD/MM/YYYY
- **Descarga CSV** — Exporta los datos filtrados del período seleccionado
- **Multi-moneda** — EUR para países UE, USD para el resto

## Datos disponibles

| País | Fuente | Período | Moneda |
|------|--------|---------|--------|
| 🇩🇪 Alemania | Eurostat | 2002–presente | EUR |
| 🇪🇸 España | Eurostat | 2002–presente | EUR |
| 🇫🇷 Francia | Eurostat | 2002–presente | EUR |
| 🇮🇹 Italia | Eurostat | 2002–presente | EUR |
| 🇺🇸 Estados Unidos | Census Bureau | 2010–presente | USD |
| 🇬🇧 Reino Unido | UN Comtrade | 2010–presente | USD |
| 🇯🇵 Japón | UN Comtrade | 2010–presente | USD |
| 🇨🇦 Canadá | UN Comtrade | 2010–presente | USD |

---

## Inicio rápido

```bash
git clone https://github.com/jaimeberdejo/Widget_Meteoconomics_Master.git
cd Widget_Meteoconomics_Master
pip install -r requirements.txt
streamlit run widget_meteoconomics.py
```

## Estructura del proyecto

```
Widget_Meteoconomics_Master/
├── widget_meteoconomics.py       # Orquestador principal (Streamlit)
├── update_all_data.py            # Script de actualización de datos
├── requirements.txt
│
├── src/                          # Módulos del dashboard
│   ├── config.py                 # Constantes: países, banderas, colores, CSS
│   ├── utils.py                  # Formateo: moneda, colores, nombres
│   ├── data_loader.py            # Carga y caché de datos
│   └── charts.py                 # Gráficos: evolución, bump chart, sunburst
│
├── etl/                          # Pipelines de extracción de datos
│   ├── __init__.py               # Constantes compartidas (SECTORES_SITC)
│   ├── etl_data.py               # Eurostat (DE, ES, FR, IT)
│   ├── etl_us.py                 # US Census Bureau
│   └── etl_comtrade.py           # UN Comtrade (GB, JP, CA)
│
└── data/                         # Datos por país
    ├── eu/                       # Eurostat → bienes_agregado + comercio_socios
    ├── us/                       # Census Bureau
    ├── gb/                       # UN Comtrade
    ├── jp/                       # UN Comtrade
    └── ca/                       # UN Comtrade
```

Cada carpeta en `data/` contiene dos CSVs:
- **`bienes_agregado.csv`** — Comercio mensual por sector SITC (10 sectores + total)
- **`comercio_socios.csv`** — Comercio bilateral con ~20 socios principales

---

## Actualización de datos

### Incremental (recomendado)

Solo descarga meses nuevos desde la última fecha existente:

```bash
python3 update_all_data.py          # Todo
python3 update_all_data.py --eu-only    # Solo UE
python3 update_all_data.py --non-eu     # Solo US, GB, JP, CA
```

### Por ETL individual

```bash
# Eurostat (sin API key)
python3 etl/etl_data.py

# US Census Bureau
export CENSUS_API_KEY='tu_api_key'
python3 etl/etl_us.py

# UN Comtrade (GB, JP, CA)
export COMTRADE_API_KEY='tu_api_key'
python3 etl/etl_comtrade.py
python3 etl/etl_comtrade.py --country GB    # Solo un país
```

### Descarga completa

Descarga todo el histórico ignorando datos existentes:

```bash
python3 update_all_data.py --force
```

---

## API Keys

| Fuente | Registro | Variable de entorno |
|--------|----------|---------------------|
| Eurostat | No requiere | — |
| [US Census Bureau](https://api.census.gov/data/key_signup.html) | Gratis | `CENSUS_API_KEY` |
| [UN Comtrade](https://comtradeplus.un.org/) | Gratis (500 calls/día) | `COMTRADE_API_KEY` |

## Sectores SITC

| Código | Sector |
|--------|--------|
| 0 | Alimentos y animales vivos |
| 1 | Bebidas y tabaco |
| 2 | Materiales crudos |
| 3 | Combustibles minerales |
| 4 | Aceites y grasas |
| 5 | Productos químicos |
| 6 | Manufacturas por material |
| 7 | Maquinaria y transporte |
| 8 | Manufacturas diversas |
| 9 | Otros |

En el dashboard, estos se agrupan en 5 categorías para los sunbursts: **Agro y Alimentos** (0,1,4), **Minería y Energía** (2,3), **Químicos** (5), **Manufacturas** (6,7,8) y **Otros** (9).

## Dependencias

```
streamlit
pandas
plotly
requests
```

---

**Meteoconomics** — Datos oficiales de [Eurostat](https://ec.europa.eu/eurostat), [US Census Bureau](https://www.census.gov/) y [UN Comtrade](https://comtradeplus.un.org/)
