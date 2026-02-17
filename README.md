# 🌍 Balanza Comercial — Widget Meteoconomics

Dashboard interactivo de comercio internacional construido con Streamlit y Plotly.

Visualiza exportaciones, importaciones, balanza comercial y socios comerciales de **9 países** con datos oficiales de Eurostat, US Census Bureau y UN Comtrade.

**[Demo en vivo](https://widget-meteo.streamlit.app/)**

---

## Funcionalidades

- **KPIs en tiempo real** — Exportaciones, importaciones, balanza comercial y tasa de cobertura
- **Evolución mensual** — Gráfico dual-axis con líneas de comercio y barras de balance
- **Top 10 socios comerciales** — Bump chart interactivo con ranking mensual
- **Distribución sectorial** — Sunbursts jerárquicos agrupados en 5 super-categorías económicas
- **Selector de fechas** — Rango personalizable (DD/MM/YYYY)
- **Descarga CSV** — Exporta los datos filtrados del período seleccionado
- **Multi-moneda** — EUR para países UE, USD para el resto
- **Avisos de calidad de datos** — Indicadores visuales cuando hay períodos con cobertura limitada

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
| 🇨🇳 China | UN Comtrade | 2010–presente | USD |

---

## Inicio rápido

```bash
git clone https://github.com/jaimeberdejo/Widget_Meteoconomics_Master.git
cd Widget_Meteoconomics_Master
pip install -r requirements.txt

# Configurar API keys
cp .env.example .env
# Editar .env con tus claves (ver sección API Keys)

streamlit run widget_meteoconomics.py
```

## Estructura del proyecto

```
Widget_Meteoconomics_Master/
├── widget_meteoconomics.py       # Aplicación principal (Streamlit)
├── update_all_data.py            # Orquestador de actualización de datos
├── requirements.txt
├── .env.example                  # Template de variables de entorno
├── JUSTIFICACION.md              # Documento de justificación del proyecto
│
├── src/                          # Módulos del dashboard
│   ├── config.py                 # Constantes: países, banderas, colores, gaps
│   ├── utils.py                  # Formateo: moneda, colores, detección de gaps
│   ├── data_loader.py            # Carga y caché de datos
│   └── charts.py                 # Gráficos: evolución, bump chart, sunburst
│
├── etl/                          # Pipelines de extracción de datos
│   ├── __init__.py               # Constantes compartidas (SECTORES_SITC)
│   ├── etl_data.py               # Eurostat (DE, ES, FR, IT)
│   ├── etl_us.py                 # US Census Bureau (US)
│   └── etl_comtrade.py           # UN Comtrade (GB, JP, CA, CN)
│
└── data/                         # Datos procesados por país
    ├── eu/                       # Eurostat
    ├── us/                       # Census Bureau
    ├── gb/                       # UN Comtrade
    ├── jp/                       # UN Comtrade
    ├── ca/                       # UN Comtrade
    └── cn/                       # UN Comtrade
```

Cada carpeta en `data/` contiene dos CSVs:
- **`bienes_agregado.csv`** — Comercio mensual por sector SITC (10 sectores + total)
- **`comercio_socios.csv`** — Comercio bilateral con ~20 socios principales

---

## Actualización de datos

### Incremental (recomendado)

Solo descarga meses nuevos desde la última fecha existente:

```bash
python3 update_all_data.py              # Todos los países
python3 update_all_data.py --eu-only    # Solo UE (Eurostat)
python3 update_all_data.py --non-eu     # Solo US, GB, JP, CA, CN
```

### Por ETL individual

```bash
# Eurostat (sin API key)
python3 etl/etl_data.py

# US Census Bureau
export CENSUS_API_KEY='tu_api_key'
python3 etl/etl_us.py

# UN Comtrade (GB, JP, CA, CN)
export COMTRADE_API_KEY='tu_api_key'
python3 etl/etl_comtrade.py
python3 etl/etl_comtrade.py --country CN    # Solo un país
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

Copia `.env.example` a `.env` y rellena tus claves:

```bash
cp .env.example .env
```

## Sectores SITC

| Código | Sector | Super-categoría |
|--------|--------|-----------------|
| 0 | Alimentos y animales vivos | Agro y Alimentos |
| 1 | Bebidas y tabaco | Agro y Alimentos |
| 2 | Materiales crudos | Minería y Energía |
| 3 | Combustibles minerales | Minería y Energía |
| 4 | Aceites y grasas | Agro y Alimentos |
| 5 | Productos químicos | Químicos |
| 6 | Manufacturas por material | Manufacturas |
| 7 | Maquinaria y transporte | Manufacturas |
| 8 | Manufacturas diversas | Manufacturas |
| 9 | Otros | Otros |

## Limitaciones

- **Lag temporal** — Las fuentes oficiales publican datos con ~2 meses de retraso respecto al mes en curso.
- **Granularidad sectorial** — Clasificación SITC a 1 dígito (10 sectores). Para análisis a nivel de producto sería necesario extender a SITC de 2+ dígitos.

## Dependencias

```
streamlit
pandas
plotly
requests
```

---

**Meteoconomics** — Datos oficiales de [Eurostat](https://ec.europa.eu/eurostat), [US Census Bureau](https://www.census.gov/) y [UN Comtrade](https://comtradeplus.un.org/)
