# Justificación del Proyecto: Widget Meteoconomics

## 1. Introducción

El **Widget Meteoconomics** es un dashboard interactivo que permite analizar la balanza comercial
de **9 economías** — España, Francia, Alemania, Italia, Estados Unidos, Reino Unido, Japón,
Canadá y China — con granularidad mensual y sectorial.

El proyecto parte de un problema concreto: los datos de comercio internacional están fragmentados
en múltiples fuentes oficiales (Eurostat, US Census Bureau, UN Comtrade), cada una con su propio
formato de API, clasificación arancelaria y unidad monetaria. Un analista que quiera comparar la
balanza comercial de Alemania con la de Japón necesita consultar dos fuentes distintas, armonizar
clasificaciones y normalizar unidades manualmente.

El widget resuelve este problema mediante un **pipeline ETL** que integra las tres fuentes en un
modelo de datos unificado, y un **dashboard interactivo** que permite explorar los datos con
visualizaciones diseñadas para revelar patrones económicos.

El dataset resultante contiene **más de 73.000 registros** con series temporales de hasta 23 años
(2002–2025) para los países de la UE y de hasta 15 años (2010–2025) para el resto.

**Demo en vivo:** [widget-meteo.streamlit.app](https://widget-meteo.streamlit.app/)

---

## 2. Fuentes de datos y pipeline ETL

### 2.1 Tres APIs institucionales

| Fuente | Países | Clasificación | Formato API | Moneda | Período |
|--------|--------|---------------|-------------|--------|---------|
| **Eurostat** (Comext) | ES, FR, DE, IT | SITC Rev.4 | JSON-stat | EUR | 2002–2025 |
| **US Census Bureau** | US | SITC | JSON | USD | 2010–2025 |
| **UN Comtrade** | GB, JP, CA, CN | HS (mapeado a SITC) | JSON | USD | 2010–2025 |

### 2.2 Desafíos técnicos del ETL

Cada fuente presenta desafíos específicos que el ETL resuelve:

- **Eurostat**: requiere construir consultas con códigos de flujo (`FLOW`), reporter (`REPORTER`)
  y clasificación SITC de forma dinámica. Los datos de 4 países europeos se descargan y procesan
  en un único pipeline.

- **US Census Bureau**: utiliza endpoints distintos para exportaciones (`/HS`) e importaciones,
  con estructuras de respuesta diferentes que deben unificarse.

- **UN Comtrade**: la complejidad más alta. La API devuelve datos clasificados en el Sistema
  Armonizado (HS), no en SITC. El ETL implementa un **mapeo HS→SITC a nivel de capítulo**
  (97 capítulos HS → 10 sectores SITC) para garantizar consistencia con las otras fuentes.
  Además, la API devuelve desgloses por modo de transporte (`motCode`) y país de consignación
  (`partner2Code`) que deben filtrarse para evitar doble conteo — un problema sutil que infla
  los valores hasta 5x si no se detecta.

### 2.3 Modelo de datos unificado

Todas las fuentes se normalizan a un esquema común:

```
(fecha, pais, sector, exportaciones, importaciones, balance)
```

Esto permite que la capa de visualización sea completamente agnóstica respecto al origen de los
datos: el mismo código genera gráficos para España (Eurostat, EUR) y para Japón (UN Comtrade, USD).

---

## 3. Valor añadido

### 3.1 Clasificación jerárquica en super-categorías económicas

Sobre los 10 sectores SITC, el widget aplica una **agrupación en 5 super-categorías** con
significado macroeconómico:

| Super-categoría | Sectores SITC | Significado |
|----------------|---------------|-------------|
| **Agro y Alimentos** | 0, 1, 4 | Sector primario alimentario |
| **Minería y Energía** | 2, 3 | Materias primas y combustibles |
| **Químicos** | 5 | Industria farmacéutica y química |
| **Manufacturas** | 6, 7, 8 | Industria transformadora |
| **Otros** | 9 | Mercancías no clasificadas |

Esta jerarquía permite analizar la estructura productiva a dos niveles de detalle y revela
patrones como la dependencia energética de un país frente a su capacidad exportadora en
manufacturas.

### 3.2 KPIs de síntesis

El dashboard calcula cuatro indicadores para cualquier período y país:

- **Exportaciones e importaciones totales** — Volumen agregado del período
- **Balanza comercial** — Diferencia neta (superávit o déficit)
- **Tasa de cobertura** — Ratio exportaciones/importaciones (>100% indica superávit)

Estos KPIs se actualizan dinámicamente al cambiar el país o el rango de fechas.

---

## 4. Visualizaciones

### 4.1 Evolución mensual dual-axis

El gráfico principal combina dos representaciones en un solo panel:

- **Líneas** (eje izquierdo): tendencia de exportaciones e importaciones
- **Barras** (eje derecho): balance comercial mensual, con color verde (superávit) o rojo (déficit)

Esta combinación permite identificar simultáneamente la evolución de los flujos y su diferencial.
Por ejemplo, se puede observar cómo durante el COVID-19 (2020) las importaciones cayeron antes
que las exportaciones en la mayoría de economías, generando superávits transitorios.

### 4.2 Bump chart de socios comerciales

El **bump chart** muestra la evolución del ranking de los 10 principales socios comerciales
a lo largo del tiempo. Es una visualización poco convencional en dashboards de comercio exterior
que permite detectar:

- **Cambios estructurales**: el ascenso de China como primer socio comercial de Alemania
- **Impactos geopolíticos**: caída de Rusia como socio tras las sanciones de 2022
- **Dependencias comerciales**: la estabilidad de EE.UU. como primer socio de Canadá

### 4.3 Sunburst jerárquico doble

Dos gráficos sunburst muestran la **composición sectorial** de importaciones y exportaciones
en disposición lado a lado. El anillo interior representa las super-categorías y el exterior
los sectores SITC individuales.

Esta visualización revela **asimetrías estructurales**: Alemania exporta principalmente
manufacturas (>70%) pero importa un mix más diversificado; Japón importa energía y exporta
maquinaria; España tiene un perfil más equilibrado entre sectores.

### 4.4 Descarga CSV

El usuario puede exportar los datos del período seleccionado en formato CSV para análisis
propio en Excel, Python, R u otras herramientas.

---

## 5. Casos de uso

### 5.1 Detección de shocks económicos

La granularidad mensual permite observar el impacto de eventos como:

- **COVID-19 (2020)**: caída sincronizada del comercio global, con recuperación asimétrica
- **Crisis energética (2022)**: aumento de importaciones en el sector de combustibles
  minerales, especialmente visible en países europeos dependientes de gas
- **Brexit (2021)**: cambio en los patrones comerciales de Reino Unido con la UE

### 5.2 Análisis de competitividad

Comparando los sunbursts de dos países se pueden identificar ventajas competitivas:
la tasa de cobertura por sector revela en qué industrias un país es exportador neto
(competitivo) o importador neto (dependiente).

### 5.3 Evaluación de riesgo geopolítico

El bump chart permite evaluar la concentración de socios comerciales: un país cuyo
comercio depende de pocos socios tiene mayor exposición a disrupciones que uno con
una base diversificada.

---

## 6. Arquitectura técnica

### 6.1 Estructura modular

```
Widget_Meteoconomics_Master/
├── widget_meteoconomics.py       # App principal (Streamlit)
├── update_all_data.py            # Orquestador de actualización
├── src/
│   ├── config.py                 # Constantes: países, sectores, colores
│   ├── data_loader.py            # Carga y caché de datos
│   ├── charts.py                 # Visualizaciones (Plotly)
│   └── utils.py                  # Formateo y utilidades
├── etl/
│   ├── etl_data.py               # Pipeline Eurostat
│   ├── etl_us.py                 # Pipeline US Census Bureau
│   └── etl_comtrade.py           # Pipeline UN Comtrade
└── data/                         # CSVs procesados por país
```

La separación entre ETL, lógica de negocio y presentación permite modificar cualquier
capa sin afectar a las demás.

### 6.2 Actualización incremental

Los scripts ETL detectan la última fecha disponible en los CSVs existentes y descargan
solo los meses faltantes, minimizando las llamadas a las APIs (relevante para UN Comtrade,
que tiene un límite de 500 peticiones/día).

### 6.3 Caché y rendimiento

Streamlit cachea los DataFrames en memoria mediante `@st.cache_data`, evitando releer
los CSVs en cada interacción del usuario. Los datos procesados se almacenan como CSV
planos, formato que garantiza portabilidad y facilita la inspección manual.

### 6.4 Despliegue

La aplicación está desplegada en **Streamlit Community Cloud** con despliegue continuo
vinculado al repositorio de GitHub. Cada push a `main` actualiza automáticamente la
aplicación en producción.

---

## 7. Limitaciones

- **Lag temporal**: las fuentes oficiales publican datos con ~2 meses de retraso respecto
  al mes en curso, inherente al proceso de recopilación estadística.

- **Granularidad sectorial**: la clasificación SITC a 1 dígito (10 sectores) ofrece una
  visión macro. Para análisis a nivel de producto sería necesario extender a SITC de 2+
  dígitos.

- **Servicios no incluidos**: el dashboard cubre solo comercio de bienes. El comercio de
  servicios (turismo, financiero, tecnológico) no está reflejado, lo que subestima la
  balanza comercial total de economías como Reino Unido donde los servicios representan
  una parte significativa.

---

*Proyecto desarrollado como práctica académica — Widget Meteoconomics, 2025*
