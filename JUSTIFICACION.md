# Justificación del Proyecto: Widget Meteoconomics

## 1. Introducción

El **Widget Meteoconomics** es un dashboard interactivo de análisis de balanza comercial que permite explorar
los flujos de importación y exportación de bienes de **9 economías** (España, Francia, Alemania, Italia,
Estados Unidos, Reino Unido, Japón, Canadá y China) con datos mensuales.

El proyecto nace de una necesidad real: los datos de comercio internacional están dispersos en múltiples
fuentes oficiales, cada una con formatos, clasificaciones y periodicidades distintas. Esto dificulta el
análisis comparativo entre economías. El widget resuelve este problema ofreciendo una **interfaz unificada**
que armoniza tres fuentes heterogéneas en un modelo de datos consistente.

---

## 2. Fuentes de datos oficiales

El proyecto integra datos de **tres APIs institucionales**:

| Fuente | Países | Clasificación nativa | Datos desde |
|--------|--------|---------------------|-------------|
| **Eurostat** (Comext) | ES, FR, DE, IT | SITC Rev.4 | 2002 |
| **US Census Bureau** | US | SITC | 2010 |
| **UN Comtrade** | GB, JP, CA, CN | HS → SITC (mapeado) | 2010 |

Cada fuente requiere un proceso de extracción, transformación y carga (ETL) específico que normaliza
los datos a un esquema común: `(fecha, pais, sector, exportaciones, importaciones)`.

---

## 3. Valor añadido al análisis económico

### 3.1 Armonización de fuentes heterogéneas

Las tres fuentes utilizan formatos de respuesta distintos (JSON/CSV/SDMX), clasificaciones sectoriales
diferentes y unidades monetarias variadas (EUR vs USD). El proyecto implementa un pipeline ETL que
normaliza todo a un modelo unificado, permitiendo comparar directamente la balanza comercial de
España con la de Japón o China.

### 3.2 Mapeo HS → SITC para consistencia sectorial

Los datos de UN Comtrade llegan clasificados en el Sistema Armonizado (HS), mientras que Eurostat
y US Census Bureau utilizan SITC directamente. El ETL incluye un **mapeo HS→SITC a nivel de
capítulo** para los datos de Comtrade, garantizando que los 10 sectores mostrados son consistentes
independientemente de la fuente original.

### 3.3 Clasificación jerárquica en super-categorías económicas

Sobre los 10 sectores SITC, el widget aplica una **agrupación en 5 super-categorías** con significado
económico claro:

- **Agro y Alimentos** — Sectores 0, 1, 4
- **Minería y Energía** — Sectores 2, 3
- **Químicos** — Sector 5
- **Manufacturas** — Sectores 6, 7, 8
- **Otros** — Sector 9

Esta jerarquía permite analizar la estructura productiva de cada economía a dos niveles de detalle.

### 3.4 KPIs clave

El dashboard calcula y muestra cuatro indicadores para el período seleccionado:

- **Total exportaciones** e **importaciones** — Volumen agregado
- **Balanza comercial** — Superávit o déficit
- **Tasa de cobertura** — Ratio exportaciones/importaciones (>100% = superávit)

---

## 4. Creatividad y funcionalidades

### 4.1 Bump chart de socios comerciales

El **bump chart** muestra la evolución del ranking de los 10 principales socios comerciales a lo
largo del tiempo. Este tipo de visualización es poco común en dashboards de comercio exterior y
permite identificar rápidamente:

- Cambios en las relaciones bilaterales (p.ej., ascenso de China como socio de la UE)
- Impactos de eventos geopolíticos (sanciones, acuerdos comerciales)
- Estabilidad o volatilidad de las dependencias comerciales

### 4.2 Sunburst jerárquico doble (importaciones vs exportaciones)

Dos gráficos sunburst permiten comparar visualmente la **composición sectorial** de importaciones
y exportaciones. La disposición lado a lado revela **asimetrías estructurales**: por ejemplo,
un país que exporta principalmente manufacturas pero importa energía y materias primas.

Cada segmento muestra valor absoluto y porcentaje, con colores consistentes entre ambos gráficos
para facilitar la comparación.

### 4.3 Evolución dual-axis (líneas + barras de balance)

El gráfico de evolución combina:

- **Líneas** para exportaciones e importaciones (eje izquierdo)
- **Barras** para el balance comercial (eje derecho)

Esta representación dual permite ver simultáneamente la tendencia de los flujos y su diferencial,
con colores que indican superávit (verde) o déficit (rojo).

### 4.4 Descarga CSV

El usuario puede descargar los datos del período seleccionado en formato CSV para análisis propio
en Excel, Python, R u otras herramientas.

---

## 5. Utilidad práctica

### 5.1 Identificación de tendencias y shocks económicos

La granularidad mensual permite detectar eventos como:

- Caídas abruptas del comercio (COVID-19, crisis financiera 2008)
- Recuperaciones asimétricas entre importaciones y exportaciones
- Estacionalidad en sectores específicos

### 5.2 Análisis de dependencias bilaterales

El bump chart revela la concentración o diversificación de socios comerciales, información
relevante para evaluar riesgos geopolíticos y cadenas de suministro.

### 5.3 Comparativa sectorial entre economías

La estructura jerárquica (super-categorías → sectores SITC) permite comparar perfiles productivos:
¿Es Alemania más dependiente de manufacturas que España? ¿Cómo se distribuyen las importaciones
energéticas de Japón frente a las de Italia?

### 5.4 Soporte a análisis fundamental y técnico

Los datos descargables pueden alimentar modelos cuantitativos, análisis de correlación con tipos
de cambio, o estudios de competitividad sectorial.

---

## 6. Arquitectura técnica

### 6.1 Modularidad

El proyecto sigue una estructura modular clara:

```
Widget_Meteoconomics_Master/
├── widget_meteoconomics.py   # Aplicación principal (Streamlit)
├── src/
│   ├── config.py             # Configuración centralizada
│   ├── data_loader.py        # Carga y caché de datos
│   ├── charts.py             # Funciones de visualización (Plotly)
│   └── utils.py              # Utilidades de formato
├── data/                     # Datos por país (CSV procesados)
│   ├── eu/                   # Eurostat: ES, FR, DE, IT
│   ├── us/                   # US Census Bureau
│   ├── gb/                   # UN Comtrade: Reino Unido
│   ├── jp/                   # UN Comtrade: Japón
│   ├── ca/                   # UN Comtrade: Canadá
│   └── cn/                   # UN Comtrade: China
└── etl/                      # Scripts de extracción y transformación
```

### 6.2 ETL incremental y caché

- Los scripts ETL pueden ejecutarse de forma incremental, descargando solo los meses faltantes
- Streamlit cachea los DataFrames en memoria (`@st.cache_data`) para evitar recargas innecesarias
- Los datos procesados se almacenan como CSV planos, facilitando la portabilidad

### 6.3 Despliegue en Streamlit Cloud

La aplicación está preparada para despliegue directo en **Streamlit Community Cloud**, sin
necesidad de infraestructura adicional. El archivo `requirements.txt` incluye todas las
dependencias necesarias.

---

## 7. Limitaciones y transparencia

### 7.1 Lag temporal de las fuentes

Las fuentes oficiales publican datos con un retraso aproximado de 2 meses respecto al mes
en curso. Este lag es inherente al proceso de recopilación estadística y no es controlable
por el proyecto.

### 7.2 Granularidad sectorial

La clasificación SITC a 1 dígito (10 sectores) ofrece una visión macro. Para análisis más
detallados a nivel de producto, sería necesario extender el modelo a SITC de 2 o más dígitos.

---

*Proyecto desarrollado como práctica académica — Widget Meteoconomics, 2025*
