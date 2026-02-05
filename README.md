# 🌍 Widget Meteoconomics - Balanza Comercial

Dashboard interactivo de análisis de la balanza comercial de bienes para Alemania, España, Francia e Italia, utilizando datos oficiales de Eurostat.

## 📊 Características

- **KPIs principales**: Exportaciones, Importaciones, Balance Comercial y Tasa de Cobertura
- **Evolución temporal**: Gráfico mensual de flujos comerciales y balance
- **Análisis sectorial**: Sunbursts interactivos de importaciones y exportaciones por sector SITC
- **Socios comerciales**: Bump chart con ranking evolutivo de los top 10 socios comerciales
- **Datos reales**: Actualización directa desde la API oficial de Eurostat

## 🗂️ Estructura del Proyecto

```
Widget_Meteoconomics_Master/
├── data/
│   ├── bienes_agregado.csv      # Comercio por sectores SITC (0.9 MB)
│   └── comercio_socios.csv      # Comercio bilateral con 31 socios (1.7 MB)
├── .streamlit/
│   └── config.toml              # Configuración de Streamlit
├── etl_data.py                  # ETL principal - Descarga datos de Eurostat
├── update_all_data.py           # Script maestro de actualización
├── widget_meteoconomics.py      # Dashboard Streamlit
├── requirements.txt             # Dependencias Python
└── README.md
```

## 🚀 Instalación

### Requisitos
- Python 3.9+
- pip

### Pasos

1. **Clonar el repositorio**
```bash
git clone https://github.com/jaimeberdejo/Widget_Meteoconomics_Master.git
cd Widget_Meteoconomics_Master
```

2. **Instalar dependencias**
```bash
pip install -r requirements.txt
```

3. **Descargar datos de Eurostat** (opcional, ya incluidos)
```bash
python3 etl_data.py
```

4. **Ejecutar el dashboard**
```bash
streamlit run widget_meteoconomics.py
```

El dashboard se abrirá automáticamente en `http://localhost:8501`

## 📈 Fuentes de Datos

### Eurostat DS-059331 (COMEXT)
Base de datos de comercio exterior de bienes de la Unión Europea

**Call 1: Bienes por Sector SITC**
- **Endpoint**: `ds-059331` (COMEXT)
- **Frecuencia**: Mensual
- **Período**: 2002-01 hasta presente
- **Reporters**: DE, ES, FR, IT
- **Partner**: WORLD (comercio mundial agregado)
- **Productos**: Sectores SITC 0-9 + TOTAL
  - 0: Alimentos y animales vivos
  - 1: Bebidas y tabaco
  - 2: Materiales crudos
  - 3: Combustibles minerales
  - 4: Aceites y grasas
  - 5: Productos químicos
  - 6: Manufacturas por material
  - 7: Maquinaria y transporte
  - 8: Manufacturas diversas
  - 9: Otros
- **Archivo**: `data/bienes_agregado.csv`

**Call 2: Bienes Bilaterales**
- **Endpoint**: `ds-059331` (COMEXT)
- **Frecuencia**: Mensual
- **Período**: 2002-01 hasta presente
- **Reporters**: DE, ES, FR, IT
- **Partners**: 31 países (AT, AU, BE, BR, CA, CH, CL, CN, CZ, DE, ES, FR, GB, IE, IN, IT, JP, KR, MX, NL, NO, PL, PT, RU, SA, SE, SG, TW, UA, US, VN)
- **Producto**: TOTAL (agregado)
- **Archivo**: `data/comercio_socios.csv`

## 🔄 Actualización de Datos

Los datos se actualizan automáticamente si tienen más de 7 días de antigüedad. Para forzar una actualización:

```bash
# Actualización completa (elimina cache)
python3 update_all_data.py --force

# O directamente con el ETL
python3 etl_data.py --force
```

## 📊 Archivos de Datos

### `bienes_agregado.csv`
Comercio de bienes desglosado por sector económico (SITC)

| Columna | Descripción |
|---------|-------------|
| `fecha` | Mes (YYYY-MM) |
| `pais` | Nombre del país reporter |
| `pais_code` | Código ISO (DE, ES, FR, IT) |
| `sector` | Nombre del sector SITC |
| `sector_code` | Código SITC (0-9, TOTAL) |
| `exportaciones` | Valor en EUR |
| `importaciones` | Valor en EUR |
| `balance` | Exportaciones - Importaciones |

**Tamaño**: ~0.9 MB | **Filas**: ~12,595

### `comercio_socios.csv`
Comercio bilateral de bienes con socios específicos

| Columna | Descripción |
|---------|-------------|
| `fecha` | Mes (YYYY-MM) |
| `pais` | Nombre del país reporter |
| `pais_code` | Código ISO del reporter |
| `socio` | Nombre del país socio |
| `socio_code` | Código ISO del socio |
| `exportaciones` | Valor total de bienes en EUR |
| `importaciones` | Valor total de bienes en EUR |

**Tamaño**: ~1.7 MB | **Filas**: ~34,407

## 🛠️ Scripts

### `etl_data.py`
ETL principal que descarga y procesa datos de Eurostat

```bash
# Descarga normal (usa cache si es reciente)
python3 etl_data.py

# Forzar descarga (ignora cache)
python3 etl_data.py --force
```

### `update_all_data.py`
Script maestro para actualizar todos los datos con logging

```bash
# Actualización normal
python3 update_all_data.py

# Forzar actualización completa
python3 update_all_data.py --force
```

### `widget_meteoconomics.py`
Dashboard interactivo de Streamlit

```bash
streamlit run widget_meteoconomics.py
```

## 📦 Dependencias

```
streamlit
pandas
plotly
requests
```

Ver `requirements.txt` para versiones específicas.

## 🎨 Características del Dashboard

### Selector de País
- Alemania 🇩🇪
- España 🇪🇸
- Francia 🇫🇷
- Italia 🇮🇹

### Rango Temporal Configurable
Slider interactivo para seleccionar el período de análisis (por defecto: últimos 12 meses)

### Visualizaciones

1. **KPIs (4 métricas principales)**
   - Exportaciones totales
   - Importaciones totales
   - Balance comercial
   - Tasa de cobertura (%)

2. **Evolución Mensual**
   - Líneas de exportaciones e importaciones
   - Barras de balance comercial
   - Doble eje Y para mejor visualización

3. **Sunbursts de Sectores**
   - Importaciones por sector (izquierda)
   - Exportaciones por sector (derecha)
   - Agrupación jerárquica por categorías

4. **Bump Chart de Socios**
   - Ranking evolutivo de top 10 socios comerciales
   - Toggle entre exportaciones e importaciones
   - Visualización de cambios de posición

### Descarga de Datos
Botón para descargar los datos filtrados en formato CSV

## 📝 Notas Técnicas

- **Cache**: Los datos se cachean durante 1 hora en Streamlit
- **Actualización automática**: Si los datos tienen más de 7 días, se descargan automáticamente
- **Formato de valores**: Millones (M) y Billones (B) de euros
- **Período de datos**: Desde enero 2002 hasta el presente

## 🔗 Enlaces

- **Eurostat COMEXT**: https://ec.europa.eu/eurostat/web/international-trade-in-goods/data/database
- **API Documentation**: https://wikis.ec.europa.eu/display/EUROSTATHELP/API+-+Getting+started+with+statistics+API

## 📄 Licencia

Este proyecto utiliza datos públicos de Eurostat. Los datos están sujetos a la [política de copyright de Eurostat](https://ec.europa.eu/eurostat/about-us/policies/copyright).

## 👤 Autor

Jaime Berdejo - [GitHub](https://github.com/jaimeberdejo)

## 🤝 Contribuciones

Las contribuciones son bienvenidas. Por favor, abre un issue o pull request para sugerencias o mejoras.

---

**Meteoconomics** - Datos reales desde la API oficial de Eurostat
