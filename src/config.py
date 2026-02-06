"""
Configuration constants for the Trade Balance Dashboard.

Contains country codes, flags, sector definitions, and styling configuration.
"""

from pathlib import Path

# Data folder paths by country group
DATA_FOLDERS = {
    "eu": Path("data/eu"),  # DE, ES, FR, IT
    "us": Path("data/us"),  # US
    "uk": Path("data/uk"),  # GB
    "jp": Path("data/jp"),  # JP
    "ca": Path("data/ca"),  # CA
}

# Available countries for selection (EU + non-EU)
PAISES_V1 = {
    # EU (Eurostat)
    "ES": "España",
    "FR": "Francia",
    "DE": "Alemania",
    "IT": "Italia",
    # Non-EU (new data sources)
    "US": "Estados Unidos",
    "GB": "Reino Unido",
    "JP": "Japón",
    "CA": "Canadá",
}

# Country flag emojis
BANDERAS = {
    "ES": "🇪🇸",
    "FR": "🇫🇷",
    "DE": "🇩🇪",
    "IT": "🇮🇹",
    "GB": "🇬🇧",
    "AT": "🇦🇹",
    "BE": "🇧🇪",
    "BG": "🇧🇬",
    "HR": "🇭🇷",
    "CY": "🇨🇾",
    "CZ": "🇨🇿",
    "DK": "🇩🇰",
    "EE": "🇪🇪",
    "FI": "🇫🇮",
    "GR": "🇬🇷",
    "HU": "🇭🇺",
    "IE": "🇮🇪",
    "LV": "🇱🇻",
    "LT": "🇱🇹",
    "LU": "🇱🇺",
    "MT": "🇲🇹",
    "NL": "🇳🇱",
    "PL": "🇵🇱",
    "PT": "🇵🇹",
    "RO": "🇷🇴",
    "SK": "🇸🇰",
    "SI": "🇸🇮",
    "SE": "🇸🇪",
    "NO": "🇳🇴",
    "CH": "🇨🇭",
    "CN": "🇨🇳",
    "US": "🇺🇸",
    "TR": "🇹🇷",
    "RU": "🇷🇺",
    "JP": "🇯🇵",
    "IN": "🇮🇳",
    "KR": "🇰🇷",
    "BR": "🇧🇷",
    "MX": "🇲🇽",
    "CA": "🇨🇦",
    "AU": "🇦🇺",
    "SA": "🇸🇦",
    "SG": "🇸🇬",
    "VN": "🇻🇳",
    "UA": "🇺🇦",
    "TW": "🇹🇼",
    "CL": "🇨🇱",
    "UK": "🇬🇧",
}

# Country names in Spanish
PAISES_NOMBRE = {
    "AT": "Austria",
    "BE": "Bélgica",
    "BG": "Bulgaria",
    "HR": "Croacia",
    "CZ": "Rep. Checa",
    "DK": "Dinamarca",
    "FI": "Finlandia",
    "FR": "Francia",
    "DE": "Alemania",
    "GR": "Grecia",
    "HU": "Hungría",
    "IE": "Irlanda",
    "IT": "Italia",
    "NL": "P. Bajos",
    "PL": "Polonia",
    "PT": "Portugal",
    "RO": "Rumanía",
    "SK": "Eslovaquia",
    "SI": "Eslovenia",
    "ES": "España",
    "SE": "Suecia",
    "GB": "R. Unido",
    "UK": "R. Unido",
    "CH": "Suiza",
    "NO": "Noruega",
    "CN": "China",
    "US": "EE.UU.",
    "TR": "Turquía",
    "RU": "Rusia",
    "JP": "Japón",
    "IN": "India",
    "KR": "Corea S.",
    "BR": "Brasil",
    "MX": "México",
    "CA": "Canadá",
    "AU": "Australia",
    "SA": "A. Saudita",
    "SG": "Singapur",
    "VN": "Vietnam",
    "UA": "Ucrania",
    "TW": "Taiwán",
    "CL": "Chile",
}

# SITC Sectors - MUST MATCH etl_data.py
SECTORES_SITC = {
    "TOTAL": "Total Comercio",
    "0": "Alimentos y animales vivos",
    "1": "Bebidas y tabaco",
    "2": "Materiales crudos",
    "3": "Combustibles minerales",
    "4": "Aceites y grasas",
    "5": "Productos químicos",
    "6": "Manufacturas por material",
    "7": "Maquinaria y transporte",
    "8": "Manufacturas diversas",
    "9": "Otros",
}

# Sunburst chart groupings
GRUPOS_SUNBURST = {
    "Agro y Alimentos": ["0", "1", "4"],
    "Minería y Energía": ["2", "3"],
    "Químicos": ["5"],
    "Manufacturas": ["6", "7", "8"],
    "Otros": ["9"],
}

# Sector to group mapping
SECTOR_A_GRUPO = {}
for grupo, sectores in GRUPOS_SUNBURST.items():
    for s in sectores:
        SECTOR_A_GRUPO[s] = grupo

# Enhanced color scheme for sunburst charts
SUNBURST_BASE_COLORS = {
    "Agro y Alimentos": "#2E86AB",
    "Minería y Energía": "#F18F01",
    "Químicos": "#C73E1D",
    "Manufacturas": "#6A994E",
    "Otros": "#8B8C89",
}

# Custom CSS for compact layout
CUSTOM_CSS = """
<style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    .block-container {
        padding: 1rem 2rem !important;
        max-width: 1100px !important;
    }
    [data-testid="stMetric"] {
        background: transparent;
        padding: 0.2rem 0.5rem !important;
    }
    [data-testid="stMetricLabel"] { font-size: 0.8rem !important; }
    [data-testid="stMetricValue"] { font-size: 1.2rem !important; }
    h3 { font-size: 1rem !important; margin: 0.3rem 0 !important; }
    h5 { font-size: 0.9rem !important; margin: 0.5rem 0 0.3rem 0 !important; }
    .element-container { margin-bottom: 0.2rem !important; }
</style>
"""
