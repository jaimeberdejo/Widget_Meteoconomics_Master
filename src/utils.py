from src.config import PAISES_NOMBRE, BANDERAS


def format_currency(value, symbol='€'):
    """Formatea un valor monetario con símbolo y abreviatura."""
    if abs(value) >= 1_000_000_000_000:
        return f"{symbol}{value/1_000_000_000_000:.2f} T"
    elif abs(value) >= 1_000_000_000:
        return f"{symbol}{value/1_000_000_000:.2f} B"
    elif abs(value) >= 1_000_000:
        return f"{symbol}{value/1_000_000:.0f} M"
    return f"{symbol}{value:,.0f}"


def format_partner_name(code):
    """Devuelve bandera + nombre para un código de país."""
    nombre = PAISES_NOMBRE.get(code, code)
    bandera = BANDERAS.get(code, '')
    return f"{bandera} {nombre}"


def format_value_short(val, symbol='€'):
    """Formato corto para valores en sunburst (ej: €1.2B, $345M)."""
    if abs(val) >= 1_000_000_000:
        return f"{symbol}{val/1_000_000_000:.1f}B"
    elif abs(val) >= 1_000_000:
        return f"{symbol}{val/1_000_000:.0f}M"
    elif abs(val) >= 1_000:
        return f"{symbol}{val/1_000:.0f}K"
    return f"{symbol}{val:.0f}"


def lighten_color(hex_color, factor=0.3):
    """Aclara un color hex por un factor (0-1)."""
    hex_color = hex_color.lstrip('#')
    r, g, b = int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)
    r = int(r + (255 - r) * factor)
    g = int(g + (255 - g) * factor)
    b = int(b + (255 - b) * factor)
    return f"#{r:02x}{g:02x}{b:02x}"


def darken_color(hex_color, factor=0.2):
    """Oscurece un color hex por un factor (0-1)."""
    hex_color = hex_color.lstrip('#')
    r, g, b = int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)
    r = int(r * (1 - factor))
    g = int(g * (1 - factor))
    b = int(b * (1 - factor))
    return f"#{r:02x}{g:02x}{b:02x}"
