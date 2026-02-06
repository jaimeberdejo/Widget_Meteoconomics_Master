"""
Utility functions for formatting and displaying data.
"""

from src.config import PAISES_NOMBRE, BANDERAS


def format_currency(value):
    """Format value as currency with appropriate suffix (T/B/M).

    Args:
        value: Numeric value to format.

    Returns:
        Formatted string with € symbol and suffix.
    """
    if abs(value) >= 1_000_000_000_000:
        return f"€{value / 1_000_000_000_000:.2f} T"
    elif abs(value) >= 1_000_000_000:
        return f"€{value / 1_000_000_000:.2f} B"
    elif abs(value) >= 1_000_000:
        return f"€{value / 1_000_000:.0f} M"
    return f"€{value:,.0f}"


def format_partner_name(code):
    """Format trading partner name with flag emoji.

    Args:
        code: Country ISO code.

    Returns:
        Formatted string with flag and country name.
    """
    nombre = PAISES_NOMBRE.get(code, code)
    bandera = BANDERAS.get(code, "")
    return f"{bandera} {nombre}"


def format_value_short(val):
    """Format currency value in abbreviated form for display.

    Args:
        val: Numeric value to format.

    Returns:
        Abbreviated string with € symbol and suffix.
    """
    if val >= 1e9:
        return f"€{val / 1e9:.1f}B"
    elif val >= 1e6:
        return f"€{val / 1e6:.0f}M"
    elif val >= 1e3:
        return f"€{val / 1e3:.0f}K"
    else:
        return f"€{val:.0f}"


def lighten_color(hex_color, factor=0.3):
    """Lighten a hex color by mixing with white.

    Args:
        hex_color: Hex color string (with or without #).
        factor: Lightening factor (0-1).

    Returns:
        Lightened hex color string.
    """
    hex_color = hex_color.lstrip("#")
    r, g, b = (
        int(hex_color[0:2], 16),
        int(hex_color[2:4], 16),
        int(hex_color[4:6], 16),
    )
    r = int(r + (255 - r) * factor)
    g = int(g + (255 - g) * factor)
    b = int(b + (255 - b) * factor)
    return f"#{r:02x}{g:02x}{b:02x}"


def darken_color(hex_color, factor=0.2):
    """Darken a hex color.

    Args:
        hex_color: Hex color string (with or without #).
        factor: Darkening factor (0-1).

    Returns:
        Darkened hex color string.
    """
    hex_color = hex_color.lstrip("#")
    r, g, b = (
        int(hex_color[0:2], 16),
        int(hex_color[2:4], 16),
        int(hex_color[4:6], 16),
    )
    r = int(r * (1 - factor))
    g = int(g * (1 - factor))
    b = int(b * (1 - factor))
    return f"#{r:02x}{g:02x}{b:02x}"
