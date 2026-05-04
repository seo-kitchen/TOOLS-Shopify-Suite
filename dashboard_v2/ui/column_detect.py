"""Column-alias sets + detector for loosely-named Excel uploads.

Ported verbatim from the old streamlit_app.py so existing supplier Excel
layouts keep resolving.
"""
from __future__ import annotations

import re

SKU_ALIASES = {
    "sku", "variant sku", "brand_id", "artikel", "artikelnummer", "product id",
    "item number", "code", "articlecode", "article code", "artikelcode",
}
EAN_ALIASES = {
    "ean", "ean piece", "ean code piece", "ean stuk", "barcode piece", "ean los",
    "ean code", "barcode", "ean packaging/giftbox", "ean packaging", "ean giftbox",
    "ean box", "ean-ucc _code", "ean-ucc code", "ean code per stuk",
}
NAAM_ALIASES = {
    "product name", "name", "title", "omschrijving", "naam", "description en",
    "short product name piece (english)", "short product name piece (dutch)",
    "description", "productnaam",
}
PRIJS_ALIASES = {
    "prijs", "price", "verkoopprijs", "retail price", "rrp", "consumer price",
    "list price", "sale price", "prix", "vkp",
}
KLEUR_ALIASES = {
    "color", "colour", "kleur", "farbe", "couleur", "color en", "kleur en",
}
MATERIAAL_ALIASES = {
    "material", "materiaal", "matiere", "werkstoff", "material en", "materiaal en",
}


def _norm(s) -> str:
    return re.sub(r"\s+", " ", str(s)).strip().lower()


def detect_column(columns: list, aliases: set) -> str | None:
    """Return the first column whose normalised name is in the alias set."""
    for col in columns:
        if _norm(col) in aliases:
            return col
    return None
