"""
Eenmalig retroactief script: parse de 'Product dimensions cm' kolom uit het
Serax masterbestand en update lengte_cm / breedte_cm / hoogte_cm in seo_products.

De Serax dimensions staan in één gecombineerde string zoals:
    "L 13,8 W 13,8 H 7 "
    "L 8 W 5,8 H 7,7 "
    "L 12,5 W 8,2 H 9,4 "

Deze parser haalt L/W/H (case-insensitive) eruit, accepteert komma OF punt
als decimaalteken, en update de records in Supabase op SKU match.

Gebruik:
    python execution/fix_serax_dimensions.py
"""

import os
import re
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Forceer UTF-8 stdout op Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

SERAX_FILES = [
    "Master Files/Masterdata serax new items_2026_Interieur-Shop (2).xlsx",
    "Master Files/Masterdata Serax basic items_Interieur-Shop (1).xlsx",
]

# Pattern: vind 'L 13,8' / 'W 13.8' / 'H 7' (case insensitive)
DIM_PATTERN = re.compile(
    r"\b([LlWwHh])\s*([\d]+(?:[.,]\d+)?)",
    re.IGNORECASE,
)


def parse_dimensions(s: str) -> dict:
    """
    Parseert "L 13,8 W 13,8 H 7" → {"lengte_cm": 13.8, "breedte_cm": 13.8, "hoogte_cm": 7}
    Geeft alleen de gevonden velden terug. None of leeg → leeg dict.
    """
    if not s or not isinstance(s, str):
        return {}
    s = s.strip()
    if not s or s.lower() in {"nan", "n/a", "-"}:
        return {}

    result = {}
    for match in DIM_PATTERN.finditer(s):
        letter = match.group(1).upper()
        value = match.group(2).replace(",", ".")
        try:
            num = float(value)
        except ValueError:
            continue
        if letter == "L":
            result["lengte_cm"] = num
        elif letter == "W":
            result["breedte_cm"] = num
        elif letter == "H":
            result["hoogte_cm"] = num
    return result


def load_master(path: Path) -> list:
    """Lees één masterbestand en geef lijst van (sku, dim_dict) terug."""
    raw_head = pd.read_excel(path, header=None, nrows=3, dtype=str)
    header_rij = 1 if "Brand_id" in raw_head.iloc[1].values else 0
    df = pd.read_excel(path, header=header_rij, dtype=str)
    print(f"  {path.name}: {len(df)} rijen, header rij {header_rij}")

    if "Brand_id" not in df.columns:
        print(f"  FOUT: kolom 'Brand_id' niet gevonden in {path.name}", file=sys.stderr)
        return []
    if "Product dimensions cm" not in df.columns:
        print(f"  FOUT: kolom 'Product dimensions cm' niet gevonden in {path.name}", file=sys.stderr)
        return []

    result = []
    for _, row in df.iterrows():
        sku = str(row.get("Brand_id") or "").strip()
        if not sku or sku.lower() == "nan":
            continue
        raw = row.get("Product dimensions cm")
        if not raw or pd.isna(raw):
            continue
        dims = parse_dimensions(str(raw))
        if dims:
            result.append((sku, dims))
        else:
            print(f"  PARSE FAIL: {sku!r} → {raw!r}")
    return result


def main():
    updates_map: dict = {}  # sku -> dim_dict (later bestand wint bij duplicaat)

    print("Masterbestanden inlezen:")
    for fstr in SERAX_FILES:
        pad = Path(fstr)
        if not pad.exists():
            print(f"  OVERGESLAGEN (niet gevonden): {pad}", file=sys.stderr)
            continue
        entries = load_master(pad)
        for sku, dims in entries:
            updates_map[sku] = dims
        print(f"  → {len(entries)} SKUs met dimensies geladen")

    updates = list(updates_map.items())
    print(f"\nTotaal unieke SKUs te updaten: {len(updates)}")

    if not updates:
        print("Niets te updaten.")
        return

    # Toon eerste 5 als sanity check
    print(f"\nEerste 5 voorbeelden:")
    for sku, dims in updates[:5]:
        print(f"  {sku}: {dims}")

    # Update in Supabase per SKU
    from supabase import create_client
    sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

    print(f"\nUpdaten in seo_products...")
    updated = 0
    not_found = 0
    for i, (sku, dims) in enumerate(updates):
        res = sb.table("seo_products").update(dims).eq("sku", sku).execute()
        if res.data:
            updated += len(res.data)
        else:
            not_found += 1
        if (i + 1) % 50 == 0:
            print(f"  ... {i + 1}/{len(updates)}")

    print(f"\nKlaar:")
    print(f"  Geüpdatet: {updated} records")
    print(f"  Niet gevonden in DB: {not_found} (SKUs uit het Excel die niet in seo_products staan)")


if __name__ == "__main__":
    main()
