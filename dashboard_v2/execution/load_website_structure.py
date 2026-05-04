"""
Eenmalige setup: laad de Shopify-websitestructuur in Supabase.
Veilig om opnieuw te draaien (upsert).

Gebruik:
    python execution/load_website_structure.py \
        --webshop active_products.csv \
        --archive archive_products.csv

Bevat ook een pure-Python entrypoint `load_website_structure(...)` voor
gebruik vanuit de Streamlit dashboard.
"""

import argparse
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Metafield-kolomnamen zoals Shopify ze exporteert
KLEUR_COLS    = ["Metafield: custom.kleur [single_line_text_field]", "kleur"]
MATERIAAL_COLS = ["Metafield: custom.materiaal [single_line_text_field]", "materiaal"]
DESIGNER_COLS  = ["Metafield: custom.designer [single_line_text_field]", "designer"]


# ---------------------------------------------------------------------------
# Result types & exceptions
# ---------------------------------------------------------------------------


class LoadError(Exception):
    """Raised when website-structure load cannot proceed."""


@dataclass
class LoadResult:
    products_loaded: int = 0
    archive_loaded: int = 0
    collections_loaded: int = 0
    filters_loaded: int = 0
    filter_counts: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def get_supabase():
    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def find_col(df: pd.DataFrame, candidates: list) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def extract_cat_tags(tags_str: str) -> list[str]:
    """Haal cat_* waarden op uit tags-string."""
    if not tags_str or pd.isna(tags_str):
        return []
    return [t.strip() for t in str(tags_str).split(",") if t.strip().startswith("cat_")]


def load_csv(path: str, status: str, sb) -> int:
    """Laad webshop of archief CSV in seo_shopify_index. Geeft aantal geladen rijen terug."""
    df = pd.read_csv(path, dtype=str, low_memory=False)

    col_sku       = next((c for c in ["Variant SKU", "variant_sku"] if c in df.columns), None)
    col_ean       = next((c for c in ["Variant Barcode", "variant_barcode"] if c in df.columns), None)
    col_prod_id   = next((c for c in ["ID", "id"] if c in df.columns), None)
    col_variant_id = next((c for c in ["Variant ID", "variant_id"] if c in df.columns), None)
    col_title     = next((c for c in ["Title", "title"] if c in df.columns), None)

    if not col_sku:
        print(f"  FOUT: 'Variant SKU' kolom niet gevonden in {path}", file=sys.stderr)
        return 0

    rows = []
    for _, row in df.iterrows():
        sku = str(row.get(col_sku, "") or "").strip()
        if not sku or sku == "nan":
            continue

        rows.append({
            "sku":                str(row.get(col_sku, "") or "").strip(),
            "ean":                str(row.get(col_ean, "") or "").strip() or None,
            "shopify_product_id": str(row.get(col_prod_id, "") or "").strip() or None,
            "shopify_variant_id": str(row.get(col_variant_id, "") or "").strip() or None,
            "status_shopify":     status,
            "product_title":      str(row.get(col_title, "") or "").strip() or None,
        })

    seen = set()
    unique_rows = []
    for r in rows:
        if r["sku"] not in seen:
            seen.add(r["sku"])
            unique_rows.append(r)

    if not unique_rows:
        return 0

    for i in range(0, len(unique_rows), 100):
        batch = unique_rows[i:i + 100]
        sb.table("seo_shopify_index").upsert(batch, on_conflict="sku").execute()

    return len(unique_rows)


def load_collections(df: pd.DataFrame, sb) -> int:
    """Extraheer unieke Product Types als collecties."""
    col_type = next((c for c in ["Product Type", "Type"] if c in df.columns), None)
    if not col_type:
        return 0

    typen = {str(v).strip() for v in df[col_type].dropna() if str(v).strip()}
    rows = [{"naam": t, "type": "producttype", "actief": True} for t in typen if t]

    for r in rows:
        sb.table("seo_website_collections").upsert(r, on_conflict="naam").execute()
    return len(rows)


def load_filter_values(df: pd.DataFrame, sb) -> dict:
    """Extraheer unieke filter-waarden voor kleur, materiaal en designer."""
    counts: dict = {}
    for ftype, candidates in [
        ("kleur",    KLEUR_COLS),
        ("materiaal", MATERIAAL_COLS),
        ("designer",  DESIGNER_COLS),
    ]:
        col = find_col(df, candidates)
        if not col:
            continue
        waarden = {str(v).strip() for v in df[col].dropna()
                   if str(v).strip() and str(v).strip() != "nan"}
        rows = [{"type": ftype, "waarde": w} for w in waarden]
        for r in rows:
            sb.table("seo_filter_values").upsert(r, on_conflict="type,waarde").execute()
        counts[ftype] = len(rows)

    return counts


# ---------------------------------------------------------------------------
# Pure-function entrypoint
# ---------------------------------------------------------------------------


def load_website_structure(
    active_csv: str,
    archive_csv: str | None = None,
    progress: Callable[[int, int, str], None] | None = None,
    logger: Callable[[str], None] | None = None,
) -> LoadResult:
    """Laad Shopify website-structuur (producten, collecties, filterwaarden)."""
    log = logger or (lambda _msg: None)

    def _progress(step: int, total: int, msg: str) -> None:
        if progress:
            progress(step, total, msg)

    if not Path(active_csv).exists():
        raise LoadError(f"Webshop CSV niet gevonden: {active_csv}")
    if archive_csv and not Path(archive_csv).exists():
        raise LoadError(f"Archief CSV niet gevonden: {archive_csv}")

    try:
        sb = get_supabase()
    except Exception as e:
        raise LoadError(f"Supabase verbinden mislukt: {e}") from e

    TOTAL_STEPS = 4
    result = LoadResult()

    _progress(1, TOTAL_STEPS, "Actieve producten laden")
    log("Shopify-index: actieve producten...")
    try:
        result.products_loaded = load_csv(active_csv, "actief", sb)
    except Exception as e:
        raise LoadError(f"Actieve CSV laden mislukt: {e}") from e
    log(f"  {result.products_loaded} actieve producten geladen")

    _progress(2, TOTAL_STEPS, "Archief laden")
    if archive_csv:
        try:
            result.archive_loaded = load_csv(archive_csv, "archief", sb)
        except Exception as e:
            raise LoadError(f"Archief CSV laden mislukt: {e}") from e
        log(f"  {result.archive_loaded} archiefproducten geladen")

    _progress(3, TOTAL_STEPS, "Collecties laden")
    try:
        df = pd.read_csv(active_csv, dtype=str, low_memory=False)
        result.collections_loaded = load_collections(df, sb)
    except Exception as e:
        raise LoadError(f"Collecties laden mislukt: {e}") from e
    log(f"  {result.collections_loaded} collecties geladen")

    _progress(4, TOTAL_STEPS, "Filterwaarden laden")
    try:
        result.filter_counts = load_filter_values(df, sb)
    except Exception as e:
        raise LoadError(f"Filterwaarden laden mislukt: {e}") from e
    result.filters_loaded = sum(result.filter_counts.values())
    for ftype, count in result.filter_counts.items():
        log(f"  Filterwaarden ({ftype}): {count}")

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--webshop", required=True, help="Pad naar actieve webshop export CSV")
    parser.add_argument("--archive", help="Pad naar archief export CSV (optioneel)")
    args = parser.parse_args()

    try:
        result = load_website_structure(
            active_csv=args.webshop,
            archive_csv=args.archive,
            logger=lambda m: print(m),
        )
    except LoadError as e:
        print(f"FOUT: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"""
Website-structuur geladen:
  {result.products_loaded} actieve producten in Shopify-index
  {result.archive_loaded} archiefproducten in Shopify-index
  {result.collections_loaded} collecties / producttypes
  {result.filters_loaded} filterwaarden totaal

Is dit de volledige huidige website-structuur? Controleer de aantallen hierboven.
""")
