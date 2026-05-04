"""
Eenmalige setup: laad de Shopify-websitestructuur in Supabase.
Veilig om opnieuw te draaien (upsert).

Gebruik:
    python execution/load_website_structure.py \
        --webshop active_products.csv \
        --archive archive_products.csv
"""

import argparse
import os
import re
import sys
from dotenv import load_dotenv

import pandas as pd

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Metafield-kolomnamen zoals Shopify ze exporteert
KLEUR_COLS    = ["Metafield: custom.kleur [single_line_text_field]", "kleur"]
MATERIAAL_COLS = ["Metafield: custom.materiaal [single_line_text_field]", "materiaal"]
DESIGNER_COLS  = ["Metafield: custom.designer [single_line_text_field]", "designer"]


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

    # Kolom-mapping — Shopify export gebruikt deze namen
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

    # Dedupliceer op SKU (neem eerste)
    seen = set()
    unique_rows = []
    for r in rows:
        if r["sku"] not in seen:
            seen.add(r["sku"])
            unique_rows.append(r)

    if not unique_rows:
        return 0

    # Upsert op SKU — actief overschrijft archief bij conflict
    for i in range(0, len(unique_rows), 100):
        batch = unique_rows[i:i + 100]
        sb.table("seo_shopify_index").upsert(batch, on_conflict="sku").execute()

    return len(unique_rows)


def load_collections(df: pd.DataFrame, sb):
    """Extraheer unieke Product Types als collecties."""
    col_type = next((c for c in ["Product Type", "Type"] if c in df.columns), None)
    if not col_type:
        return 0

    typen = {str(v).strip() for v in df[col_type].dropna() if str(v).strip()}
    rows = [{"naam": t, "type": "producttype", "actief": True} for t in typen if t]

    for r in rows:
        sb.table("seo_website_collections").upsert(r, on_conflict="naam").execute()
    return len(rows)


def load_filter_values(df: pd.DataFrame, sb):
    """Extraheer unieke filter-waarden voor kleur, materiaal en designer."""
    counts = {}
    for ftype, candidates in [
        ("kleur",    KLEUR_COLS),
        ("materiaal", MATERIAAL_COLS),
        ("designer",  DESIGNER_COLS),
    ]:
        col = find_col(df, candidates)
        if not col:
            continue
        waarden = {str(v).strip() for v in df[col].dropna() if str(v).strip() and str(v).strip() != "nan"}
        rows = [{"type": ftype, "waarde": w} for w in waarden]
        for r in rows:
            sb.table("seo_filter_values").upsert(r, on_conflict="type,waarde").execute()
        counts[ftype] = len(rows)

    return counts


def load_website_structure(webshop_path: str, archive_path: str | None):
    sb = get_supabase()

    print("Website-structuur laden...\n")

    # 1. Shopify index
    n_actief = load_csv(webshop_path, "actief", sb)
    print(f"  Shopify-index: {n_actief} actieve producten geladen")

    n_archief = 0
    if archive_path:
        n_archief = load_csv(archive_path, "archief", sb)
        print(f"  Shopify-index: {n_archief} archiefproducten geladen")

    # 2. Collecties + filterwaarden uit webshop CSV
    df = pd.read_csv(webshop_path, dtype=str, low_memory=False)

    n_coll = load_collections(df, sb)
    print(f"  Collecties: {n_coll} producttypes geladen")

    filter_counts = load_filter_values(df, sb)
    for ftype, count in filter_counts.items():
        print(f"  Filterwaarden ({ftype}): {count} geladen")

    print(f"""
Website-structuur geladen:
  {n_actief} actieve producten in Shopify-index
  {n_archief} archiefproducten in Shopify-index
  {n_coll} collecties / producttypes
  {sum(filter_counts.values())} filterwaarden totaal

Is dit de volledige huidige website-structuur? Controleer de aantallen hierboven.
""")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--webshop", required=True, help="Pad naar actieve webshop export CSV")
    parser.add_argument("--archive", help="Pad naar archief export CSV (optioneel)")
    args = parser.parse_args()

    load_website_structure(args.webshop, args.archive)
