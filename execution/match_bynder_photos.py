"""
Match Bynder-foto's tegen Supabase seo_products + seo_shopify_index.

Leest de geparsede SKUs uit .tmp/bynder_fotos/share{N}_parsed.json en zoekt
voor elke SKU een match in Supabase. Rapporteert side-by-side: bestandsnaam
plus exacte SKU/titel/handle/barcode zoals in DB, of "GEEN MATCH".

Gebruik:
    python execution/match_bynder_photos.py --share 1
    python execution/match_bynder_photos.py --share 1 --output .tmp/bynder_fotos/share1_match.xlsx
"""

import argparse
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")


def get_supabase():
    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def fetch_products_by_skus(sb, skus: list[str]) -> dict[str, dict]:
    """Haal producten op uit seo_products op exacte sku match."""
    found: dict[str, dict] = {}
    # chunked in operator
    chunk = 100
    for i in range(0, len(skus), chunk):
        sub = skus[i:i+chunk]
        r = sb.table("seo_products").select(
            "id, sku, product_title_nl, fase, status, status_shopify, ean_shopify"
        ).in_("sku", sub).execute()
        for row in (r.data or []):
            found[row["sku"]] = row
    return found


def fetch_shopify_index(sb, skus: list[str]) -> dict[str, dict]:
    """Haal bestaande Shopify producten (via seo_shopify_index) op sku match."""
    found: dict[str, dict] = {}
    chunk = 100
    for i in range(0, len(skus), chunk):
        sub = skus[i:i+chunk]
        try:
            r = sb.table("seo_shopify_index").select("*").in_("sku", sub).execute()
            for row in (r.data or []):
                found[row["sku"]] = row
        except Exception as e:
            print(f"  waarschuwing seo_shopify_index: {e}")
            return found
    return found


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--share", required=True, help="Share-nummer (1 of 2)")
    ap.add_argument("--output", help="Optioneel pad voor xlsx export")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent.parent
    parsed_path = root / ".tmp" / "bynder_fotos" / f"share{args.share}_parsed.json"
    if not parsed_path.exists():
        print(f"FOUT: {parsed_path} bestaat niet"); sys.exit(1)

    with parsed_path.open() as f:
        data = json.load(f)

    by_sku = data["by_sku"]
    all_skus = sorted(by_sku.keys())
    print(f"Share {args.share}: {len(all_skus)} unieke SKUs, {sum(len(v) for v in by_sku.values())} foto's")

    sb = get_supabase()

    # Probeer zowel de exacte parsed SKU als een "base" versie (zonder suffix letter/kleur)
    print("\nOphalen uit seo_products...")
    prod_hits = fetch_products_by_skus(sb, all_skus)
    print(f"  Direct match: {len(prod_hits)} / {len(all_skus)}")

    # Voor SKUs zonder match: probeer base-SKU (zonder trailing letter of -YYY)
    import re
    misses = [s for s in all_skus if s not in prod_hits]
    alt_map: dict[str, list[str]] = {}
    if misses:
        variants: set[str] = set()
        for s in misses:
            # strip single trailing uppercase letter (e.g. B1318002M -> B1318002)
            v1 = re.sub(r"([A-Z])$", "", s)
            if v1 != s:
                variants.add(v1)
            # strip -YYY color suffix
            v2 = re.sub(r"-\d{3}$", "", s)
            if v2 != s:
                variants.add(v2)
        variants = list(variants)
        print(f"\nProberen alternatieve vorm voor {len(misses)} missers ({len(variants)} varianten)...")
        alt_hits = fetch_products_by_skus(sb, variants)
        print(f"  Alt-match: {len(alt_hits)}")
        for s in misses:
            v1 = re.sub(r"([A-Z])$", "", s)
            v2 = re.sub(r"-\d{3}$", "", s)
            for v in (v1, v2):
                if v != s and v in alt_hits:
                    alt_map.setdefault(s, []).append(v)

    # seo_shopify_index voor LIVE match
    print("\nOphalen uit seo_shopify_index...")
    live_hits = fetch_shopify_index(sb, all_skus)
    print(f"  Live (Shopify) match: {len(live_hits)} / {len(all_skus)}")

    # Rapport
    print("\n" + "="*100)
    print("SIDE-BY-SIDE: BYNDER SKU  <->  SUPABASE MATCH")
    print("="*100)
    rows = []
    for sku in all_skus:
        photos = by_sku[sku]
        p = prod_hits.get(sku)
        l = live_hits.get(sku)
        alt = alt_map.get(sku, [])
        status = ""
        if p:
            status = f"seo_products HIT | fase={p.get('fase')} status={p.get('status')} status_shopify={p.get('status_shopify')}"
            title = p.get("product_title_nl") or ""
        elif l:
            status = f"shopify_index HIT | handle={l.get('handle')}"
            title = l.get("title") or ""
        elif alt:
            status = f"ALT MATCH via {alt[0]}"
            title = ""
        else:
            status = "GEEN MATCH"
            title = ""
        print(f"  {sku:20s} | {len(photos):2d} foto's | {status}")
        if title:
            print(f"                        title: {title[:90]}")
        rows.append({
            "bynder_sku": sku,
            "aantal_fotos": len(photos),
            "in_seo_products": bool(p),
            "fase": p.get("fase") if p else "",
            "status": p.get("status") if p else "",
            "status_shopify": p.get("status_shopify") if p else "",
            "in_shopify_index": bool(l),
            "handle": l.get("handle") if l else "",
            "product_title": (p or l or {}).get("product_title_nl") or (l or {}).get("title") or "",
            "alt_match": ", ".join(alt),
            "fotobestanden": " | ".join(photos),
        })

    # Totaal
    matched_products = sum(1 for r in rows if r["in_seo_products"])
    matched_shopify = sum(1 for r in rows if r["in_shopify_index"])
    matched_alt = sum(1 for r in rows if r["alt_match"])
    no_match = sum(1 for r in rows if not r["in_seo_products"] and not r["in_shopify_index"] and not r["alt_match"])
    print("\n" + "="*100)
    print(f"SAMENVATTING")
    print(f"  In seo_products (direct):    {matched_products}")
    print(f"  In seo_shopify_index (live): {matched_shopify}")
    print(f"  Alleen via alternatieve SKU: {matched_alt}")
    print(f"  Geen match ergens:           {no_match}")
    print("="*100)

    if args.output:
        import openpyxl
        wb = openpyxl.Workbook(); ws = wb.active
        ws.append(list(rows[0].keys()))
        for r in rows:
            ws.append([r[k] for k in rows[0].keys()])
        wb.save(args.output)
        print(f"\nExcel: {args.output}")


if __name__ == "__main__":
    main()
