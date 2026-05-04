"""
Stap 2: Match producten tegen de Shopify-index in Supabase.
100% zeker = automatisch. Twijfel = stoppen en vragen.

Gebruik:
    python execution/match.py --fase 3
"""

import argparse
import os
import sys
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")


def get_supabase():
    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def build_index(sb) -> tuple[dict, dict]:
    """
    Bouw SKU- en EAN-index vanuit seo_shopify_index.
    Geeft: ({sku -> record}, {ean -> record})
    """
    result = sb.table("seo_shopify_index").select("*").execute()
    sku_index = {}
    ean_index = {}
    for row in result.data:
        sku = (row.get("sku") or "").strip()
        ean = (row.get("ean") or "").strip()
        if sku:
            sku_index[sku] = row
        if ean:
            ean_index[ean] = row
    return sku_index, ean_index


def match_product(product: dict, sku_index: dict, ean_index: dict) -> dict:
    """
    Matchlogica per SOP:
    - Exacte SKU-match -> zeker
    - Exacte EAN-match zonder SKU-match -> twijfel (voorleggen)
    - Geen match -> nieuw (zeker)

    Geeft dict met keys: status_shopify, match_methode, match_zekerheid,
                         shopify_product_id, shopify_variant_id, twijfel_reden
    """
    sku = (product.get("sku") or "").strip()
    ean = (product.get("ean_shopify") or "").strip()

    result = {
        "status_shopify":     "nieuw",
        "match_methode":      "geen",
        "match_zekerheid":    "100%",
        "shopify_product_id": None,
        "shopify_variant_id": None,
        "twijfel_reden":      None,
    }

    sku_match = sku_index.get(sku)
    ean_match = ean_index.get(ean)

    # Geval 1: SKU-match
    if sku_match:
        result["status_shopify"]     = sku_match["status_shopify"]
        result["match_methode"]      = "sku"
        result["match_zekerheid"]    = "100%"
        result["shopify_product_id"] = sku_match.get("shopify_product_id")
        result["shopify_variant_id"] = sku_match.get("shopify_variant_id")

        # Extra check: is de EAN consistent?
        if ean and ean_match and ean_match.get("sku") != sku:
            result["match_zekerheid"] = "twijfel"
            result["twijfel_reden"]   = (
                f"SKU-match gevonden ({sku_match['status_shopify']}), maar EAN {ean} "
                f"behoort ook toe aan andere SKU: {ean_match.get('sku')} — data-conflict?"
            )
        return result

    # Geval 2: alleen EAN-match (geen SKU-match) -> twijfel
    if ean_match:
        result["status_shopify"]     = ean_match["status_shopify"]
        result["match_methode"]      = "ean"
        result["match_zekerheid"]    = "twijfel"
        result["shopify_product_id"] = ean_match.get("shopify_product_id")
        result["shopify_variant_id"] = ean_match.get("shopify_variant_id")
        result["twijfel_reden"]      = (
            f"EAN-match gevonden bij andere SKU: {ean_match.get('sku')} "
            f"({ean_match.get('product_title', '')}) — hernoemd product?"
        )
        return result

    # Geval 3: geen match -> nieuw (100% zeker)
    return result


def apply_match(sb, product_id: str, match: dict):
    sb.table("seo_products").update({
        "status_shopify":     match["status_shopify"],
        "match_methode":      match["match_methode"],
        "match_zekerheid":    match["match_zekerheid"],
        "shopify_product_id": match["shopify_product_id"],
        "shopify_variant_id": match["shopify_variant_id"],
        "review_reden":       match.get("twijfel_reden"),
    }).eq("id", product_id).execute()


def match_fase(fase: str):
    sb = get_supabase()

    # Controleer of er website-structuur geladen is
    index_count = sb.table("seo_shopify_index").select("id", count="exact").execute()
    if not index_count.count:
        print("FOUT: Shopify-index is leeg. Draai eerst load_website_structure.py.", file=sys.stderr)
        sys.exit(1)

    result = sb.table("seo_products").select("*").eq("status", "raw").eq("fase", fase).execute()
    products = result.data

    if not products:
        print(f"Geen producten met status='raw' gevonden voor fase {fase}.")
        return

    print(f"Matching: {len(products)} producten (fase {fase})")

    sku_index, ean_index = build_index(sb)

    zeker = {"actief": [], "archief": [], "nieuw": []}
    twijfel = []

    for product in products:
        match = match_product(product, sku_index, ean_index)
        apply_match(sb, product["id"], match)

        if match["match_zekerheid"] == "100%":
            zeker[match["status_shopify"]].append(product)
        else:
            twijfel.append((product, match))

    # Rapporteer zekere matches
    print(f"""
Match-resultaten fase {fase}:
  Automatisch gematcht ({sum(len(v) for v in zeker.values())} producten, 100% zeker):
    - {len(zeker['actief'])} actief op webshop
    - {len(zeker['archief'])} in archief
    - {len(zeker['nieuw'])} nieuw
""")

    if not twijfel:
        print("  Geen twijfelgevallen. Klaar voor stap 3 (transform.py).")
        return

    # Twijfelgevallen interactief voorleggen
    print(f"  {len(twijfel)} twijfelgeval(len) — jouw beslissing vereist:\n")
    for i, (product, match) in enumerate(twijfel, 1):
        sku   = product.get("sku", "?")
        titel = product.get("product_name_raw", "")
        print(f"  {i}. SKU: {sku} | {titel}")
        print(f"     {match['twijfel_reden']}")
        print(f"     Huidige suggestie: {match['status_shopify']}")
        print(f"     Opties: actief / archief / nieuw / overslaan")

        while True:
            keuze = input(f"     Jouw keuze [{match['status_shopify']}]: ").strip().lower()
            if not keuze:
                keuze = match["status_shopify"]
            if keuze in ("actief", "archief", "nieuw", "overslaan"):
                break
            print("     Ongeldige keuze. Typ: actief, archief, nieuw of overslaan")

        if keuze == "overslaan":
            print(f"     -> Overgeslagen (blijft status='raw')")
            sb.table("seo_products").update({
                "review_reden": f"[OVERGESLAGEN] {match['twijfel_reden']}"
            }).eq("id", product["id"]).execute()
        else:
            sb.table("seo_products").update({
                "status_shopify":  keuze,
                "match_zekerheid": "100%",
                "review_reden":    f"[HANDMATIG] {match['twijfel_reden']}",
            }).eq("id", product["id"]).execute()
            print(f"     -> Opgeslagen als: {keuze}")
        print()

    print("Matching voltooid. Controleer de resultaten en ga door naar transform.py.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fase", required=True, help="Fasecode, bijv. 3")
    args = parser.parse_args()

    match_fase(args.fase)
