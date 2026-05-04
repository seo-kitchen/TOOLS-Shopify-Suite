"""
Stap 2: Match producten tegen de Shopify-index in Supabase.
100% zeker = automatisch. Twijfel = via callback (CLI of web-UI) of deferred.

Gebruik:
    python execution/match.py --fase 3
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from typing import Callable, Optional

from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")


class MatchError(Exception):
    """Raised when matching cannot proceed (bijv. lege Shopify-index)."""


@dataclass
class MatchResult:
    matched_count: int = 0
    new_count: int = 0
    archief_count: int = 0
    actief_count: int = 0
    skipped_count: int = 0
    deferred: list[dict] = field(default_factory=list)
    # deferred entries: {product_id, excel_row, shopify_hit, reason}


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


def _cli_prompt(excel_row: dict, shopify_hit: dict) -> str:
    """Originele CLI-UX: vraag de gebruiker via input() wat te doen met een twijfelgeval."""
    sku   = excel_row.get("sku", "?")
    titel = excel_row.get("product_name_raw", "")
    print(f"  SKU: {sku} | {titel}")
    print(f"     {shopify_hit.get('twijfel_reden')}")
    print(f"     Huidige suggestie: {shopify_hit.get('status_shopify')}")
    print(f"     Opties: actief / archief / nieuw / skip")

    default = shopify_hit.get("status_shopify", "nieuw")
    while True:
        keuze = input(f"     Jouw keuze [{default}]: ").strip().lower()
        if not keuze:
            keuze = default
        # Ondersteun Nederlandse én Engelse terminologie
        if keuze in ("actief", "archief", "nieuw"):
            return keuze
        if keuze in ("skip", "overslaan"):
            return "skip"
        print("     Ongeldige keuze. Typ: actief, archief, nieuw of skip")


def match_fase(
    fase: str,
    ids: list[int] | None = None,
    on_conflict: Optional[Callable[[dict, dict], str]] = None,
    progress: Optional[Callable[[int, int, str], None]] = None,
    logger: Optional[Callable[[str], None]] = None,
) -> MatchResult:
    """
    Match producten in `fase` (of alleen `ids`) tegen seo_shopify_index.

    Args:
        fase: fasecode, bijv. "3"
        ids: optioneel — beperk tot deze product-IDs
        on_conflict: callback(excel_row, shopify_hit) -> "actief" | "archief" |
            "nieuw" | "skip" | "__defer__". Standaard = defer naar result.deferred.
        progress: callback(current, total, sku)
        logger: callback(message)

    Returns:
        MatchResult
    """
    log = logger or (lambda m: print(m))
    result_obj = MatchResult()

    sb = get_supabase()

    # Controleer of er website-structuur geladen is
    index_count = sb.table("seo_shopify_index").select("id", count="exact").execute()
    if not index_count.count:
        raise MatchError("Shopify-index is leeg. Draai eerst load_website_structure.py.")

    q = sb.table("seo_products").select("*").eq("status", "raw").eq("fase", fase)
    if ids:
        q = q.in_("id", ids)
    products = q.execute().data

    if not products:
        log(f"Geen producten met status='raw' gevonden voor fase {fase}.")
        return result_obj

    log(f"Matching: {len(products)} producten (fase {fase})")

    sku_index, ean_index = build_index(sb)

    # Default handler: defer
    if on_conflict is None:
        on_conflict = lambda excel_row, shopify_hit: "__defer__"

    total = len(products)
    for i, product in enumerate(products, 1):
        if progress:
            progress(i, total, product.get("sku", ""))

        match = match_product(product, sku_index, ean_index)

        if match["match_zekerheid"] == "100%":
            apply_match(sb, product["id"], match)
            status = match["status_shopify"]
            if status == "actief":
                result_obj.actief_count += 1
            elif status == "archief":
                result_obj.archief_count += 1
            else:
                result_obj.new_count += 1
            result_obj.matched_count += 1
            continue

        # Twijfelgeval — vraag callback
        keuze = on_conflict(product, match)

        if keuze == "__defer__":
            result_obj.deferred.append({
                "product_id":   product["id"],
                "excel_row":    product,
                "shopify_hit":  match,
                "reason":       match.get("twijfel_reden"),
            })
            continue

        if keuze == "skip":
            sb.table("seo_products").update({
                "review_reden": f"[OVERGESLAGEN] {match['twijfel_reden']}"
            }).eq("id", product["id"]).execute()
            result_obj.skipped_count += 1
            continue

        if keuze in ("actief", "archief", "nieuw"):
            sb.table("seo_products").update({
                "status_shopify":  keuze,
                "match_zekerheid": "100%",
                "review_reden":    f"[HANDMATIG] {match['twijfel_reden']}",
            }).eq("id", product["id"]).execute()
            if keuze == "actief":
                result_obj.actief_count += 1
            elif keuze == "archief":
                result_obj.archief_count += 1
            else:
                result_obj.new_count += 1
            result_obj.matched_count += 1
            continue

        # Onbekende keuze -> defer
        result_obj.deferred.append({
            "product_id":   product["id"],
            "excel_row":    product,
            "shopify_hit":  match,
            "reason":       f"Onbekende keuze van on_conflict: {keuze!r}",
        })

    log(
        f"\nMatch-resultaten fase {fase}:\n"
        f"  Gematcht:  {result_obj.matched_count}\n"
        f"    actief:  {result_obj.actief_count}\n"
        f"    archief: {result_obj.archief_count}\n"
        f"    nieuw:   {result_obj.new_count}\n"
        f"  Skipped:   {result_obj.skipped_count}\n"
        f"  Deferred:  {len(result_obj.deferred)}\n"
    )

    return result_obj


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fase", required=True, help="Fasecode, bijv. 3")
    args = parser.parse_args()

    try:
        match_fase(args.fase, on_conflict=_cli_prompt)
    except MatchError as e:
        print(f"FOUT: {e}", file=sys.stderr)
        sys.exit(1)
