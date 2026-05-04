"""
Match Bynder-foto's tegen Supabase seo_products + seo_shopify_index.

Leest de geparsede SKUs uit .tmp/bynder_fotos/share{N}_parsed.json en zoekt
voor elke SKU een match in Supabase. Rapporteert side-by-side: bestandsnaam
plus exacte SKU/titel/handle/barcode zoals in DB, of "GEEN MATCH".

Gebruik:
    python execution/match_bynder_photos.py --share 1
    python execution/match_bynder_photos.py --share 1 --output .tmp/bynder_fotos/share1_match.xlsx

Bevat ook een pure-Python entrypoint `match_bynder(...)` voor gebruik
vanuit de Streamlit dashboard.
"""

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")


# ---------------------------------------------------------------------------
# Result types & exceptions
# ---------------------------------------------------------------------------


class BynderError(Exception):
    """Raised when the bynder match pipeline cannot proceed."""


@dataclass
class BynderResult:
    matched_count: int = 0
    unmatched_count: int = 0
    xlsx_path: Optional[str] = None
    rows: list[dict] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def get_supabase():
    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def fetch_products_by_skus(sb, skus: list[str]) -> dict[str, dict]:
    """Haal producten op uit seo_products op exacte sku match."""
    found: dict[str, dict] = {}
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


# ---------------------------------------------------------------------------
# Pure-function entrypoint
# ---------------------------------------------------------------------------


def match_bynder(
    share: int,
    output_path: str | None = None,
    logger: Callable[[str], None] | None = None,
) -> BynderResult:
    """Match Bynder foto's tegen Supabase."""
    log = logger or (lambda _msg: None)

    root = Path(__file__).resolve().parent.parent
    parsed_path = root / ".tmp" / "bynder_fotos" / f"share{share}_parsed.json"
    if not parsed_path.exists():
        raise BynderError(f"Parsed JSON niet gevonden: {parsed_path}")

    try:
        with parsed_path.open() as f:
            data = json.load(f)
    except Exception as e:
        raise BynderError(f"Kan parsed JSON niet lezen: {e}") from e

    by_sku = data["by_sku"]
    all_skus = sorted(by_sku.keys())
    log(f"Share {share}: {len(all_skus)} unieke SKUs, "
        f"{sum(len(v) for v in by_sku.values())} foto's")

    try:
        sb = get_supabase()
    except Exception as e:
        raise BynderError(f"Supabase verbinden mislukt: {e}") from e

    log("Ophalen uit seo_products...")
    prod_hits = fetch_products_by_skus(sb, all_skus)
    log(f"  Direct match: {len(prod_hits)} / {len(all_skus)}")

    misses = [s for s in all_skus if s not in prod_hits]
    alt_map: dict[str, list[str]] = {}
    if misses:
        variants: set[str] = set()
        for s in misses:
            v1 = re.sub(r"([A-Z])$", "", s)
            if v1 != s:
                variants.add(v1)
            v2 = re.sub(r"-\d{3}$", "", s)
            if v2 != s:
                variants.add(v2)
        variants_list = list(variants)
        log(f"Proberen alternatieve vorm voor {len(misses)} missers "
            f"({len(variants_list)} varianten)...")
        alt_hits = fetch_products_by_skus(sb, variants_list)
        log(f"  Alt-match: {len(alt_hits)}")
        for s in misses:
            v1 = re.sub(r"([A-Z])$", "", s)
            v2 = re.sub(r"-\d{3}$", "", s)
            for v in (v1, v2):
                if v != s and v in alt_hits:
                    alt_map.setdefault(s, []).append(v)

    log("Ophalen uit seo_shopify_index...")
    live_hits = fetch_shopify_index(sb, all_skus)
    log(f"  Live (Shopify) match: {len(live_hits)} / {len(all_skus)}")

    rows: list[dict] = []
    for sku in all_skus:
        photos = by_sku[sku]
        p = prod_hits.get(sku)
        l = live_hits.get(sku)
        alt = alt_map.get(sku, [])
        if p:
            title = p.get("product_title_nl") or ""
        elif l:
            title = l.get("title") or ""
        else:
            title = ""
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

    matched_products = sum(1 for r in rows if r["in_seo_products"])
    matched_shopify = sum(1 for r in rows if r["in_shopify_index"])
    matched_alt = sum(1 for r in rows if r["alt_match"])
    no_match = sum(
        1 for r in rows
        if not r["in_seo_products"] and not r["in_shopify_index"] and not r["alt_match"]
    )
    matched_total = sum(
        1 for r in rows
        if r["in_seo_products"] or r["in_shopify_index"] or r["alt_match"]
    )

    log(f"Matched (seo_products/shopify/alt): {matched_total}")
    log(f"Geen match: {no_match}")

    xlsx_path: Optional[str] = None
    if output_path and rows:
        try:
            import openpyxl
            wb = openpyxl.Workbook()
            ws = wb.active
            ws.append(list(rows[0].keys()))
            for r in rows:
                ws.append([r[k] for k in rows[0].keys()])
            wb.save(output_path)
            xlsx_path = output_path
            log(f"Excel geschreven: {output_path}")
        except Exception as e:
            raise BynderError(f"Excel export mislukt: {e}") from e

    return BynderResult(
        matched_count=matched_total,
        unmatched_count=no_match,
        xlsx_path=xlsx_path,
        rows=rows,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--share", required=True, help="Share-nummer (1 of 2)")
    ap.add_argument("--output", help="Optioneel pad voor xlsx export")
    args = ap.parse_args()

    try:
        share_num = int(args.share)
    except ValueError:
        print(f"FOUT: --share moet een nummer zijn, kreeg {args.share!r}", file=sys.stderr)
        sys.exit(1)

    try:
        result = match_bynder(
            share=share_num,
            output_path=args.output,
            logger=lambda m: print(m),
        )
    except BynderError as e:
        print(f"FOUT: {e}", file=sys.stderr)
        sys.exit(1)

    print("\n" + "=" * 100)
    print("SIDE-BY-SIDE: BYNDER SKU  <->  SUPABASE MATCH")
    print("=" * 100)
    for r in result.rows:
        status_bits = []
        if r["in_seo_products"]:
            status_bits.append(
                f"seo_products HIT | fase={r['fase']} status={r['status']} "
                f"status_shopify={r['status_shopify']}"
            )
        elif r["in_shopify_index"]:
            status_bits.append(f"shopify_index HIT | handle={r['handle']}")
        elif r["alt_match"]:
            status_bits.append(f"ALT MATCH via {r['alt_match']}")
        else:
            status_bits.append("GEEN MATCH")
        print(f"  {r['bynder_sku']:20s} | {r['aantal_fotos']:2d} foto's | {' '.join(status_bits)}")
        if r["product_title"]:
            print(f"                        title: {r['product_title'][:90]}")

    print("\n" + "=" * 100)
    print("SAMENVATTING")
    print(f"  Matched totaal:              {result.matched_count}")
    print(f"  Geen match:                  {result.unmatched_count}")
    print("=" * 100)
    if result.xlsx_path:
        print(f"\nExcel: {result.xlsx_path}")


if __name__ == "__main__":
    main()
