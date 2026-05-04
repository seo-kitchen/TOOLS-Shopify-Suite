"""Shopify → Supabase: synct foto-status van alle actieve producten.

Per product wordt opgeslagen:
  - has_image          : bool — heeft product minstens 1 foto?
  - first_image_src    : URL van de eerste foto (laagste position)
  - first_image_alt    : alt-tekst van de eerste foto
  - image_count        : aantal foto's
  - image_alt_status   : 'ok' | 'missing'
  - image_name_status  : 'seofriendly' | 'supplier' | 'unknown'
                         supplier = leveranciersnaam-patroon (geen koppeltekens,
                         all-caps, korte naam, etc.)

Gebruik:
    python execution/shopify_photo_sync.py
    python execution/shopify_photo_sync.py --dry-run
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

STORE   = os.getenv("SHOPIFY_STORE", "")
TOKEN   = os.getenv("SHOPIFY_ACCESS_TOKEN", "")
API_VER = "2026-04"


# ── Bestandsnaam-analyse ──────────────────────────────────────────────────────

def _filename_from_url(url: str) -> str:
    """Extraheer de bestandsnaam uit een Shopify CDN URL."""
    # https://cdn.shopify.com/.../products/bestandsnaam.jpg?v=...
    path = url.split("?")[0].rstrip("/")
    return path.split("/")[-1]


def _image_name_status(src: str) -> str:
    """
    Bepaal of de bestandsnaam SEO-vriendelijk is of een leveranciersnaam.

    Leveranciersnaam-indicatoren:
      - Geen koppeltekens (bijv. 'B0126001.jpg', 'IMG1234.jpg')
      - Begint met hoofdletters + cijfers (bijv. 'B0126_001_WH.jpg')
      - Bevat underscores maar geen koppeltekens
      - Korter dan 15 tekens voor de extensie
      - Alleen cijfers en letters, geen beschrijvende woorden

    SEO-vriendelijk:
      - Koppeltekens aanwezig
      - Lowercase
      - Beschrijvende woorden (>= 2 woorden na splitsen op '-')
    """
    if not src:
        return "unknown"

    fname = _filename_from_url(src)
    name  = re.sub(r"\.[a-zA-Z]{3,4}$", "", fname)  # extensie weghalen

    has_hyphens     = "-" in name
    has_underscores = "_" in name
    word_count      = len([w for w in name.split("-") if w]) if has_hyphens else 1
    is_short        = len(name) < 15
    mostly_upper    = sum(1 for c in name if c.isupper()) > len(name) * 0.4
    starts_code     = bool(re.match(r"^[A-Z]{1,3}\d", name))  # bijv. B0126, IMG, SKU

    # Leveranciersnaam-patroon
    if starts_code:
        return "supplier"
    if has_underscores and not has_hyphens:
        return "supplier"
    if is_short and not has_hyphens:
        return "supplier"
    if mostly_upper and not has_hyphens:
        return "supplier"

    # SEO-vriendelijk
    if has_hyphens and word_count >= 2:
        return "seofriendly"

    return "unknown"


# ── Shopify REST ophalen ──────────────────────────────────────────────────────

def _fetch_all_products() -> list[dict]:
    """Haal alle actieve producten op met image-data via REST API (gepagineerd)."""
    headers = {"X-Shopify-Access-Token": TOKEN}
    url     = f"https://{STORE}/admin/api/{API_VER}/products.json"
    params  = {
        "status": "active",
        "fields": "id,handle,images,body_html,variants",
        "limit":  250,
    }
    results = []
    page    = 0

    while url:
        time.sleep(0.5)  # 2 req/sec max
        resp = requests.get(url, headers=headers, params=params, timeout=30)

        if resp.status_code == 429:
            print("  Rate limit — 10s wachten...")
            time.sleep(10)
            resp = requests.get(url, headers=headers, params=params, timeout=30)

        resp.raise_for_status()
        products = resp.json().get("products", [])
        results.extend(products)
        page += 1
        print(f"  Page {page}: {len(products)} producten | totaal {len(results)}", end="\r")

        # Volgende pagina via Link header
        link = resp.headers.get("Link", "")
        url, params = None, None
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip().strip("<>")

    print(f"\n  Klaar: {len(results)} producten opgehaald")
    return results


# ── Row bouwen ────────────────────────────────────────────────────────────────

def _build_row(product: dict) -> dict:
    import re as _re
    handle  = product.get("handle", "")
    images  = sorted(product.get("images", []), key=lambda i: i.get("position", 999))
    count   = len(images)
    first   = images[0] if images else None

    src        = first.get("src", "") if first else ""
    alt        = (first.get("alt") or "").strip() if first else ""
    alt_status = "ok" if alt else "missing"
    name_stat  = _image_name_status(src) if src else "unknown"

    # Productomschrijving
    body_html   = product.get("body_html") or ""
    body_text   = _re.sub(r"<[^>]+>", "", body_html).strip()
    has_desc    = len(body_text) > 10
    desc_length = len(body_text)

    # SKU van eerste variant
    variants = product.get("variants") or []
    sku = (variants[0].get("sku") or "").strip() if variants else ""

    return {
        "handle":             handle,
        "has_image":          count > 0,
        "image_count":        count,
        "first_image_src":    src or None,
        "first_image_alt":    alt or None,
        "image_alt_status":   alt_status,
        "image_name_status":  name_stat,
        "has_description":    has_desc,
        "description_length": desc_length,
        "sku":                sku or None,
        "updated_at":         datetime.now(timezone.utc).isoformat(),
    }


# ── Upsert via psycopg2 ───────────────────────────────────────────────────────

def _upsert(rows: list[dict], batch_size: int = 500) -> None:
    import psycopg2
    import psycopg2.extras

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        sys.exit("DATABASE_URL ontbreekt in .env")

    SQL = """
        UPDATE shopify_meta_audit SET
            has_image           = %(has_image)s,
            image_count         = %(image_count)s,
            first_image_src     = %(first_image_src)s,
            first_image_alt     = %(first_image_alt)s,
            image_alt_status    = %(image_alt_status)s,
            image_name_status   = %(image_name_status)s,
            has_description     = %(has_description)s,
            description_length  = %(description_length)s,
            sku                 = %(sku)s,
            updated_at          = %(updated_at)s
        WHERE handle = %(handle)s
    """
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    cur  = conn.cursor()
    total = len(rows)
    try:
        for i in range(0, total, batch_size):
            chunk = rows[i:i + batch_size]
            psycopg2.extras.execute_batch(cur, SQL, chunk)
            conn.commit()
            print(f"  Update {min(i + batch_size, total)}/{total}", end="\r")
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    print()


# ── Publieke functie ──────────────────────────────────────────────────────────

def sync(dry_run: bool = False) -> dict:
    """
    Sync foto-status van alle actieve Shopify-producten naar shopify_meta_audit.
    Geeft resultaat-dict terug.
    """
    if not STORE or not TOKEN:
        return {"error": "SHOPIFY_STORE of SHOPIFY_ACCESS_TOKEN ontbreekt in .env", "total": 0}

    try:
        print("Stap 1/2 — Shopify producten ophalen (REST API) ...")
        products = _fetch_all_products()

        print("Stap 2/2 — Verwerken ...")
        rows = [_build_row(p) for p in products]

        # Statistieken
        stats = {
            "totaal":          len(rows),
            "geen_foto":       sum(1 for r in rows if not r["has_image"]),
            "geen_alt":        sum(1 for r in rows if r["has_image"] and r["image_alt_status"] == "missing"),
            "supplier":        sum(1 for r in rows if r["image_name_status"] == "supplier"),
            "seofriendly":     sum(1 for r in rows if r["image_name_status"] == "seofriendly"),
            "geen_omschrijving": sum(1 for r in rows if not r["has_description"]),
        }

        print(f"\n{'─'*40}")
        print(f"Totaal producten:        {stats['totaal']}")
        print(f"Geen foto:               {stats['geen_foto']}")
        print(f"Foto, geen alt-tekst:    {stats['geen_alt']}")
        print(f"Leveranciersnaam:        {stats['supplier']}")
        print(f"SEO-vriendelijke naam:   {stats['seofriendly']}")
        print(f"Geen productomschrijving:{stats['geen_omschrijving']}")

        if dry_run:
            print("\n--dry-run: eerste 5 rijen:")
            for r in rows[:5]:
                print(r)
            return {**stats, "dry_run": True}

        print(f"\nUpdaten shopify_meta_audit ({len(rows)} rijen) ...")
        _upsert(rows)
        print("Foto-sync klaar ✓")
        return {**stats, "error": None}

    except Exception as e:
        return {"error": str(e), "totaal": 0}


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    result = sync(dry_run=args.dry_run)
    if result.get("error"):
        print(f"\nFOUT: {result['error']}", file=sys.stderr)
        sys.exit(1)
