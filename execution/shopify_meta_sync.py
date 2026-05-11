"""Shopify → Supabase: synct ALLE actieve producten naar shopify_meta_audit.

Haalt op via Shopify GraphQL Bulk Operations (één API-call, async download):
  - Product ID, handle, title, vendor, product_type, status, tags, prijs
  - SEO title & description  (seo.title / seo.description)

Upsert op shopify_product_id. Velden die NOOIT worden overschreven:
  approved_title, approved_desc, pushed_at  (handmatig gecureerd).

Vereisten (.env):
  SHOPIFY_STORE            bijv. interieur-shop-nl.myshopify.com
  SHOPIFY_ACCESS_TOKEN     private-app of custom-app token met read_products scope

Gebruik:
    python execution/shopify_meta_sync.py
    python execution/shopify_meta_sync.py --dry-run      # toon 5 rijen, geen upsert
    python execution/shopify_meta_sync.py --status any   # ook draft/archived
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
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
GQL_URL = f"https://{STORE}/admin/api/{API_VER}/graphql.json"

TITLE_MAX = 58
TITLE_MIN = 30
DESC_MAX  = 155
DESC_MIN  = 120

TEMPLATE_PATTERNS = [
    "bij Interieur-shop.nl",
    "Stijlvol design en snelle levering",
]

VENDOR_NORMALIZE = {
    "sp":     "Salt & Pepper",
    "SP":     "Salt & Pepper",
    "S&P":    "Salt & Pepper",
    "SP_COL": "Salt & Pepper",
    "sp_col": "Salt & Pepper",
}


# ── Audit helpers ─────────────────────────────────────────────────────────────

def _normalize_vendor(v: str) -> str:
    v = (v or "").strip()
    return VENDOR_NORMALIZE.get(v, VENDOR_NORMALIZE.get(v.lower(), v))


def _audit_title(t: str) -> str:
    if not t:
        return "missing"
    n = len(t)
    if n > TITLE_MAX:
        return "too_long"
    if n < TITLE_MIN:
        return "too_short"
    return "ok"


def _audit_desc(d: str) -> str:
    if not d:
        return "missing"
    if any(p in d for p in TEMPLATE_PATTERNS):
        return "templated"
    n = len(d)
    if n > DESC_MAX:
        return "too_long"
    if n < DESC_MIN:
        return "too_short"
    return "ok"


# ── GraphQL helpers ───────────────────────────────────────────────────────────

def _gql(query: str, variables: dict | None = None) -> dict:
    headers = {
        "X-Shopify-Access-Token": TOKEN,
        "Content-Type": "application/json",
    }
    payload: dict = {"query": query}
    if variables:
        payload["variables"] = variables
    resp = requests.post(GQL_URL, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


BULK_MUTATION = """
mutation BulkSync($query: String!) {
  bulkOperationRunQuery(query: $query) {
    bulkOperation {
      id
      status
    }
    userErrors {
      field
      message
    }
  }
}
"""

POLL_QUERY = """
{
  currentBulkOperation {
    id
    status
    errorCode
    objectCount
    fileSize
    url
    partialDataUrl
  }
}
"""


def _start_bulk(status_filter: str) -> str:
    """Start bulk query, return operation ID."""
    product_query = f"""
    {{
      products(query: "status:{status_filter}") {{
        edges {{
          node {{
            id
            title
            handle
            vendor
            productType
            status
            tags
            publishedAt
            priceRangeV2 {{
              minVariantPrice {{
                amount
              }}
            }}
            seo {{
              title
              description
            }}
          }}
        }}
      }}
    }}
    """
    result = _gql(BULK_MUTATION, {"query": product_query})
    errors = result.get("data", {}).get("bulkOperationRunQuery", {}).get("userErrors", [])
    if errors:
        raise RuntimeError(f"Bulk operation mislukt: {errors}")
    op_id = result["data"]["bulkOperationRunQuery"]["bulkOperation"]["id"]
    print(f"  Bulk operatie gestart: {op_id}")
    return op_id


def _poll_bulk(max_wait: int = 600) -> dict:
    """Poll tot bulk klaar is. Geeft de afgeronde operatie terug."""
    print("  Wachten op Shopify bulk export", end="", flush=True)
    deadline = time.time() + max_wait
    while time.time() < deadline:
        time.sleep(5)
        print(".", end="", flush=True)
        data = _gql(POLL_QUERY)["data"]["currentBulkOperation"]
        status = data.get("status", "")
        if status == "COMPLETED":
            print(f" klaar! ({data.get('objectCount', '?')} objecten)")
            return data
        if status in ("FAILED", "CANCELED"):
            raise RuntimeError(f"Bulk operatie gefaald: status={status}, code={data.get('errorCode')}")
    raise TimeoutError(f"Bulk operatie niet klaar binnen {max_wait}s")


def _download_jsonl(url: str) -> list[dict]:
    """Download en parse de JSONL bulk-output."""
    resp = requests.get(url, timeout=120, stream=True)
    resp.raise_for_status()

    # Shopify geeft soms gzip terug
    content_enc = resp.headers.get("Content-Encoding", "")
    raw = resp.content
    if content_enc == "gzip" or url.endswith(".gz"):
        raw = gzip.decompress(raw)

    lines = raw.decode("utf-8").strip().splitlines()
    records = []
    for line in lines:
        line = line.strip()
        if line:
            records.append(json.loads(line))
    return records


# ── Row bouwen ────────────────────────────────────────────────────────────────

def _build_row(node: dict) -> dict | None:
    gid = node.get("id", "")
    # gid = "gid://shopify/Product/1234567890"
    numeric_id = gid.split("/")[-1] if "/" in gid else gid
    if not numeric_id:
        return None

    title      = (node.get("title") or "").strip()
    handle     = (node.get("handle") or "").strip()
    vendor     = _normalize_vendor(node.get("vendor") or "")
    ptype      = (node.get("productType") or "").strip()
    status     = (node.get("status") or "").lower()
    tags_list  = node.get("tags") or []
    tags       = ", ".join(tags_list) if isinstance(tags_list, list) else str(tags_list)
    published  = node.get("publishedAt")  # None = draft/not published

    seo        = node.get("seo") or {}
    meta_title = (seo.get("title") or "").strip()
    meta_desc  = (seo.get("description") or "").strip()

    price_range = node.get("priceRangeV2") or {}
    min_price   = price_range.get("minVariantPrice") or {}
    try:
        price = float(min_price.get("amount") or 0) or None
    except (ValueError, TypeError):
        price = None

    return {
        "shopify_product_id":       numeric_id,
        "handle":                   handle,
        "product_title":            title,
        "vendor":                   vendor,
        "product_type":             ptype,
        "product_status":           status,
        "published_at":             published,
        "price":                    price,
        "tags":                     tags,
        "current_meta_title":       meta_title or None,
        "current_meta_description": meta_desc or None,
        "current_title_length":     len(meta_title),
        "current_desc_length":      len(meta_desc),
        "title_status":             _audit_title(meta_title),
        "desc_status":              _audit_desc(meta_desc),
        "review_status":            "pending",
        "updated_at":               datetime.now(timezone.utc).isoformat(),
    }


# ── Upsert ────────────────────────────────────────────────────────────────────

def _upsert(rows: list[dict], batch_size: int = 500) -> None:
    """Upsert via directe PostgreSQL verbinding (omzeilt PostgREST volledig)."""
    import psycopg2
    import psycopg2.extras

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        sys.exit(
            "FOUT: DATABASE_URL ontbreekt in .env\n"
            "Voeg toe: DATABASE_URL=postgresql://postgres:[pw]@db.[ref].supabase.co:5432/postgres\n"
            "(te vinden in Supabase Dashboard → Settings → Database → Connection string)"
        )

    # Sanity check: DSN moet beginnen met postgresql:// of postgres://.
    # Op Railway is dit fout gegaan als de waarde de variabele-naam zelf is geworden
    # (alle leestekens vervangen door _, alles uppercase). Detecteer en faal duidelijk.
    if not (db_url.startswith("postgresql://") or db_url.startswith("postgres://")):
        sys.exit(
            "FOUT: DATABASE_URL is geen geldige Postgres-DSN.\n"
            f"  Huidige waarde (begint met): {db_url[:60]}...\n"
            "  Verwacht: postgresql://postgres:[pw]@db.[ref].supabase.co:5432/postgres\n\n"
            "Op Railway? Check Variables → DATABASE_URL: zet daar de echte URI "
            "(Supabase Dashboard → Settings → Database → Connection string → URI), "
            "niet een template-referentie."
        )

    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    cur = conn.cursor()

    SQL = """
        INSERT INTO shopify_meta_audit (
            shopify_product_id, handle, product_title, vendor, product_type,
            product_status, price, tags, published_at,
            current_meta_title, current_meta_description,
            current_title_length, current_desc_length,
            title_status, desc_status, review_status, updated_at
        ) VALUES %s
        ON CONFLICT (shopify_product_id) DO UPDATE SET
            handle                   = EXCLUDED.handle,
            product_title             = EXCLUDED.product_title,
            vendor                    = EXCLUDED.vendor,
            product_type              = EXCLUDED.product_type,
            product_status            = EXCLUDED.product_status,
            price                     = EXCLUDED.price,
            tags                      = EXCLUDED.tags,
            published_at              = EXCLUDED.published_at,
            current_meta_title        = EXCLUDED.current_meta_title,
            current_meta_description  = EXCLUDED.current_meta_description,
            current_title_length      = EXCLUDED.current_title_length,
            current_desc_length       = EXCLUDED.current_desc_length,
            title_status              = EXCLUDED.title_status,
            desc_status               = EXCLUDED.desc_status,
            updated_at                = NOW()
    """

    total = len(rows)
    try:
        for i in range(0, total, batch_size):
            chunk = rows[i:i + batch_size]
            values = [
                (
                    r["shopify_product_id"], r["handle"], r.get("product_title"),
                    r.get("vendor"), r.get("product_type"), r.get("product_status"),
                    r.get("price"), r.get("tags"), r.get("published_at"),
                    r.get("current_meta_title"), r.get("current_meta_description"),
                    r.get("current_title_length"), r.get("current_desc_length"),
                    r.get("title_status"), r.get("desc_status"), "pending",
                    datetime.now(timezone.utc),
                )
                for r in chunk
            ]
            psycopg2.extras.execute_values(cur, SQL, values)
            conn.commit()
            print(f"  Upsert {min(i + batch_size, total)}/{total}", end="\r")
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    print()


# ── Samenvatting ──────────────────────────────────────────────────────────────

def _summary(rows: list[dict]) -> None:
    t = Counter(r["title_status"] for r in rows)
    d = Counter(r["desc_status"] for r in rows)
    s = Counter(r["product_status"] for r in rows)

    print(f"\n{'─'*40}")
    print(f"Totaal producten: {len(rows)}")
    print(f"\nShopify status:")
    for k, v in s.most_common():
        print(f"  {k:12} {v}")
    print(f"\nMeta title status:")
    for k, v in t.most_common():
        print(f"  {k:12} {v}")
    print(f"\nMeta description status:")
    for k, v in d.most_common():
        print(f"  {k:12} {v}")


# ── Publieke functie (ook importeerbaar door Streamlit) ───────────────────────

def sync(status_filter: str = "active", dry_run: bool = False) -> dict:
    """
    Voer de volledige sync uit. Geeft dict terug met resultaat-stats.

    Parameters:
        status_filter: "active" | "draft" | "archived" | "any"
        dry_run:       True = toon 5 rijen, geen upsert

    Returns:
        {"total": int, "upserted": int, "skipped": int, "error": str|None}
    """
    if not STORE or not TOKEN:
        return {"error": "SHOPIFY_STORE of SHOPIFY_ACCESS_TOKEN ontbreekt in .env", "total": 0}

    try:
        print(f"Stap 1/3 — Bulk operatie starten (filter: status:{status_filter}) ...")
        _start_bulk(status_filter)

        print("Stap 2/3 — Wachten op Shopify ...")
        op = _poll_bulk()

        download_url = op.get("url")
        if not download_url:
            return {"error": "Geen download-URL — geen producten gevonden of lege export.", "total": 0}

        print(f"Stap 3/3 — Downloaden & verwerken ({op.get('objectCount','?')} objecten) ...")
        nodes = _download_jsonl(download_url)

        rows = []
        skipped = 0
        for node in nodes:
            # Bulk JSONL bevat ook nested nodes (bijv. priceRangeV2) als aparte regels met __parentId.
            # We filteren op Product-nodes: id begint met gid://shopify/Product/
            if not str(node.get("id", "")).startswith("gid://shopify/Product/"):
                continue
            row = _build_row(node)
            if row:
                rows.append(row)
            else:
                skipped += 1

        _summary(rows)

        if dry_run:
            print(f"\n--dry-run: eerste 5 rijen:")
            for r in rows[:5]:
                print(json.dumps(r, ensure_ascii=False, indent=2))
            return {"total": len(rows), "upserted": 0, "skipped": skipped, "dry_run": True}

        print(f"\nUpserting {len(rows)} rijen naar shopify_meta_audit ...")
        _upsert(rows)
        print("Sync klaar ✓")

        return {"total": len(rows), "upserted": len(rows), "skipped": skipped, "error": None}

    except Exception as exc:
        return {"error": str(exc), "total": 0, "upserted": 0, "skipped": 0}


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")

    ap = argparse.ArgumentParser(description="Sync actieve Shopify producten → shopify_meta_audit")
    ap.add_argument("--status",  default="active", choices=["active", "draft", "archived", "any"],
                    help="Shopify product status filter (default: active)")
    ap.add_argument("--dry-run", action="store_true", help="Toon 5 rijen, geen upsert")
    args = ap.parse_args()

    result = sync(status_filter=args.status, dry_run=args.dry_run)
    if result.get("error"):
        print(f"\nFOUT: {result['error']}", file=sys.stderr)
        sys.exit(1)
