"""
meta_audit_csv_export.py — Exporteer approved meta titles/descriptions naar CSV
voor bulk import via Hextom Bulk Product Edit.

CSV bevat:
  - Handle (Hextom's primaire key)
  - Product ID (referentie)
  - Title (productnaam, voor context)
  - Vendor
  - SEO Title (NEW)            ← de nieuwe meta title
  - SEO Description (NEW)      ← de nieuwe meta description
  - SEO Title (OLD)            ← huidige Shopify-waarde (voor rollback)
  - SEO Description (OLD)      ← huidige Shopify-waarde (voor rollback)

Twee bestanden worden geschreven:
  - {out}_import.csv     → push dit naar Hextom
  - {out}_rollback.csv   → houd bij, re-import dit als iets mis gaat

Gebruik:
    python execution/meta_audit_csv_export.py                    # alle approved
    python execution/meta_audit_csv_export.py --limit 200        # eerste 200
    python execution/meta_audit_csv_export.py --limit 200 --skip 200  # volgende 200
    python execution/meta_audit_csv_export.py --vendor "Serax"   # per vendor
    python execution/meta_audit_csv_export.py --out .tmp/batch1  # custom output name
"""

import argparse
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")


def get_supabase():
    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_approved(sb, vendor: str | None, limit: int | None, skip: int) -> list[dict]:
    rows, offset, page = [], 0, 1000
    while True:
        q = sb.table("shopify_meta_audit").select(
            "shopify_product_id, handle, product_title, vendor, "
            "current_meta_title, current_meta_description, "
            "approved_title, approved_desc, approved_at"
        ).eq("review_status", "approved")
        if vendor:
            q = q.eq("vendor", vendor)
        q = q.order("approved_at")  # oldest approvals eerst
        batch = q.range(offset, offset + page - 1).execute().data
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page:
            break
        offset += page

    if skip:
        rows = rows[skip:]
    if limit:
        rows = rows[:limit]
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--vendor", help="Filter op vendor")
    ap.add_argument("--limit", type=int, help="Maximum aantal producten")
    ap.add_argument("--skip", type=int, default=0, help="Skip eerste N producten (voor tweede batch)")
    ap.add_argument("--out", default=".tmp/meta_audit_batch",
                    help="Output pad prefix (zonder extensie)")
    args = ap.parse_args()

    import csv
    import pandas as pd
    sb = get_supabase()
    rows = fetch_approved(sb, args.vendor, args.limit, args.skip)

    # SKU-map uit Excel
    df = pd.read_excel(
        "master files/Alle Active Producten.xlsx"
    ).drop_duplicates(subset=["Product ID"])
    sku_map = {}
    for _, r in df.iterrows():
        pid = r["Product ID"]
        if pd.isna(pid):
            continue
        try:
            pid = str(int(float(pid)))
        except Exception:
            continue
        sku = r.get("Variant SKU")
        if pd.notna(sku) and str(sku).strip():
            sku_map[pid] = str(sku).strip()

    if not rows:
        sys.exit("Geen approved producten gevonden. "
                 "Keur eerst producten goed in het dashboard.")

    print(f"{len(rows)} approved producten geselecteerd.")

    out_dir = Path(args.out).parent
    out_dir.mkdir(parents=True, exist_ok=True)

    import_path = Path(f"{args.out}_import.csv")
    rollback_path = Path(f"{args.out}_rollback.csv")

    # Hextom importeert meestal op Handle. Kolomnamen passen bij
    # Shopify bulk-edit conventie: "SEO Title" en "SEO Description".
    # Ter referentie nemen we ook Product ID + productnaam mee.
    import_cols = [
        "Handle", "Product ID", "SKU", "Title", "Vendor",
        "SEO Title", "SEO Description",
    ]
    rollback_cols = [
        "Handle", "Product ID", "SKU", "Title", "Vendor",
        "SEO Title", "SEO Description",
    ]

    with import_path.open("w", newline="", encoding="utf-8") as f_imp, \
         rollback_path.open("w", newline="", encoding="utf-8") as f_rb:

        w_imp = csv.writer(f_imp)
        w_rb = csv.writer(f_rb)
        w_imp.writerow(import_cols)
        w_rb.writerow(rollback_cols)

        for r in rows:
            sku = sku_map.get(r["shopify_product_id"], "")
            w_imp.writerow([
                r["handle"],
                r["shopify_product_id"],
                sku,
                r.get("product_title") or "",
                r.get("vendor") or "",
                r.get("approved_title") or "",
                r.get("approved_desc") or "",
            ])
            w_rb.writerow([
                r["handle"],
                r["shopify_product_id"],
                sku,
                r.get("product_title") or "",
                r.get("vendor") or "",
                r.get("current_meta_title") or "",
                r.get("current_meta_description") or "",
            ])

    print(f"\nGeschreven:")
    print(f"  IMPORT (push naar Hextom):  {import_path}")
    print(f"  ROLLBACK (bewaar!):         {rollback_path}")
    print(f"\n  Aantal rijen: {len(rows)}")
    print(f"\n  Werkwijze:")
    print(f"  1. Open {import_path.name} en controleer een sample")
    print(f"  2. Importeer in Hextom Bulk Product Edit")
    print(f"  3. Test 5-10 producten in Shopify / op de live site")
    print(f"  4. Bij problemen: importeer {rollback_path.name} terug")


if __name__ == "__main__":
    main()
