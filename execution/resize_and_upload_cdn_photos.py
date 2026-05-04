# -*- coding: utf-8 -*-
"""
Download, resize en upload externe CDN fotos (serax.com / shopify.com / valerie-objects.com)
naar Supabase storage en update seo_products met de nieuwe URLs.

Max 1200x1601 px, JPEG q85 progressive — zelfde als Hextom Image Bulk Edit target.

Gebruik:
    python execution/resize_and_upload_cdn_photos.py
    python execution/resize_and_upload_cdn_photos.py --limit 10   # test
    python execution/resize_and_upload_cdn_photos.py --sku B7326004-500
"""

import argparse
import io
import os
import sys
import time
from pathlib import Path

import requests
from PIL import Image
from dotenv import load_dotenv

# Serax B2B fotos kunnen 100MP+ zijn — verhoog PIL limiet
Image.MAX_IMAGE_PIXELS = 300_000_000

load_dotenv()

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
BUCKET = "product-photos"
MAX_WIDTH = 1200
MAX_HEIGHT = 1601
JPEG_QUALITY = 85

PHOTO_COLS = [
    "photo_packshot_1", "photo_packshot_2", "photo_packshot_3",
    "photo_packshot_4", "photo_packshot_5",
    "photo_lifestyle_1", "photo_lifestyle_2", "photo_lifestyle_3",
    "photo_lifestyle_4", "photo_lifestyle_5",
]

COL_TO_SUFFIX = {
    "photo_packshot_1": "packshot_1", "photo_packshot_2": "packshot_2",
    "photo_packshot_3": "packshot_3", "photo_packshot_4": "packshot_4",
    "photo_packshot_5": "packshot_5",
    "photo_lifestyle_1": "lifestyle_1", "photo_lifestyle_2": "lifestyle_2",
    "photo_lifestyle_3": "lifestyle_3", "photo_lifestyle_4": "lifestyle_4",
    "photo_lifestyle_5": "lifestyle_5",
}

HEADERS = {"User-Agent": "Mozilla/5.0"}


def get_supabase():
    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def is_external(url):
    return bool(url) and "supabase.co/storage" not in url


def resize_image(img_bytes: bytes) -> bytes:
    img = Image.open(io.BytesIO(img_bytes))
    if img.mode not in ("RGB", "L"):
        img = img.convert("RGB")
    w, h = img.size
    if w > MAX_WIDTH or h > MAX_HEIGHT:
        img.thumbnail((MAX_WIDTH, MAX_HEIGHT), Image.LANCZOS)
    out = io.BytesIO()
    img.save(out, format="JPEG", quality=JPEG_QUALITY, progressive=True, optimize=True)
    return out.getvalue()


def upload_to_supabase(sb, sku: str, suffix: str, img_bytes: bytes) -> str:
    path = f"serax/{sku}_{suffix}.jpg"
    sb.storage.from_(BUCKET).upload(
        path, img_bytes,
        file_options={"content-type": "image/jpeg", "upsert": "true"}
    )
    return f"{SUPABASE_URL}/storage/v1/object/public/{BUCKET}/{path}"


def process_product(sb, row: dict, dry_run=False) -> dict:
    sku = row["sku"]
    updates = {}

    for col in PHOTO_COLS:
        url = row.get(col)
        if not url or not is_external(url):
            continue
        suffix = COL_TO_SUFFIX[col]
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            if resp.status_code != 200:
                print(f"    {col}: HTTP {resp.status_code} — skip")
                continue
            orig_kb = len(resp.content) // 1024
            resized = resize_image(resp.content)
            new_kb = len(resized) // 1024
            if not dry_run:
                new_url = upload_to_supabase(sb, sku, suffix, resized)
                updates[col] = new_url
            print(f"    {col}: {orig_kb}KB → {new_kb}KB ✓")
            time.sleep(0.2)
        except Exception as e:
            print(f"    {col}: FOUT — {e}")

    if updates and not dry_run:
        sb.table("seo_products").update(updates).eq("sku", sku).execute()

    return updates


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, help="Max aantal producten")
    ap.add_argument("--sku", help="Specifieke SKU")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    sb = get_supabase()

    if args.sku:
        r = sb.table("seo_products").select("sku," + ",".join(PHOTO_COLS)).eq("sku", args.sku).execute()
    else:
        # Haal alle producten op met externe URLs
        rows_all = []
        for prefix in ["B", "V"]:
            r = (sb.table("seo_products")
                   .select("sku," + ",".join(PHOTO_COLS))
                   .like("sku", f"{prefix}%")
                   .not_.is_("photo_packshot_1", "null")
                   .limit(500)
                   .execute())
            rows_all.extend(r.data or [])
        # Filter: alleen externe URLs
        r_data = [row for row in rows_all if is_external(row.get("photo_packshot_1", "") or "")]
        r = type("R", (), {"data": r_data})()

    rows = r.data or []
    if args.limit:
        rows = rows[:args.limit]

    print(f"Te verwerken: {len(rows)} producten{'  (DRY RUN)' if args.dry_run else ''}\n")

    ok = errors = 0
    for i, row in enumerate(rows, 1):
        sku = row["sku"]
        ext_count = sum(1 for c in PHOTO_COLS if is_external(row.get(c) or ""))
        print(f"[{i}/{len(rows)}] {sku} ({ext_count} externe fotos)")
        updates = process_product(sb, row, dry_run=args.dry_run)
        if updates:
            ok += 1
        else:
            errors += 1
        if i % 20 == 0:
            time.sleep(1)

    print(f"\n{'='*50}")
    print(f"Verwerkt: {ok}  |  Fouten/leeg: {errors}")


if __name__ == "__main__":
    main()
