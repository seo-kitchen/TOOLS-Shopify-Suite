"""
Foto's resizen voor Shopify import.

Download foto's van Serax CDN (images.finedl.eu), resize naar max 4999x4999
(onder Shopify's 25 MP limiet), upload naar Supabase Storage, en update de
URLs in de database.

Gebruik:
    python execution/resize_photos.py --dry-run          # alleen tellen
    python execution/resize_photos.py --limit 5          # test met 5 producten
    python execution/resize_photos.py                    # alles verwerken
    python execution/resize_photos.py --skip-existing    # sla reeds geresizede over
"""

import argparse
import os
import sys
import time
from io import BytesIO
from pathlib import Path

import requests
from PIL import Image
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

MAX_DIMENSION = 4999  # Shopify max = 5000x5000 = 25 MP
JPEG_QUALITY = 90
BUCKET = "product-photos"
SOURCE_DOMAIN = "images.finedl.eu"

PHOTO_FIELDS = [
    "photo_packshot_1", "photo_packshot_2", "photo_packshot_3",
    "photo_packshot_4", "photo_packshot_5",
    "photo_lifestyle_1", "photo_lifestyle_2", "photo_lifestyle_3",
    "photo_lifestyle_4", "photo_lifestyle_5",
]


def get_supabase():
    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def is_source_url(url: str) -> bool:
    """Check of URL van de Serax CDN komt."""
    return bool(url) and SOURCE_DOMAIN in url


def is_already_resized(url: str) -> bool:
    """Check of URL al naar Supabase Storage wijst."""
    return bool(url) and "supabase.co/storage" in url


def storage_path(sku: str, field: str) -> str:
    """Genereer storage path: serax/B0219213_packshot_1.jpg"""
    clean_sku = sku.replace("/", "_").replace(" ", "_")
    return f"serax/{clean_sku}_{field.replace('photo_', '')}.jpg"


def public_url(path: str) -> str:
    """Genereer publieke URL voor Supabase Storage bestand."""
    return f"{SUPABASE_URL}/storage/v1/object/public/{BUCKET}/{path}"


def download_and_check(url: str) -> tuple[Image.Image | None, int, int]:
    """Download afbeelding, geef (image, width, height) terug."""
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        img = Image.open(BytesIO(r.content))
        return img, img.size[0], img.size[1]
    except Exception as e:
        print(f"    Download fout: {e}")
        return None, 0, 0


def needs_resize(width: int, height: int) -> bool:
    """Check of afbeelding boven Shopify's 25 MP limiet zit."""
    return width > MAX_DIMENSION or height > MAX_DIMENSION


def resize_and_encode(img: Image.Image) -> bytes:
    """Resize naar max 4999x4999, return als JPEG bytes."""
    img = img.convert("RGB")
    img.thumbnail((MAX_DIMENSION, MAX_DIMENSION), Image.LANCZOS)
    buf = BytesIO()
    img.save(buf, format="JPEG", quality=JPEG_QUALITY, optimize=True)
    return buf.getvalue()


def upload_to_storage(sb, path: str, data: bytes) -> bool:
    """Upload bytes naar Supabase Storage."""
    try:
        sb.storage.from_(BUCKET).upload(
            path, data,
            {"content-type": "image/jpeg", "upsert": "true"}
        )
        return True
    except Exception as e:
        print(f"    Upload fout: {e}")
        return False


def fetch_products_with_photos(sb, limit: int | None = None) -> list[dict]:
    """Haal producten op die minstens 1 foto-URL hebben."""
    fields = "id, sku, " + ", ".join(PHOTO_FIELDS)
    query = sb.table("seo_products").select(fields)

    all_products = []
    offset = 0
    batch_size = 1000

    while True:
        q = query.range(offset, offset + batch_size - 1)
        result = q.execute()
        if not result.data:
            break
        all_products.extend(result.data)
        if len(result.data) < batch_size:
            break
        offset += batch_size

    # Filter: alleen producten met minstens 1 source URL
    with_photos = []
    for p in all_products:
        if any(is_source_url(p.get(f) or "") for f in PHOTO_FIELDS):
            with_photos.append(p)

    if limit:
        with_photos = with_photos[:limit]

    return with_photos


def main():
    parser = argparse.ArgumentParser(description="Foto's resizen voor Shopify")
    parser.add_argument("--dry-run", action="store_true", help="Alleen tellen, niet verwerken")
    parser.add_argument("--limit", type=int, help="Max aantal producten")
    parser.add_argument("--skip-existing", action="store_true", help="Sla reeds geresizede over")
    args = parser.parse_args()

    sb = get_supabase()

    print("Producten ophalen...")
    products = fetch_products_with_photos(sb, args.limit)
    print(f"  {len(products)} producten met foto's gevonden")

    # Analyse
    total_photos = 0
    oversized = 0
    already_done = 0
    to_process = []

    print("\nFoto's analyseren...\n")

    for product in products:
        sku = product["sku"]
        for field in PHOTO_FIELDS:
            url = (product.get(field) or "").strip()
            if not url:
                continue

            if is_already_resized(url):
                already_done += 1
                continue

            if not is_source_url(url):
                continue

            total_photos += 1
            to_process.append({"product": product, "field": field, "url": url})

    print(f"Totaal foto's van {SOURCE_DOMAIN}: {total_photos}")
    print(f"Reeds geresized (Supabase): {already_done}")
    print(f"Te verwerken: {len(to_process)}")

    if args.dry_run:
        # Steekproef: check dimensies van eerste 10
        print(f"\n--- Steekproef dimensies (max 10) ---")
        sample = to_process[:10]
        oversized_count = 0
        ok_count = 0
        for item in sample:
            img, w, h = download_and_check(item["url"])
            if img is None:
                continue
            mp = w * h / 1_000_000
            too_big = needs_resize(w, h)
            status = "TE GROOT" if too_big else "OK"
            if too_big:
                oversized_count += 1
            else:
                ok_count += 1
            print(f"  {item['product']['sku']} {item['field']}: {w}x{h} = {mp:.1f} MP [{status}]")
            img.close()

        print(f"\nSteekproef: {oversized_count} te groot, {ok_count} OK")
        print(f"\n[DRY RUN] Geen foto's verwerkt. Draai zonder --dry-run om te resizen.")
        return

    # Verwerken
    print(f"\nStart verwerking van {len(to_process)} foto's...\n")

    success = 0
    skipped = 0
    errors = 0
    resized = 0

    for i, item in enumerate(to_process):
        product = item["product"]
        field = item["field"]
        url = item["url"]
        sku = product["sku"]
        pid = product["id"]

        prefix = f"[{i+1}/{len(to_process)}] {sku} {field}"

        # Download
        img, w, h = download_and_check(url)
        if img is None:
            errors += 1
            continue

        mp = w * h / 1_000_000

        if not needs_resize(w, h):
            # Onder de limiet — toch uploaden naar eigen storage voor consistentie
            print(f"  {prefix}: {w}x{h} ({mp:.1f} MP) OK - uploaden zonder resize")
            img_data = resize_and_encode(img)  # converteert naar JPEG, behoudt grootte
        else:
            print(f"  {prefix}: {w}x{h} ({mp:.1f} MP) -> resize naar max {MAX_DIMENSION}")
            img_data = resize_and_encode(img)
            resized += 1

        img.close()

        # Upload
        path = storage_path(sku, field)
        if not upload_to_storage(sb, path, img_data):
            errors += 1
            continue

        # DB update
        new_url = public_url(path)
        try:
            sb.table("seo_products").update({field: new_url}).eq("id", pid).execute()
            success += 1
        except Exception as e:
            print(f"    DB update fout: {e}")
            errors += 1

        # Rate limiting (voorkom CDN blokkade)
        if (i + 1) % 20 == 0:
            pct = int((i + 1) / len(to_process) * 100)
            print(f"\n  --- Voortgang: {i+1}/{len(to_process)} ({pct}%) ---\n")
            time.sleep(1)

    print(f"\n{'='*50}")
    print(f"KLAAR")
    print(f"{'='*50}")
    print(f"  Succesvol: {success}")
    print(f"  Geresized: {resized}")
    print(f"  Overgeslagen: {skipped}")
    print(f"  Fouten: {errors}")
    print(f"\nFoto's staan in: {SUPABASE_URL}/storage/v1/object/public/{BUCKET}/serax/")


if __name__ == "__main__":
    main()
