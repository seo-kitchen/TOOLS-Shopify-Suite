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

Bevat ook een pure-Python entrypoint `resize_photos(...)` voor gebruik
vanuit de Streamlit dashboard.
"""

import argparse
import os
import sys
import time
from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path
from typing import Callable, Optional

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


# ---------------------------------------------------------------------------
# Result types & exceptions
# ---------------------------------------------------------------------------


class ResizeError(Exception):
    """Raised when the resize pipeline cannot start/complete."""


@dataclass
class ResizeResult:
    processed_count: int = 0
    uploaded_count: int = 0
    skipped_count: int = 0
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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

    with_photos = []
    for p in all_products:
        if any(is_source_url(p.get(f) or "") for f in PHOTO_FIELDS):
            with_photos.append(p)

    if limit:
        with_photos = with_photos[:limit]

    return with_photos


# ---------------------------------------------------------------------------
# Pure-function entrypoint
# ---------------------------------------------------------------------------


def resize_photos(
    dry_run: bool = False,
    limit: int | None = None,
    skip_existing: bool = True,
    progress: Callable[[int, int, str], None] | None = None,
    logger: Callable[[str], None] | None = None,
) -> ResizeResult:
    """Resize Serax CDN foto's, upload naar Supabase Storage, update DB URLs.

    `skip_existing=True` betekent: sla over wat al op Supabase Storage staat
    (dit is de default van de pipeline — slechts expliciet uitgezet kan extra werk opleveren).
    """
    log = logger or (lambda _msg: None)

    def _progress(step: int, total: int, msg: str) -> None:
        if progress:
            progress(step, total, msg)

    try:
        sb = get_supabase()
    except Exception as e:
        raise ResizeError(f"Supabase verbinden mislukt: {e}") from e

    log("Producten ophalen...")
    try:
        products = fetch_products_with_photos(sb, limit)
    except Exception as e:
        raise ResizeError(f"Producten ophalen mislukt: {e}") from e
    log(f"  {len(products)} producten met foto's gevonden")

    # Analyse
    total_photos = 0
    already_done = 0
    to_process: list[dict] = []

    for product in products:
        for field in PHOTO_FIELDS:
            url = (product.get(field) or "").strip()
            if not url:
                continue

            if is_already_resized(url):
                already_done += 1
                if skip_existing:
                    continue

            if not is_source_url(url) and not is_already_resized(url):
                continue

            if is_already_resized(url) and skip_existing:
                continue

            if is_source_url(url):
                total_photos += 1
                to_process.append({"product": product, "field": field, "url": url})

    log(f"Totaal foto's van {SOURCE_DOMAIN}: {total_photos}")
    log(f"Reeds geresized (Supabase): {already_done}")
    log(f"Te verwerken: {len(to_process)}")

    result = ResizeResult()
    result.skipped_count = already_done

    if dry_run:
        sample = to_process[:10]
        oversized_count = 0
        ok_count = 0
        for item in sample:
            img, w, h = download_and_check(item["url"])
            if img is None:
                continue
            if needs_resize(w, h):
                oversized_count += 1
            else:
                ok_count += 1
            img.close()
        log(f"[DRY RUN] Steekproef: {oversized_count} te groot, {ok_count} OK")
        result.processed_count = len(to_process)
        return result

    total = len(to_process)
    for i, item in enumerate(to_process):
        product = item["product"]
        field = item["field"]
        url = item["url"]
        sku = product["sku"]
        pid = product["id"]

        _progress(i + 1, total, f"{sku} {field}")

        img, w, h = download_and_check(url)
        if img is None:
            result.errors.append(f"{sku} {field}: download failed")
            continue

        try:
            img_data = resize_and_encode(img)
        except Exception as e:
            result.errors.append(f"{sku} {field}: encode failed: {e}")
            img.close()
            continue

        img.close()

        path = storage_path(sku, field)
        if not upload_to_storage(sb, path, img_data):
            result.errors.append(f"{sku} {field}: upload failed")
            continue

        new_url = public_url(path)
        try:
            sb.table("seo_products").update({field: new_url}).eq("id", pid).execute()
            result.uploaded_count += 1
        except Exception as e:
            result.errors.append(f"{sku} {field}: DB update failed: {e}")
            continue

        result.processed_count += 1

        if (i + 1) % 20 == 0:
            time.sleep(1)

    return result


def main():
    parser = argparse.ArgumentParser(description="Foto's resizen voor Shopify")
    parser.add_argument("--dry-run", action="store_true", help="Alleen tellen, niet verwerken")
    parser.add_argument("--limit", type=int, help="Max aantal producten")
    parser.add_argument("--skip-existing", action="store_true", help="Sla reeds geresizede over")
    args = parser.parse_args()

    try:
        result = resize_photos(
            dry_run=args.dry_run,
            limit=args.limit,
            skip_existing=args.skip_existing or True,
            logger=lambda m: print(m),
            progress=lambda i, t, msg: print(f"  [{i}/{t}] {msg}"),
        )
    except ResizeError as e:
        print(f"FOUT: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"\n{'=' * 50}")
    print("KLAAR")
    print(f"{'=' * 50}")
    print(f"  Verwerkt: {result.processed_count}")
    print(f"  Geüpload: {result.uploaded_count}")
    print(f"  Overgeslagen: {result.skipped_count}")
    print(f"  Fouten: {len(result.errors)}")
    if result.errors[:10]:
        for err in result.errors[:10]:
            print(f"    - {err}")
    print(f"\nFoto's staan in: {SUPABASE_URL}/storage/v1/object/public/{BUCKET}/serax/")


if __name__ == "__main__":
    main()
