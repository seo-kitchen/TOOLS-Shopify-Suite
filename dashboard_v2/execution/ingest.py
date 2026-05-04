"""
Stap 1: Leveranciers-Excel valideren en laden in Supabase.
Zie directives/ingest.md voor volledige instructies.

Gebruik:
    python execution/ingest.py --file masterdata.xlsx --fase 3 [--fotos foto_export.xlsx]
"""

import argparse
import sys
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

REQUIRED_COLUMNS = ["SKU", "EAN Piece", "EAN Packaging/Giftbox"]

COLUMN_ALIASES = {
    "EAN Packaging/Giftbox": ["EAN Packaging", "EAN Giftbox", "EAN Box"],
    "Product Material":      ["Material", "Materiaal"],
    "Category":              ["Product Category", "Serax Category"],
    "Item Category":         ["Serax Item Cat.", "Item Cat."],
    "Product Name":          ["Name", "Productnaam"],
    "Color":                 ["Colour"],
    "RRP Stuk EUR":          ["RRP Piece EUR", "RRP Stuk", "Retail Price Piece EUR"],
    "RRP GB EUR":            ["RRP Giftbox EUR", "RRP GB", "Retail Price GB EUR"],
    "Inkoopprijs Stuk EUR":  ["Purchase Price Piece EUR", "Cost Price Piece EUR", "Inkoopprijs Stuk"],
    "Inkoopprijs GB EUR":    ["Purchase Price GB EUR", "Cost Price GB EUR", "Inkoopprijs GB"],
    "Giftbox Quantity":      ["GB Quantity", "Giftbox Qty", "GB Qty"],
}


class IngestError(Exception):
    """Raised when ingest cannot proceed (file missing, columns missing, geen geldige producten)."""
    pass


@dataclass
class IngestResult:
    inserted_count: int
    warnings: list = field(default_factory=list)
    run_id: Optional[int] = None


def get_supabase():
    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def resolve_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {}
    for canonical, aliases in COLUMN_ALIASES.items():
        if canonical not in df.columns:
            for alias in aliases:
                if alias in df.columns:
                    rename_map[alias] = canonical
                    break
    return df.rename(columns=rename_map)


def validate_columns(df: pd.DataFrame) -> list:
    return [col for col in REQUIRED_COLUMNS if col not in df.columns]


def normalize_ean(value) -> str:
    if pd.isna(value) or str(value).strip() in ("", "nan"):
        return ""
    cleaned = re.sub(r"[^\d]", "", str(value).split(".")[0])
    return cleaned.zfill(13) if cleaned else ""


def normalize_decimal(value) -> float | None:
    if pd.isna(value) or str(value).strip() in ("", "nan"):
        return None
    try:
        return float(str(value).replace(",", "."))
    except ValueError:
        return None


def parse_int(value) -> int | None:
    if pd.isna(value) or str(value).strip() in ("", "nan"):
        return None
    try:
        return int(float(str(value)))
    except ValueError:
        return None


def load_foto_export(path: str, logger: Callable[[str], None] = print) -> dict:
    """
    Laad foto-export Excel. Geeft {sku: {photo_packshot_1..5, photo_lifestyle_1..5}}.
    Verwachte kolomnamen: SKU, Packshot 1..5, Lifestyle 1..5
    """
    df = pd.read_excel(path, dtype=str)
    foto_map = {}
    col_sku = next((c for c in ["SKU", "Variant SKU", "brand_id"] if c in df.columns), None)
    if not col_sku:
        logger("  WAARSCHUWING: geen SKU-kolom gevonden in foto-export, foto's overgeslagen.")
        return {}

    for _, row in df.iterrows():
        sku = str(row.get(col_sku, "") or "").strip()
        if not sku or sku == "nan":
            continue
        entry = {}
        for i in range(1, 6):
            for prefix, key_prefix in [("Packshot", "photo_packshot"), ("Lifestyle", "photo_lifestyle")]:
                col = f"{prefix} {i}"
                val = str(row.get(col, "") or "").strip()
                entry[f"{key_prefix}_{i}"] = val if val != "nan" else ""
        foto_map[sku] = entry

    return foto_map


def build_product_row(row: pd.Series, fase: str, foto_map: dict) -> tuple[dict, list]:
    """Bouw een product-rij. Geeft (rij, warnings)."""
    warnings = []
    sku = str(row.get("SKU", "") or "").strip()

    ean_giftbox = normalize_ean(row.get("EAN Packaging/Giftbox", ""))
    ean_piece   = normalize_ean(row.get("EAN Piece", ""))

    if ean_giftbox:
        ean_shopify = ean_giftbox
    elif ean_piece:
        ean_shopify = ean_piece
        warnings.append(f"EAN Packaging/Giftbox leeg — EAN Piece als fallback (SKU: {sku})")
    else:
        ean_shopify = ""
        warnings.append(f"Geen EAN gevonden (SKU: {sku})")

    fotos = foto_map.get(sku, {})

    return {
        "sku":                   sku,
        "ean_shopify":           ean_shopify,
        "ean_piece":             ean_piece,
        "product_name_raw":      str(row.get("Product Name", "") or "").strip(),
        "designer":              str(row.get("Designer", "") or "").strip(),
        "kleur_en":              str(row.get("Color", "") or "").strip(),
        "materiaal_nl":          str(row.get("Product Material", "") or "").strip(),
        "leverancier_category":  str(row.get("Category", "") or "").strip(),
        "leverancier_item_cat":  str(row.get("Item Category", "") or "").strip(),
        "hoogte_cm":             normalize_decimal(row.get("Height", row.get("Hoogte", ""))),
        "lengte_cm":             normalize_decimal(row.get("Length", row.get("Lengte", ""))),
        "breedte_cm":            normalize_decimal(row.get("Width", row.get("Breedte", ""))),
        "giftbox":               str(row.get("Giftbox", "") or "").strip().upper(),
        "giftbox_qty":           parse_int(row.get("Giftbox Quantity")),
        "rrp_stuk_eur":          normalize_decimal(row.get("RRP Stuk EUR")),
        "rrp_gb_eur":            normalize_decimal(row.get("RRP GB EUR")),
        "inkoopprijs_stuk_eur":  normalize_decimal(row.get("Inkoopprijs Stuk EUR")),
        "inkoopprijs_gb_eur":    normalize_decimal(row.get("Inkoopprijs GB EUR")),
        "photo_packshot_1":      fotos.get("photo_packshot_1", ""),
        "photo_packshot_2":      fotos.get("photo_packshot_2", ""),
        "photo_packshot_3":      fotos.get("photo_packshot_3", ""),
        "photo_packshot_4":      fotos.get("photo_packshot_4", ""),
        "photo_packshot_5":      fotos.get("photo_packshot_5", ""),
        "photo_lifestyle_1":     fotos.get("photo_lifestyle_1", ""),
        "photo_lifestyle_2":     fotos.get("photo_lifestyle_2", ""),
        "photo_lifestyle_3":     fotos.get("photo_lifestyle_3", ""),
        "photo_lifestyle_4":     fotos.get("photo_lifestyle_4", ""),
        "photo_lifestyle_5":     fotos.get("photo_lifestyle_5", ""),
        "status":                "raw",
        "fase":                  str(fase),
    }, warnings


def ingest_masterdata(
    file_path: str,
    fase: str,
    fotos_path: str | None = None,
    progress: Callable[[int, int, str], None] | None = None,
    logger: Callable[[str], None] | None = None,
) -> IngestResult:
    """Pure-function variant: raises IngestError on failure, returns IngestResult on success."""
    log = logger if logger is not None else print

    path = Path(file_path)
    if not path.exists():
        raise IngestError(f"Bestand niet gevonden: {file_path}")

    log(f"Inladen: {path.name} (fase {fase})")
    df = pd.read_excel(path, dtype=str)
    df = resolve_columns(df)

    missing = validate_columns(df)
    if missing:
        raise IngestError(f"Verplichte kolommen ontbreken: {missing}")

    total_rows = len(df)
    log(f"  {total_rows} rijen gevonden in Excel")

    foto_map = {}
    if fotos_path:
        foto_map = load_foto_export(fotos_path, logger=log)
        log(f"  Foto-export: {len(foto_map)} SKUs met foto's geladen")

    rows = []
    all_warnings = []

    for idx, (_, row) in enumerate(df.iterrows(), start=1):
        product, warnings = build_product_row(row, fase, foto_map)
        all_warnings.extend(warnings)
        if not product["ean_shopify"]:
            continue
        rows.append(product)

        if progress is not None and (idx % 100 == 0 or idx == total_rows):
            progress(idx, total_rows, f"Rij {idx}/{total_rows} verwerkt")

    # Dubbele EANs detecteren
    eans = [r["ean_shopify"] for r in rows]
    duplicates = {e for e in eans if eans.count(e) > 1}
    if duplicates:
        all_warnings.append(f"Dubbele EANs in dit bestand: {duplicates} — controleer handmatig")

    if all_warnings:
        log(f"\n  Waarschuwingen ({len(all_warnings)}):")
        for w in all_warnings:
            log(f"    - {w}")

    if not rows:
        raise IngestError("Geen geldige producten om te laden.")

    log(f"\nUploaden naar Supabase ({len(rows)} producten)...")
    sb = get_supabase()

    total_batches = (len(rows) + 99) // 100
    for i in range(0, len(rows), 100):
        batch = rows[i:i + 100]
        sb.table("seo_products").insert(batch).execute()
        batch_num = i // 100 + 1
        log(f"  Batch {batch_num}: {len(batch)} producten geladen")
        if progress is not None:
            progress(batch_num, total_batches, f"Batch {batch_num}/{total_batches} geüpload")

    run_insert = sb.table("seo_import_runs").insert({
        "bestandsnaam":     path.name,
        "fase":             str(fase),
        "aantal_producten": len(rows),
        "aantal_warnings":  len(all_warnings),
        "fouten":           "; ".join(all_warnings) if all_warnings else None,
    }).execute()

    run_id = None
    try:
        if run_insert.data:
            run_id = run_insert.data[0].get("id")
    except Exception:
        run_id = None

    log(f"\nKlaar: {len(rows)} producten geladen, {len(all_warnings)} waarschuwingen")
    if all_warnings:
        log("Controleer de waarschuwingen hierboven voor je doorgaat.")

    return IngestResult(inserted_count=len(rows), warnings=all_warnings, run_id=run_id)


def ingest(file_path: str, fase: str, fotos_path: str | None):
    """Backwards-compatible CLI wrapper: keeps old sys.exit-on-error behavior."""
    try:
        ingest_masterdata(file_path, fase, fotos_path, progress=None, logger=print)
    except IngestError as e:
        print(f"FOUT: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file",  required=True, help="Pad naar leveranciers Excel")
    parser.add_argument("--fase",  required=True, help="Fasecode, bijv. 3")
    parser.add_argument("--fotos", help="Pad naar foto-export Excel (optioneel)")
    args = parser.parse_args()

    ingest(args.file, args.fase, args.fotos)
