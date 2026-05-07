"""Ingest v2 — Excel upload naar products_raw (nieuwe schema).

Auto-detect kolomindeling per leverancier (Serax/Pottery Pots/Printworks/S&P).
Schrijft naar products_raw met fase + supplier.

Geen wijzigingen aan products_curated — die blijft intact.
Producten zonder bestaande curated worden later getransformeerd.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Callable

import pandas as pd
from dotenv import load_dotenv

load_dotenv()


@dataclass
class IngestResult:
    inserted_count: int = 0
    updated_count: int = 0
    skipped_count: int = 0
    warnings: list[str] = field(default_factory=list)


# ── Kolommen normalisatie per leverancier ──
# Mapping van mogelijke Excel-kolomnamen → ons schema veld
COLUMN_ALIASES = {
    "sku": ["sku", "artikelnummer", "art.nr", "artikel", "item code", "product code"],
    "ean_piece": ["ean", "ean piece", "ean_piece", "barcode", "ean stuk"],
    "ean_shopify": ["ean shopify", "ean_shopify", "shopify ean", "ean per stuk", "barcode"],
    "product_name_raw": ["product name", "name", "productnaam", "product_name", "title", "naam"],
    "supplier": ["supplier", "merk", "brand", "vendor", "leverancier"],
    "designer": ["designer", "ontwerper"],
    "kleur_en": ["color", "colour", "kleur", "kleur en"],
    "materiaal_raw": ["material", "materiaal", "material raw"],
    "hoogte_cm": ["height", "hoogte", "h", "h (cm)", "hoogte_cm"],
    "lengte_cm": ["length", "lengte", "l", "l (cm)", "lengte_cm", "depth", "diepte"],
    "breedte_cm": ["width", "breedte", "b", "b (cm)", "breedte_cm", "diameter"],
    "rrp_stuk_eur": ["rrp", "rrp stuk", "rrp eur", "verkoopprijs", "retail price", "price", "prijs"],
    "rrp_gb_eur": ["rrp gb", "rrp giftbox", "rrp gb eur", "giftbox price"],
    "inkoopprijs_stuk_eur": ["inkoopprijs", "cost", "wholesale", "purchase price"],
    "inkoopprijs_gb_eur": ["inkoopprijs gb", "inkoop giftbox", "cost giftbox"],
    "leverancier_category": ["category", "categorie", "leverancier_category", "main category"],
    "leverancier_item_cat": ["item category", "item cat", "leverancier_item_cat", "subcategory", "type"],
    "giftbox": ["giftbox", "set", "is set"],
    "giftbox_qty": ["qty", "quantity", "giftbox qty", "stuks", "aantal in set"],
    "photo_packshot_1": ["photo 1", "photo_1", "image 1", "packshot 1", "photo_packshot_1"],
    "photo_packshot_2": ["photo 2", "photo_2", "image 2", "packshot 2", "photo_packshot_2"],
    "photo_packshot_3": ["photo 3", "photo_3", "image 3", "packshot 3", "photo_packshot_3"],
    "photo_packshot_4": ["photo 4", "photo_4", "image 4", "packshot 4", "photo_packshot_4"],
    "photo_packshot_5": ["photo 5", "photo_5", "image 5", "packshot 5", "photo_packshot_5"],
    "photo_lifestyle_1": ["lifestyle 1", "lifestyle_1", "photo_lifestyle_1"],
    "photo_lifestyle_2": ["lifestyle 2", "lifestyle_2", "photo_lifestyle_2"],
    "photo_lifestyle_3": ["lifestyle 3", "lifestyle_3", "photo_lifestyle_3"],
    "photo_lifestyle_4": ["lifestyle 4", "lifestyle_4", "photo_lifestyle_4"],
    "photo_lifestyle_5": ["lifestyle 5", "lifestyle_5", "photo_lifestyle_5"],
}


def _normalize_col(s: str) -> str:
    return str(s).strip().lower().replace("_", " ").replace("-", " ")


def _detect_columns(df_columns: list[str]) -> dict[str, str]:
    """Mapping: ons-veld → Excel-kolomnaam."""
    norm_cols = {col: _normalize_col(col) for col in df_columns}
    mapping: dict[str, str] = {}
    for veld, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            alias_norm = _normalize_col(alias)
            for orig, norm in norm_cols.items():
                if norm == alias_norm and veld not in mapping:
                    mapping[veld] = orig
                    break
            if veld in mapping:
                break
    return mapping


def get_supabase():
    from supabase import create_client
    url = os.getenv("SUPABASE_NEW_URL", "")
    key = os.getenv("SUPABASE_NEW_SERVICE_KEY", "") or os.getenv("SUPABASE_NEW_KEY", "")
    if not url or not key:
        raise RuntimeError("SUPABASE_NEW_URL/KEY ontbreekt.")
    return create_client(url, key)


def _safe_str(val) -> str:
    if val is None or pd.isna(val):
        return ""
    s = str(val).strip()
    return "" if s.lower() in ("nan", "none", "null") else s


def _safe_float(val) -> float | None:
    if val is None or pd.isna(val):
        return None
    try:
        return float(str(val).replace(",", "."))
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> int | None:
    f = _safe_float(val)
    return int(f) if f is not None else None


def ingest_masterdata(
    file_path: str,
    fase: str,
    supplier: str = "",
    fotos_path: str | None = None,
    progress: Callable[[int, int, str], None] | None = None,
    logger: Callable[[str], None] | None = None,
) -> IngestResult:
    """Lees Excel → schrijf naar products_raw (upsert op sku)."""
    log = logger or print
    sb = get_supabase()
    result = IngestResult()

    df = pd.read_excel(file_path, dtype=str, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]
    log(f"Ingelezen: {len(df)} rijen, {len(df.columns)} kolommen")

    # Auto-detect kolommen
    col_map = _detect_columns(list(df.columns))
    log(f"Gedetecteerde kolommen: {list(col_map.keys())}")

    if "sku" not in col_map:
        result.warnings.append("Geen SKU-kolom gedetecteerd — kan niet ingestnen")
        return result
    if "product_name_raw" not in col_map:
        result.warnings.append("Geen productnaam-kolom gedetecteerd")

    # Foto's apart laden indien meegegeven
    fotos_by_sku: dict[str, dict] = {}
    if fotos_path:
        try:
            df_fotos = pd.read_excel(fotos_path, dtype=str, engine="openpyxl")
            df_fotos.columns = [str(c).strip() for c in df_fotos.columns]
            foto_col_map = _detect_columns(list(df_fotos.columns))
            sku_col = foto_col_map.get("sku")
            if sku_col:
                for _, row in df_fotos.iterrows():
                    sku = _safe_str(row[sku_col])
                    if sku:
                        fotos_by_sku[sku] = {
                            v: _safe_str(row[c]) for v, c in foto_col_map.items()
                            if v.startswith("photo_") and c in df_fotos.columns
                        }
                log(f"Foto-lookup geladen voor {len(fotos_by_sku)} SKUs")
        except Exception as e:
            log(f"Foto-Excel niet geladen: {e}")

    # Bestaande SKUs ophalen
    existing_skus: set[str] = set()
    try:
        page_size = 1000
        offset = 0
        while True:
            res = sb.table("products_raw").select("sku") \
                .range(offset, offset + page_size - 1).execute().data or []
            if not res:
                break
            existing_skus.update(r["sku"] for r in res if r.get("sku"))
            if len(res) < page_size:
                break
            offset += page_size
    except Exception as e:
        log(f"Kon bestaande SKUs niet ophalen: {e}")

    # Per rij verwerken
    rows_to_insert = []
    rows_to_update = []
    n = len(df)

    for idx, row in df.iterrows():
        if progress and idx % 50 == 0:
            progress(idx, n, f"rij {idx}")

        sku = _safe_str(row[col_map["sku"]])
        if not sku:
            result.skipped_count += 1
            continue

        record: dict = {
            "sku": sku,
            "fase": fase,
            "supplier": supplier or _safe_str(row[col_map["supplier"]]) if "supplier" in col_map else supplier,
            "import_batch": f"fase{fase}_{os.path.basename(file_path)}",
        }

        # Strings
        for veld in ("ean_piece", "ean_shopify", "product_name_raw",
                     "designer", "kleur_en", "materiaal_raw",
                     "leverancier_category", "leverancier_item_cat", "giftbox"):
            if veld in col_map:
                v = _safe_str(row[col_map[veld]])
                if v:
                    record[veld] = v

        # Numbers
        for veld in ("hoogte_cm", "lengte_cm", "breedte_cm",
                     "rrp_stuk_eur", "rrp_gb_eur",
                     "inkoopprijs_stuk_eur", "inkoopprijs_gb_eur"):
            if veld in col_map:
                v = _safe_float(row[col_map[veld]])
                if v is not None:
                    record[veld] = v

        if "giftbox_qty" in col_map:
            v = _safe_int(row[col_map["giftbox_qty"]])
            if v is not None:
                record["giftbox_qty"] = v

        # Foto's: eerst uit hoofd-Excel, dan uit foto-lookup
        for i in range(1, 6):
            for soort in ("packshot", "lifestyle"):
                veld = f"photo_{soort}_{i}"
                if veld in col_map:
                    v = _safe_str(row[col_map[veld]])
                    if v:
                        record[veld] = v
                if fotos_by_sku.get(sku, {}).get(veld):
                    record.setdefault(veld, fotos_by_sku[sku][veld])

        if sku in existing_skus:
            rows_to_update.append(record)
        else:
            rows_to_insert.append(record)

    # Bulk inserts (chunks van 100)
    log(f"Inserting {len(rows_to_insert)} nieuwe, updating {len(rows_to_update)} bestaande...")
    for i in range(0, len(rows_to_insert), 100):
        chunk = rows_to_insert[i:i + 100]
        try:
            sb.table("products_raw").insert(chunk).execute()
            result.inserted_count += len(chunk)
        except Exception as e:
            log(f"Insert-chunk fout: {e}")
            result.warnings.append(f"Insert chunk {i}: {e}")

    for r in rows_to_update:
        try:
            sb.table("products_raw").update(r).eq("sku", r["sku"]).execute()
            result.updated_count += 1
        except Exception as e:
            log(f"Update {r['sku']} fout: {e}")

    if progress:
        progress(n, n, "klaar")

    log(f"\nIngest klaar — {result.inserted_count} nieuw, {result.updated_count} updated, "
        f"{result.skipped_count} overgeslagen")
    return result


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="Pad naar Excel-bestand")
    parser.add_argument("--fase", required=True, help="Fase nummer")
    parser.add_argument("--supplier", default="", help="Merk")
    parser.add_argument("--fotos", help="Optioneel: pad naar foto-Excel")
    args = parser.parse_args()
    ingest_masterdata(args.file, fase=args.fase, supplier=args.supplier, fotos_path=args.fotos)
