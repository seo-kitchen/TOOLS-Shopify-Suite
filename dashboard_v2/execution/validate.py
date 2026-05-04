"""
Stap 4: Kwaliteitscheck + validatie tegen website-structuur.
Zie directives/validate.md voor volledige instructies.

Gebruik:
    python execution/validate.py --fase 3
"""

import argparse
import csv
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

REQUIRED_FIELDS = ["product_title_nl", "ean_shopify", "hoofdcategorie", "verkoopprijs"]


class ValidateError(Exception):
    """Raised when validate cannot proceed."""
    pass


@dataclass
class ValidateResult:
    total: int
    ok: int
    review: int
    autofixed: int
    issues: list = field(default_factory=list)
    review_csv_path: Optional[str] = None


def get_supabase():
    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def clean_decimal(value) -> str | None:
    if value is None:
        return None
    import re
    s = str(value).replace(",", ".")
    try:
        f = float(s)
        return f"{f:.10f}".rstrip("0").rstrip(".")
    except ValueError:
        return s


def validate_fase(
    fase: str,
    ids: list[int] | None = None,
    autofix: bool = True,
    progress: Callable[[int, int, str], None] | None = None,
    logger: Callable[[str], None] | None = None,
) -> ValidateResult:
    """Pure-function variant. ids=None validates all in fase; ids=[...] limits to those product ids."""
    log = logger if logger is not None else print

    sb = get_supabase()

    query = sb.table("seo_products").select("*").eq("fase", fase).in_(
        "status", ["ready", "review"]
    )
    if ids is not None:
        if not ids:
            log(f"Geen ids meegegeven — niets te valideren.")
            return ValidateResult(total=0, ok=0, review=0, autofixed=0, issues=[], review_csv_path=None)
        query = query.in_("id", ids)

    result = query.execute()
    products = result.data

    if not products:
        log(f"Geen producten gevonden voor fase {fase}.")
        return ValidateResult(total=0, ok=0, review=0, autofixed=0, issues=[], review_csv_path=None)

    total = len(products)
    log(f"Valideren: {total} producten (fase {fase})\n")

    # Bestaande website-collecties ophalen
    coll_result  = sb.table("seo_website_collections").select("naam").execute()
    bekende_cats = {r["naam"] for r in coll_result.data}

    # Bestaande filterwaarden ophalen
    filter_result  = sb.table("seo_filter_values").select("type,waarde").execute()
    filter_values  = {(r["type"], r["waarde"]) for r in filter_result.data}

    # Dubbele EANs detecteren binnen deze fase
    eans = [p["ean_shopify"] for p in products if p.get("ean_shopify")]
    duplicate_eans = {e for e in eans if eans.count(e) > 1}

    review_rows    = []
    all_issues     = []
    warnings       = []
    auto_fixed     = 0

    for idx, product in enumerate(products, start=1):
        pid    = product["id"]
        sku    = product.get("sku", pid)
        issues = []
        updates = {}
        set_review = False

        # 1. Dubbele EAN
        if product.get("ean_shopify") in duplicate_eans:
            issues.append("dubbele EAN binnen deze fase")
            set_review = True

        # 2. Verplichte velden
        for field_name in REQUIRED_FIELDS:
            val = product.get(field_name)
            if val is None or str(val).strip() in ("", "None", "nan"):
                issues.append(f"leeg verplicht veld: {field_name}")
                set_review = True

        # 3. Afmetingen decimaal (auto-fix komma -> punt)
        for dim in ["hoogte_cm", "lengte_cm", "breedte_cm"]:
            val = product.get(dim)
            if val is not None:
                cleaned = clean_decimal(val)
                if cleaned and str(val) != cleaned:
                    if autofix:
                        updates[dim] = float(cleaned)
                    auto_fixed += 1

        # 4. Afmetingen nul of leeg (waarschuwing, geen blokkade)
        for dim in ["hoogte_cm", "lengte_cm", "breedte_cm"]:
            val = product.get(dim)
            if not val or float(val or 0) == 0:
                warnings.append(f"SKU {sku}: {dim} ontbreekt of is 0")

        # 5. Meta description te lang (auto-truncate)
        meta = product.get("meta_description") or ""
        if len(meta) > 160:
            if autofix:
                updates["meta_description"] = meta[:160]
            auto_fixed += 1

        # 6. Prijs <= 0
        prijs = product.get("verkoopprijs")
        if prijs is not None and float(prijs or 0) <= 0:
            issues.append("verkoopprijs is 0 of negatief")
            set_review = True

        # 7. Tags leeg
        if not product.get("tags"):
            issues.append("tags ontbreken")
            warnings.append(f"SKU {sku}: tags zijn leeg")

        # 8. Validatie categorie tegen website-structuur
        hoofdcat = product.get("hoofdcategorie") or ""
        if hoofdcat and bekende_cats and hoofdcat not in bekende_cats:
            issues.append(f"categorie '{hoofdcat}' bestaat nog niet op de website — eerst aanmaken in Shopify")
            warnings.append(f"SKU {sku}: nieuwe categorie '{hoofdcat}'")

        # 9. Validatie filterwaarden tegen website
        for ftype, veld in [("kleur", "kleur_nl"), ("materiaal", "materiaal_nl")]:
            waarde = product.get(veld) or ""
            if waarde and filter_values and (ftype, waarde) not in filter_values:
                warnings.append(f"SKU {sku}: nieuwe {ftype}-filterwaarde '{waarde}' — aanmaken in Shopify")

        # 10. Geen foto's
        has_foto = any(product.get(f"photo_packshot_{i}") for i in range(1, 6))
        if not has_foto:
            warnings.append(f"SKU {sku}: geen foto-URLs")

        # Status toepassen
        if set_review:
            updates["status"] = "review"
        elif product["status"] == "review" and not set_review and not issues:
            updates["status"] = "ready"

        if updates:
            sb.table("seo_products").update(updates).eq("id", pid).execute()

        if issues:
            row = {
                "sku":    sku,
                "ean":    product.get("ean_shopify", ""),
                "status": updates.get("status", product["status"]),
                "issues": "; ".join(issues),
            }
            review_rows.append(row)
            all_issues.append({"id": pid, **row})

        if progress is not None and (idx % 100 == 0 or idx == total):
            progress(idx, total, f"Product {idx}/{total} gevalideerd")

    # Review CSV schrijven
    review_path = Path(f".tmp/review_fase{fase}.csv")
    review_path.parent.mkdir(exist_ok=True)
    with open(review_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["sku", "ean", "status", "issues"])
        writer.writeheader()
        writer.writerows(review_rows)

    # Finale telling
    result2 = sb.table("seo_products").select("status").eq("fase", fase).execute()
    counts  = {}
    for row in result2.data:
        counts[row["status"]] = counts.get(row["status"], 0) + 1

    ready_count  = counts.get("ready", 0)
    review_count = counts.get("review", 0)

    log(f"  + {ready_count} producten klaar voor export")
    log(f"  ~ {review_count} producten voor handmatige controle -> {review_path}")
    if auto_fixed:
        log(f"  * {auto_fixed} velden automatisch gecorrigeerd")
    if warnings:
        log(f"\n  Waarschuwingen ({len(warnings)}):")
        for w in warnings[:20]:
            log(f"    - {w}")
        if len(warnings) > 20:
            log(f"    ... en {len(warnings) - 20} meer (zie review CSV)")

    if review_count > 0:
        log(f"\n  Open {review_path} en corrigeer de problemen voor je exporteert.")
    else:
        log(f"\n  Klaar voor export. Draai: python execution/export.py --fase {fase}")

    return ValidateResult(
        total=total,
        ok=ready_count,
        review=review_count,
        autofixed=auto_fixed,
        issues=all_issues,
        review_csv_path=str(review_path),
    )


def validate(fase: str):
    """Backwards-compatible CLI wrapper."""
    try:
        validate_fase(fase, ids=None, autofix=True, progress=None, logger=print)
    except ValidateError as e:
        print(f"FOUT: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fase", required=True, help="Fasecode, bijv. 3")
    args = parser.parse_args()

    validate(args.fase)
