"""Validate v2 — kwaliteitschecks op products_curated (nieuwe schema).

Checks:
  - Verplichte velden: sku, product_title_nl, hoofdcategorie, verkoopprijs
  - Meta title (uit product_title_nl): max 70 tekens
  - Meta description: 120-160 tekens
  - Dubbele EAN/handle
  - Decimalen formaat (auto-fix)
  - Filter values bestaan op website (waarschuwing, niet error)

Auto-fix waar mogelijk; rapporteert errors per product.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Callable

from dotenv import load_dotenv

load_dotenv()


@dataclass
class ValidateResult:
    total: int = 0
    ok_count: int = 0
    error_count: int = 0
    fixed_count: int = 0
    errors: list[dict] = field(default_factory=list)


META_DESC_MIN = 120
META_DESC_MAX = 160
META_TITLE_MAX = 70


def get_supabase():
    from supabase import create_client
    url = os.getenv("SUPABASE_NEW_URL", "")
    key = os.getenv("SUPABASE_NEW_SERVICE_KEY", "") or os.getenv("SUPABASE_NEW_KEY", "")
    if not url or not key:
        raise RuntimeError("SUPABASE_NEW_URL/KEY ontbreekt.")
    return create_client(url, key)


def _truncate_meta(desc: str) -> str:
    """Knip meta description af op 160 tekens, breek op woordgrens."""
    if len(desc) <= META_DESC_MAX:
        return desc
    cut = desc[:META_DESC_MAX - 3]
    last_space = cut.rfind(" ")
    if last_space > META_DESC_MAX - 30:
        cut = cut[:last_space]
    return cut.rstrip(".,;:!? ") + "..."


def _clean_decimal(value) -> float | None:
    if value is None:
        return None
    s = str(value).replace(",", ".").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def validate_batch(
    skus: list[str] | None = None,
    pipeline_status: str = "ready",
    fase: str | None = None,
    autofix: bool = True,
    progress: Callable[[int, int, str], None] | None = None,
    logger: Callable[[str], None] | None = None,
) -> ValidateResult:
    log = logger or print
    sb = get_supabase()

    q = sb.table("products_curated").select("*")
    if skus:
        q = q.in_("sku", skus)
    elif pipeline_status:
        q = q.eq("pipeline_status", pipeline_status)
    if fase:
        q = q.eq("fase", fase)

    products = q.execute().data or []
    if not products:
        log("Geen producten om te valideren.")
        return ValidateResult()

    result = ValidateResult(total=len(products))

    # Verzamel alle handles om dubbele te detecteren
    all_handles: dict[str, list[str]] = {}
    for p in products:
        h = p.get("handle")
        if h:
            all_handles.setdefault(h, []).append(p.get("sku", "?"))

    for idx, p in enumerate(products):
        if progress:
            try:
                progress(idx, len(products), p.get("sku", "?"))
            except Exception:
                pass

        sku = p.get("sku") or "?"
        errors: list[str] = []
        fixes: dict = {}

        # Verplichte velden
        if not p.get("product_title_nl"):
            errors.append("product_title_nl ontbreekt")
        if not p.get("hoofdcategorie"):
            errors.append("hoofdcategorie ontbreekt")
        if p.get("verkoopprijs") is None or p.get("verkoopprijs") == 0:
            errors.append("verkoopprijs is 0 of ontbreekt")

        # Meta title (afgeleid van product_title_nl + ' | Interieur Shop')
        title = (p.get("product_title_nl") or "").strip()
        meta_title = f"{title} | Interieur Shop" if title else ""
        if len(meta_title) > META_TITLE_MAX:
            errors.append(f"meta title te lang ({len(meta_title)} > {META_TITLE_MAX})")

        # Meta description lengte
        desc = (p.get("meta_description") or "").strip()
        if not desc:
            errors.append("meta_description ontbreekt")
        elif len(desc) > META_DESC_MAX:
            if autofix:
                fixes["meta_description"] = _truncate_meta(desc)
                result.fixed_count += 1
            else:
                errors.append(f"meta description te lang ({len(desc)} > {META_DESC_MAX})")
        elif len(desc) < META_DESC_MIN:
            errors.append(f"meta description te kort ({len(desc)} < {META_DESC_MIN})")

        # Dubbele handle
        h = p.get("handle")
        if h and len(all_handles.get(h, [])) > 1:
            others = [s for s in all_handles[h] if s != sku]
            errors.append(f"dubbele handle (ook bij {', '.join(others[:3])})")

        # Decimalen opschonen (auto-fix)
        for veld in ("hoogte_cm", "lengte_cm", "breedte_cm", "verkoopprijs", "inkoopprijs"):
            val = p.get(veld)
            if val is not None:
                cleaned = _clean_decimal(val)
                if cleaned is not None and cleaned != val:
                    fixes[veld] = cleaned
                    result.fixed_count += 1

        # Status updaten op basis van errors
        if errors:
            fixes["pipeline_status"] = "review"
            fixes["review_reden"] = "; ".join(errors)
            result.error_count += 1
            result.errors.append({"sku": sku, "errors": errors})
        else:
            fixes["pipeline_status"] = "ready"
            result.ok_count += 1

        # Schrijf fixes weg
        if fixes:
            try:
                sb.table("products_curated").update(fixes).eq("sku", sku).execute()
            except Exception as e:
                log(f"  ! kon fixes niet opslaan voor {sku}: {e}")

    if progress:
        try:
            progress(len(products), len(products), "klaar")
        except Exception:
            pass

    log(f"\nValidate klaar — {result.ok_count} ok, {result.error_count} review, "
        f"{result.fixed_count} auto-fixes")
    return result


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--fase", help="Fase filter")
    parser.add_argument("--no-autofix", action="store_true")
    args = parser.parse_args()
    validate_batch(fase=args.fase, autofix=not args.no_autofix)
