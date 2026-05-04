"""
Eenmalig retroactief script: parse de 'Product dimensions cm' kolom uit het
Serax masterbestand en update lengte_cm / breedte_cm / hoogte_cm in seo_products.

De Serax dimensions staan in één gecombineerde string zoals:
    "L 13,8 W 13,8 H 7 "
    "L 8 W 5,8 H 7,7 "
    "L 12,5 W 8,2 H 9,4 "

Deze parser haalt L/W/H (case-insensitive) eruit, accepteert komma OF punt
als decimaalteken, en update de records in Supabase op SKU match.

Gebruik:
    python execution/fix_serax_dimensions.py

Bevat ook een pure-Python entrypoint `fix_serax_dimensions(...)` voor
gebruik vanuit de Streamlit dashboard.
"""

import argparse
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Forceer UTF-8 stdout op Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

SERAX_FILE = "Master Files/Masterdata serax new items_2026_Interieur-Shop (1).xlsx"

# Pattern: vind 'L 13,8' / 'W 13.8' / 'H 7' (case insensitive)
DIM_PATTERN = re.compile(
    r"\b([LlWwHh])\s*([\d]+(?:[.,]\d+)?)",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Result types & exceptions
# ---------------------------------------------------------------------------


class DimError(Exception):
    """Raised when the dimensions pipeline cannot proceed."""


@dataclass
class DimResult:
    parsed_count: int = 0
    updated_count: int = 0
    failed_parse: list[str] = field(default_factory=list)
    not_found_in_db: int = 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def parse_dimensions(s: str) -> dict:
    """
    Parseert "L 13,8 W 13,8 H 7" -> {"lengte_cm": 13.8, "breedte_cm": 13.8, "hoogte_cm": 7}
    Geeft alleen de gevonden velden terug. None of leeg -> leeg dict.
    """
    if not s or not isinstance(s, str):
        return {}
    s = s.strip()
    if not s or s.lower() in {"nan", "n/a", "-"}:
        return {}

    result = {}
    for match in DIM_PATTERN.finditer(s):
        letter = match.group(1).upper()
        value = match.group(2).replace(",", ".")
        try:
            num = float(value)
        except ValueError:
            continue
        if letter == "L":
            result["lengte_cm"] = num
        elif letter == "W":
            result["breedte_cm"] = num
        elif letter == "H":
            result["hoogte_cm"] = num
    return result


# ---------------------------------------------------------------------------
# Pure-function entrypoint
# ---------------------------------------------------------------------------


def fix_serax_dimensions(
    file_path: str | None = None,
    progress: Callable[[int, int, str], None] | None = None,
    logger: Callable[[str], None] | None = None,
) -> DimResult:
    """Parse Serax dimensions en update Supabase.

    Bij `file_path=None` wordt de hardcoded default (SERAX_FILE) gebruikt.
    """
    log = logger or (lambda _msg: None)

    def _progress(step: int, total: int, msg: str) -> None:
        if progress:
            progress(step, total, msg)

    pad = Path(file_path) if file_path else Path(SERAX_FILE)
    if not pad.exists():
        raise DimError(f"Masterbestand niet gevonden: {pad}")

    sys.path.insert(0, str(Path(__file__).parent))
    try:
        from setup_masterdata import detecteer_header_rij
        header_rij = detecteer_header_rij(str(pad))
    except Exception as e:
        raise DimError(f"Kan header rij niet detecteren: {e}") from e

    try:
        df = pd.read_excel(pad, header=header_rij, dtype=str)
    except Exception as e:
        raise DimError(f"Excel lezen mislukt: {e}") from e

    log(f"Serax masterbestand: {len(df)} rijen, {len(df.columns)} kolommen (header rij {header_rij})")

    if "Brand_id" not in df.columns:
        raise DimError("Kolom 'Brand_id' niet gevonden")
    if "Product dimensions cm" not in df.columns:
        raise DimError("Kolom 'Product dimensions cm' niet gevonden")

    result = DimResult()
    updates: list[tuple[str, dict]] = []
    no_dim = 0

    total_rows = len(df)
    for i, (_, row) in enumerate(df.iterrows()):
        sku = str(row.get("Brand_id") or "").strip()
        if not sku or sku.lower() == "nan":
            continue

        raw = row.get("Product dimensions cm")
        if not raw or pd.isna(raw):
            no_dim += 1
            continue

        dims = parse_dimensions(raw)
        if not dims:
            result.failed_parse.append(sku)
            continue

        result.parsed_count += 1
        updates.append((sku, dims))

        if (i + 1) % 100 == 0:
            _progress(i + 1, total_rows, f"parsed {result.parsed_count}")

    log(f"Parse: {result.parsed_count} ok, {len(result.failed_parse)} fail, "
        f"{no_dim} zonder dimensions")

    if not updates:
        log("Niets te updaten.")
        return result

    from supabase import create_client
    try:
        sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
    except Exception as e:
        raise DimError(f"Supabase verbinden mislukt: {e}") from e

    log(f"Updaten in seo_products: {len(updates)} records...")
    total = len(updates)
    for i, (sku, dims) in enumerate(updates):
        try:
            res = sb.table("seo_products").update(dims).eq("sku", sku).execute()
            if res.data:
                result.updated_count += len(res.data)
            else:
                result.not_found_in_db += 1
        except Exception as e:
            log(f"  UPDATE FAIL {sku}: {e}")
            result.not_found_in_db += 1

        if (i + 1) % 50 == 0:
            _progress(i + 1, total, f"updated {result.updated_count}")

    _progress(total, total, f"done updated={result.updated_count}")
    log(f"Klaar: {result.updated_count} geüpdatet, "
        f"{result.not_found_in_db} niet in DB, "
        f"{len(result.failed_parse)} parse-fails")
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", help="Pad naar Serax masterbestand (default hardcoded pad)")
    args = ap.parse_args()

    try:
        result = fix_serax_dimensions(
            file_path=args.file,
            logger=lambda m: print(m),
        )
    except DimError as e:
        print(f"FOUT: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"\nParse-resultaten:")
    print(f"  Geparsed:        {result.parsed_count}")
    print(f"  Parse fail:      {len(result.failed_parse)}")
    print(f"  Te updaten:      {result.parsed_count}")
    print(f"\nGeüpdatet:           {result.updated_count}")
    print(f"Niet gevonden in DB: {result.not_found_in_db}")
    if result.failed_parse[:10]:
        print("\nEerste 10 parse fails:")
        for sku in result.failed_parse[:10]:
            print(f"  - {sku}")


if __name__ == "__main__":
    main()
