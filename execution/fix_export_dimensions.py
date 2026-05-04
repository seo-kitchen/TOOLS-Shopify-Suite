"""
Fix afmetingen in een Serax export Excel-bestand.

Haalt lengte_cm / breedte_cm / hoogte_cm uit het Serax masterbestand
en overschrijft de (verkeerde) waarden in de exportfile.

Gebruik:
    python execution/fix_export_dimensions.py
"""

import re
import sys
from pathlib import Path

import pandas as pd

MASTER_FILE = "Master Files/Masterdata serax new items_2026_Interieur-Shop (2).xlsx"
EXPORT_FILE = "exports/Serax_86_DEFINITIEF_v2_20260423_1212.xlsx"
OUTPUT_FILE = "exports/Serax_86_DEFINITIEF_v3_afmetingen_fixed.xlsx"

DIM_PATTERN = re.compile(r"\b([LlWwHh])\s*([\d]+(?:[.,]\d+)?)", re.IGNORECASE)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def parse_dim(s: str) -> dict:
    if not s or not isinstance(s, str) or s.strip().lower() in {"nan", "n/a", "-", ""}:
        return {}
    result = {}
    for m in DIM_PATTERN.finditer(s):
        letter = m.group(1).upper()
        value = float(m.group(2).replace(",", "."))
        if letter == "L":
            result["lengte_cm"] = value
        elif letter == "W":
            result["breedte_cm"] = value
        elif letter == "H":
            result["hoogte_cm"] = value
    return result


def main():
    master_path = Path(MASTER_FILE)
    export_path = Path(EXPORT_FILE)

    if not master_path.exists():
        print(f"FOUT: masterbestand niet gevonden: {master_path}", file=sys.stderr)
        sys.exit(1)
    if not export_path.exists():
        print(f"FOUT: exportbestand niet gevonden: {export_path}", file=sys.stderr)
        sys.exit(1)

    df_md = pd.read_excel(master_path, header=1, dtype=str)
    print(f"Master ingelezen: {len(df_md)} rijen")

    # Bouw dimensie lookup
    md_dims: dict[str, dict] = {}
    for _, row in df_md.iterrows():
        sku = str(row.get("Brand_id") or "").strip()
        if not sku or sku == "nan":
            continue
        raw = str(row.get("Product dimensions cm", "") or "")
        dims = parse_dim(raw)
        if dims:
            md_dims[sku] = dims

    print(f"Dimensies geladen voor {len(md_dims)} SKUs")

    df_exp = pd.read_excel(export_path, dtype=str)
    print(f"Export ingelezen: {len(df_exp)} rijen")

    updated = 0
    not_found = 0
    no_dim_in_master = 0

    print("\nVerificatie (eerste 5 rijen):")
    print(f"{'SKU':<20} {'OUD hoogte':>12} {'OUD lengte':>12} {'OUD breedte':>12} | {'NIEUW hoogte':>13} {'NIEUW lengte':>13} {'NIEUW breedte':>13}")
    print("-" * 102)

    for idx, row in df_exp.iterrows():
        sku = str(row.get("Variant SKU", "")).strip()
        if not sku or sku == "nan":
            continue

        dims = md_dims.get(sku)
        if dims is None:
            not_found += 1
            continue

        if not dims:
            no_dim_in_master += 1
            continue

        old_h = df_exp.at[idx, "hoogte_cm"]
        old_l = df_exp.at[idx, "lengte_cm"]
        old_b = df_exp.at[idx, "breedte_cm"]

        df_exp.at[idx, "hoogte_cm"] = dims.get("hoogte_cm", "")
        df_exp.at[idx, "lengte_cm"] = dims.get("lengte_cm", "")
        df_exp.at[idx, "breedte_cm"] = dims.get("breedte_cm", "")
        updated += 1

        if idx < 5:
            print(
                f"{sku:<20} {str(old_h):>12} {str(old_l):>12} {str(old_b):>12} | "
                f"{str(dims.get('hoogte_cm','')):>13} {str(dims.get('lengte_cm','')):>13} {str(dims.get('breedte_cm','')):>13}"
            )

    print(f"\nResultaat:")
    print(f"  Bijgewerkt:           {updated}")
    print(f"  Niet in master:       {not_found}")
    print(f"  Geen dim in master:   {no_dim_in_master}")

    output_path = Path(OUTPUT_FILE)
    df_exp.to_excel(output_path, index=False)
    print(f"\nOpgeslagen als: {output_path}")


if __name__ == "__main__":
    main()
