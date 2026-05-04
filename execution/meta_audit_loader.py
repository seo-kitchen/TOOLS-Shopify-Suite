"""
meta_audit_loader.py — Laad actieve Shopify producten in `shopify_meta_audit`.

WHITELIST (hard): alleen deze kolommen uit de Excel worden gebruikt:
  - Product ID
  - Product handle
  - Product title
  - Product vendor
  - Product meta title
  - Product meta description

Geen enkele andere kolom wordt uitgelezen of geschreven. Dit is bewust —
we mogen GEEN eerder gecureerde data (prijzen, descriptions, tags) overschrijven.

Zie directives/meta_audit.md voor de volledige SOP.

Gebruik:
    python execution/meta_audit_loader.py --file "master files/Alle Active Producten.xlsx"
    python execution/meta_audit_loader.py --file "..." --dry-run   # laat eerste 10 rijen zien zonder upload
"""

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")

# Harde whitelist — ALLEEN deze kolommen.
ALLOWED_COLUMNS = [
    "Product ID",
    "Product handle",
    "Product title",
    "Product vendor",
    "Product meta title",
    "Product meta description",
]

TITLE_MAX = 58  # 2 chars safety marge onder Google's pixel-limiet
TITLE_MIN = 30
DESC_MAX = 155
DESC_MIN = 120

# ── Vendor normalisatie ─────────────────────────────────────────────────────
# Shopify-exports hebben soms inconsistent gespelde vendor-waarden.
# Normaliseer hier zodat 'Salt & Pepper' altijd correct getoond wordt.
# Key-lookup is case-sensitive; we trimmen en checken ook lowercase varianten.
VENDOR_NORMALIZE = {
    "sp": "Salt & Pepper",
    "SP": "Salt & Pepper",
    "S&P": "Salt & Pepper",
    "SP_COL": "Salt & Pepper",
    "sp_col": "Salt & Pepper",
}


def normalize_vendor(vendor: str) -> str:
    """Map vendor-varianten naar hun canonieke vorm."""
    if not vendor:
        return vendor
    v = vendor.strip()
    # Directe match
    if v in VENDOR_NORMALIZE:
        return VENDOR_NORMALIZE[v]
    # Case-insensitive fallback
    v_lower = v.lower()
    for key, canonical in VENDOR_NORMALIZE.items():
        if key.lower() == v_lower:
            return canonical
    return v


def get_supabase():
    from supabase import create_client
    if not SUPABASE_URL or not SUPABASE_KEY:
        sys.exit("FOUT: SUPABASE_URL / SUPABASE_KEY ontbreken in .env")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def clean_str(value) -> str:
    if pd.isna(value):
        return ""
    s = str(value).strip()
    return "" if s.lower() == "nan" else s


def audit_title(title: str, duplicate: bool) -> str:
    if not title:
        return "missing"
    n = len(title)
    if duplicate:
        return "duplicate"
    if n > TITLE_MAX:
        return "too_long"
    if n < TITLE_MIN:
        return "too_short"
    return "ok"


TEMPLATE_PATTERNS = [
    "bij Interieur-shop.nl",
    "Stijlvol design en snelle levering",
]


def audit_desc(desc: str, duplicate: bool) -> str:
    if not desc:
        return "missing"
    n = len(desc)
    if duplicate:
        return "duplicate"
    if any(pat in desc for pat in TEMPLATE_PATTERNS):
        return "templated"  # lengte kan in bereik vallen maar is nog steeds garbage
    if n > DESC_MAX:
        return "too_long"
    if n < DESC_MIN:
        return "too_short"
    return "ok"


def load_excel(path: str) -> pd.DataFrame:
    if not Path(path).exists():
        sys.exit(f"FOUT: bestand niet gevonden: {path}")
    df = pd.read_excel(path)

    missing = [c for c in ALLOWED_COLUMNS if c not in df.columns]
    if missing:
        sys.exit(f"FOUT: verwachte kolommen missen in Excel: {missing}")

    # Reduce to whitelist + dedupe op Product ID (export bevat 1 rij per variant)
    df = df[ALLOWED_COLUMNS].drop_duplicates(subset=["Product ID"]).reset_index(drop=True)
    return df


def build_rows(df: pd.DataFrame) -> list[dict]:
    # Duplicaten detecteren op basis van exacte string (case-insensitive, trimmed)
    titles = df["Product meta title"].apply(clean_str).str.lower()
    descs = df["Product meta description"].apply(clean_str).str.lower()

    dup_titles = set(titles[(titles != "") & (titles.duplicated(keep=False))])
    dup_descs = set(descs[(descs != "") & (descs.duplicated(keep=False))])

    rows = []
    for _, r in df.iterrows():
        pid = clean_str(r["Product ID"]).replace(".0", "")
        # Product ID komt vaak als float binnen; cast netjes
        try:
            pid_clean = str(int(float(pid))) if pid else ""
        except ValueError:
            pid_clean = pid

        if not pid_clean:
            continue  # skip rijen zonder geldig Product ID

        handle = clean_str(r["Product handle"])
        title = clean_str(r["Product title"])
        vendor = normalize_vendor(clean_str(r["Product vendor"]))
        meta_title = clean_str(r["Product meta title"])
        meta_desc = clean_str(r["Product meta description"])

        t_dup = meta_title.lower() in dup_titles if meta_title else False
        d_dup = meta_desc.lower() in dup_descs if meta_desc else False

        rows.append({
            "shopify_product_id":       pid_clean,
            "handle":                   handle,
            "product_title":            title,
            "vendor":                   vendor,
            "current_meta_title":       meta_title or None,
            "current_meta_description": meta_desc or None,
            "current_title_length":     len(meta_title) if meta_title else 0,
            "current_desc_length":      len(meta_desc) if meta_desc else 0,
            "title_status":             audit_title(meta_title, t_dup),
            "desc_status":              audit_desc(meta_desc, d_dup),
            "review_status":            "pending",
        })
    return rows


def upsert_rows(rows: list[dict], batch_size: int = 500) -> None:
    sb = get_supabase()
    total = len(rows)
    for i in range(0, total, batch_size):
        chunk = rows[i:i + batch_size]
        sb.table("shopify_meta_audit").upsert(
            chunk, on_conflict="shopify_product_id"
        ).execute()
        print(f"  Upsert {i + len(chunk)} / {total}")


def print_summary(rows: list[dict]) -> None:
    from collections import Counter
    t_counts = Counter(r["title_status"] for r in rows)
    d_counts = Counter(r["desc_status"] for r in rows)
    print("\nTitle status:")
    for k, v in t_counts.most_common():
        print(f"  {k:12} {v}")
    print("\nDescription status:")
    for k, v in d_counts.most_common():
        print(f"  {k:12} {v}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="Pad naar Alle Active Producten.xlsx")
    ap.add_argument("--dry-run", action="store_true", help="Toon eerste 10 en skip upload")
    args = ap.parse_args()

    print(f"Lezen: {args.file}")
    df = load_excel(args.file)
    print(f"  {len(df)} unieke producten gevonden")

    rows = build_rows(df)
    print_summary(rows)

    if args.dry_run:
        print("\n--dry-run — eerste 5 rijen:")
        for r in rows[:5]:
            print(r)
        return

    print(f"\nUpload naar Supabase shopify_meta_audit ...")
    upsert_rows(rows)
    print("Klaar.")


if __name__ == "__main__":
    main()
