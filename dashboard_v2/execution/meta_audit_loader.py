"""
meta_audit_loader.py -- Laad actieve Shopify producten in `shopify_meta_audit`.

WHITELIST (hard): alleen deze kolommen uit de Excel worden gebruikt:
  - Product ID
  - Product handle
  - Product title
  - Product vendor
  - Product meta title
  - Product meta description

Geen enkele andere kolom wordt uitgelezen of geschreven. Dit is bewust --
we mogen GEEN eerder gecureerde data (prijzen, descriptions, tags) overschrijven.

Zie directives/meta_audit.md voor de volledige SOP.

Gebruik:
    python execution/meta_audit_loader.py --file "master files/Alle Active Producten.xlsx"
    python execution/meta_audit_loader.py --file "..." --dry-run   # laat eerste 10 rijen zien zonder upload

Bevat ook een pure-Python entrypoint `load_meta_audit(...)` voor gebruik
vanuit de Streamlit dashboard.
"""

import argparse
import os
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")

# Harde whitelist -- ALLEEN deze kolommen.
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


# ---------------------------------------------------------------------------
# Result types & exceptions
# ---------------------------------------------------------------------------


class MetaAuditError(Exception):
    """Raised when meta-audit load cannot proceed."""


@dataclass
class MetaAuditResult:
    loaded_count: int = 0
    title_issues_count: int = 0
    desc_issues_count: int = 0
    title_status_counts: dict = field(default_factory=dict)
    desc_status_counts: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def get_supabase():
    from supabase import create_client
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise MetaAuditError("SUPABASE_URL / SUPABASE_KEY ontbreken in .env")
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


def audit_desc(desc: str, duplicate: bool) -> str:
    if not desc:
        return "missing"
    n = len(desc)
    if duplicate:
        return "duplicate"
    if n > DESC_MAX:
        return "too_long"
    if n < DESC_MIN:
        return "too_short"
    return "ok"


def load_excel(path: str) -> pd.DataFrame:
    if not Path(path).exists():
        raise MetaAuditError(f"bestand niet gevonden: {path}")
    df = pd.read_excel(path)

    missing = [c for c in ALLOWED_COLUMNS if c not in df.columns]
    if missing:
        raise MetaAuditError(f"verwachte kolommen missen in Excel: {missing}")

    df = df[ALLOWED_COLUMNS].drop_duplicates(subset=["Product ID"]).reset_index(drop=True)
    return df


def build_rows(df: pd.DataFrame) -> list[dict]:
    titles = df["Product meta title"].apply(clean_str).str.lower()
    descs = df["Product meta description"].apply(clean_str).str.lower()

    dup_titles = set(titles[(titles != "") & (titles.duplicated(keep=False))])
    dup_descs = set(descs[(descs != "") & (descs.duplicated(keep=False))])

    rows = []
    for _, r in df.iterrows():
        pid = clean_str(r["Product ID"]).replace(".0", "")
        try:
            pid_clean = str(int(float(pid))) if pid else ""
        except ValueError:
            pid_clean = pid

        if not pid_clean:
            continue

        handle = clean_str(r["Product handle"])
        title = clean_str(r["Product title"])
        vendor = clean_str(r["Product vendor"])
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


def upsert_rows(
    rows: list[dict],
    batch_size: int = 500,
    progress: Callable[[int, int, str], None] | None = None,
    logger: Callable[[str], None] | None = None,
) -> None:
    log = logger or (lambda _msg: None)
    sb = get_supabase()
    total = len(rows)
    for i in range(0, total, batch_size):
        chunk = rows[i:i + batch_size]
        sb.table("shopify_meta_audit").upsert(
            chunk, on_conflict="shopify_product_id"
        ).execute()
        log(f"  Upsert {i + len(chunk)} / {total}")
        if progress:
            progress(i + len(chunk), total, "upsert")


def print_summary(rows: list[dict]) -> None:
    t_counts = Counter(r["title_status"] for r in rows)
    d_counts = Counter(r["desc_status"] for r in rows)
    print("\nTitle status:")
    for k, v in t_counts.most_common():
        print(f"  {k:12} {v}")
    print("\nDescription status:")
    for k, v in d_counts.most_common():
        print(f"  {k:12} {v}")


# ---------------------------------------------------------------------------
# Pure-function entrypoint
# ---------------------------------------------------------------------------


def load_meta_audit(
    file_path: str,
    dry_run: bool = False,
    progress: Callable[[int, int, str], None] | None = None,
    logger: Callable[[str], None] | None = None,
) -> MetaAuditResult:
    """Load meta-audit rijen vanuit Excel en upsert in shopify_meta_audit."""
    log = logger or (lambda _msg: None)

    def _progress(step: int, total: int, msg: str) -> None:
        if progress:
            progress(step, total, msg)

    _progress(1, 3, "Excel lezen")
    log(f"Lezen: {file_path}")
    df = load_excel(file_path)  # raises MetaAuditError on failure
    log(f"  {len(df)} unieke producten gevonden")

    _progress(2, 3, "Rijen bouwen / audit draaien")
    rows = build_rows(df)

    title_counts = Counter(r["title_status"] for r in rows)
    desc_counts = Counter(r["desc_status"] for r in rows)
    title_issues = sum(v for k, v in title_counts.items() if k != "ok")
    desc_issues = sum(v for k, v in desc_counts.items() if k != "ok")

    if not dry_run:
        _progress(3, 3, "Upload naar Supabase")
        log("Upload naar Supabase shopify_meta_audit ...")
        try:
            upsert_rows(rows, progress=progress, logger=logger)
        except MetaAuditError:
            raise
        except Exception as e:
            raise MetaAuditError(f"Upsert mislukt: {e}") from e
    else:
        _progress(3, 3, "Dry-run: geen upload")
        log("[DRY RUN] Geen upload naar Supabase uitgevoerd.")

    return MetaAuditResult(
        loaded_count=len(rows),
        title_issues_count=title_issues,
        desc_issues_count=desc_issues,
        title_status_counts=dict(title_counts),
        desc_status_counts=dict(desc_counts),
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="Pad naar Alle Active Producten.xlsx")
    ap.add_argument("--dry-run", action="store_true", help="Toon eerste 10 en skip upload")
    args = ap.parse_args()

    try:
        result = load_meta_audit(
            file_path=args.file,
            dry_run=args.dry_run,
            logger=lambda m: print(m),
        )
    except MetaAuditError as e:
        print(f"FOUT: {e}", file=sys.stderr)
        sys.exit(1)

    print("\nTitle status:")
    for k, v in sorted(result.title_status_counts.items(), key=lambda kv: -kv[1]):
        print(f"  {k:12} {v}")
    print("\nDescription status:")
    for k, v in sorted(result.desc_status_counts.items(), key=lambda kv: -kv[1]):
        print(f"  {k:12} {v}")

    print(f"\nLoaded: {result.loaded_count}")
    print(f"Title issues:       {result.title_issues_count}")
    print(f"Description issues: {result.desc_issues_count}")
    if args.dry_run:
        print("\n[DRY RUN] Geen upload uitgevoerd.")
    else:
        print("Klaar.")


if __name__ == "__main__":
    main()
