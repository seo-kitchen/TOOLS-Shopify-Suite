"""
Eenmalige setup: seed de SOP-categorietabel in Supabase.
Veilig om meerdere keren te draaien (upsert).

Gebruik:
    python execution/seed_categories.py [--mode upsert|reset]
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from typing import Callable, Optional

from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")


class SeedError(Exception):
    """Raised when seeding cannot proceed."""


@dataclass
class SeedResult:
    inserted: int = 0
    updated: int = 0
    deleted: int = 0


# Volledige categorietabel uit SOP v2.7 Stap 3
# (leverancier_category, leverancier_item_cat, hoofdcategorie, subcategorie, sub_subcategorie)
CATEGORY_ROWS = [
    ("Dinnerware",    "plates",          "Servies",             "Borden",                      "Diner borden"),
    ("Dinnerware",    "bowls",           "Servies",             "Kommen/Schalen",               "Kommen"),
    ("Dinnerware",    "cups",            "Servies",             "Kommen, Mokken & Bekers",      "Koffiemokken"),
    ("Dinnerware",    "cake stands",     "Servies",             "Schalen",                      "Gebakschalen"),
    ("Dinnerware",    "tea pots",        "Servies",             "Serveergoed",                  "Theepotten"),
    ("Dinnerware",    "storage",         "Keuken & Eetkamer",   "Keukenorganisatie",            "Voorraadpotten"),
    ("Glassware",     "glasses",         "Glazen",              "Wijn/Water/Bar",               "Wijnglazen"),
    ("Glassware",     "jugs & carafes",  "Glazen",              "Karaffen & Flessen",           "Karaffen"),
    ("Pottery&UJ",    "flower pots",     "Vazen & Potten",      "Potten",                       "Bloempotten binnen"),
    ("Interior Acc.", "vases",           "Vazen & Potten",      "Vazen",                        "Design vazen"),
    ("Interior Acc.", "candles/holders", "Wonen & badkamer",    "Interieur",                    "Geurkaarsen"),
    ("Interior Acc.", "mirrors",         "Wonen & badkamer",    "Interieur & Styling",          "Spiegels"),
    ("Lighting",      "table/wall lamps","Wonen & badkamer",    "Verlichting & Meubels",        "Tafellampen"),
    ("Furniture",     "stools/tables",   "Wonen & badkamer",    "Verlichting & Meubels",        "Barkrukken"),
]


def get_supabase():
    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def seed_categories(
    mode: str = "upsert",
    logger: Optional[Callable[[str], None]] = None,
) -> SeedResult:
    """
    Seed seo_category_mapping.

    Args:
        mode: "upsert" = insert-or-update, "reset" = eerst hele tabel legen
        logger: callback(message)

    Returns:
        SeedResult met inserted/updated/deleted tellers
    """
    log = logger or (lambda m: print(m))

    if mode not in ("upsert", "reset"):
        raise SeedError(f"Onbekende mode: {mode!r}. Gebruik 'upsert' of 'reset'.")

    sb = get_supabase()
    result_obj = SeedResult()

    rows = [
        {
            "leverancier_category": r[0],
            "leverancier_item_cat": r[1],
            "hoofdcategorie":       r[2],
            "subcategorie":         r[3],
            "sub_subcategorie":     r[4],
        }
        for r in CATEGORY_ROWS
    ]

    if mode == "reset":
        existing = sb.table("seo_category_mapping").select("id").execute()
        result_obj.deleted = len(existing.data or [])
        sb.table("seo_category_mapping").delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
        log(f"Tabel geleegd ({result_obj.deleted} regels verwijderd).")

    # Bepaal bestaande keys voor inserted/updated telling
    existing_resp = sb.table("seo_category_mapping").select(
        "leverancier_category, leverancier_item_cat"
    ).execute()
    existing_keys = {
        (r["leverancier_category"], r["leverancier_item_cat"])
        for r in (existing_resp.data or [])
    }

    sb.table("seo_category_mapping").upsert(
        rows,
        on_conflict="leverancier_category,leverancier_item_cat"
    ).execute()

    for r in rows:
        key = (r["leverancier_category"], r["leverancier_item_cat"])
        if key in existing_keys:
            result_obj.updated += 1
        else:
            result_obj.inserted += 1

    log(
        f"Categorie-mapping: {len(rows)} regels verwerkt (mode={mode}) — "
        f"{result_obj.inserted} nieuw, {result_obj.updated} bijgewerkt."
    )
    for r in rows:
        log(
            f"  {r['leverancier_category']} / {r['leverancier_item_cat']} -> "
            f"{r['hoofdcategorie']} > {r['subcategorie']} > {r['sub_subcategorie']}"
        )

    return result_obj


def seed(mode: str = "upsert"):
    """Backwards-compatibele alias voor CLI / oude imports."""
    seed_categories(mode=mode)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="upsert", choices=["upsert", "reset"])
    args = parser.parse_args()
    try:
        seed_categories(args.mode)
    except SeedError as e:
        print(f"FOUT: {e}", file=sys.stderr)
        sys.exit(1)
