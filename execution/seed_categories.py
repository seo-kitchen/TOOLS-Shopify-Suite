"""
Eenmalige setup: seed de SOP-categorietabel in Supabase.
Veilig om meerdere keren te draaien (upsert).

Gebruik:
    python execution/seed_categories.py [--mode upsert|reset]
"""

import argparse
import os
import sys
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

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


def seed(mode: str = "upsert"):
    sb = get_supabase()

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
        sb.table("seo_category_mapping").delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
        print("Tabel geleegd.")

    sb.table("seo_category_mapping").upsert(
        rows,
        on_conflict="leverancier_category,leverancier_item_cat"
    ).execute()

    print(f"Categorie-mapping: {len(rows)} regels geladen (mode={mode})")
    for r in rows:
        print(f"  {r['leverancier_category']} / {r['leverancier_item_cat']} -> {r['hoofdcategorie']} > {r['subcategorie']} > {r['sub_subcategorie']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="upsert", choices=["upsert", "reset"])
    args = parser.parse_args()
    seed(args.mode)
