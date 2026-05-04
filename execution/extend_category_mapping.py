"""
Eenmalige uitbreiding van seo_category_mapping op basis van de SOP-categorieboom
en de werkelijke leveranciers-codes in seo_products.

Bevat alleen mappings die met 100% zekerheid kloppen op basis van de SOP en de
website-indeling. Pottery pots stijlen (Natural/Rough/Essential/etc.) staan
NIET in deze lijst omdat de stijl niets zegt over het producttype — die moeten
handmatig of via Claude-suggestie worden gemapt.

Gebruik:
    python execution/extend_category_mapping.py
"""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

# (leverancier_category, leverancier_item_cat, hoofdcategorie, subcategorie, sub_subcategorie)
NEW_MAPPINGS = [
    # ── Interior Accessories (renamed van 'Interior Acc.' uit oude mapping) ────
    ("Interior Accessories", "vases",                          "Vazen & Potten",    "Vazen",                 "Design vazen"),
    ("Interior Accessories", "mirrors",                        "Wonen & badkamer",  "Interieur & Styling",   "Spiegels"),
    ("Interior Accessories", "candle & tea light holders",     "Wonen & badkamer",  "Interieur & Styling",   "Geurkaarsen"),
    ("Interior Accessories", "flower pots & planters",         "Vazen & Potten",    "Potten",                "Bloempotten binnen"),
    ("Interior Accessories", "storage & organisation",         "Wonen & badkamer",  "Interieur & Styling",   "Wanddecoratie"),

    # ── Lighting subtypes ──────────────────────────────────────────────────────
    ("Lighting", "floor lamps",     "Wonen & badkamer", "Verlichting & Meubels", "Vloerlampen"),
    ("Lighting", "table lamps",     "Wonen & badkamer", "Verlichting & Meubels", "Tafellampen"),
    ("Lighting", "wall lamps",      "Wonen & badkamer", "Verlichting & Meubels", "Wandlampen"),
    ("Lighting", "pendant lamps",   "Wonen & badkamer", "Verlichting & Meubels", "Hanglampen"),

    # ── Cookware ───────────────────────────────────────────────────────────────
    ("Cookware", "pots",      "Keuken & Eetkamer", "Keuken & Bereiding", "Pannen"),
    ("Cookware", "pans",      "Keuken & Eetkamer", "Keuken & Bereiding", "Pannen"),
    ("Cookware", "ovenware",  "Keuken & Eetkamer", "Keuken & Bereiding", "Ovenschalen"),

    # ── Kitchen & Table Access ─────────────────────────────────────────────────
    ("Kitchen&Table Access", "kitchen utensils", "Keuken & Eetkamer", "Keuken & Bereiding", "Snijplanken"),
    ("Kitchen&Table Access", "trays",            "Keuken & Eetkamer", "Serveren",           "Dienbladen"),
    ("Kitchen&Table Access", "bowls",            "Servies",           "Schalen",            "Saladeschalen"),

    # ── Cutlery ────────────────────────────────────────────────────────────────
    ("Cutlery&Knives", "cutlery set", "Servies", "Bestek", "Besteksets"),

    # ── Dinnerware uitbreidingen ───────────────────────────────────────────────
    ("Dinnerware", "jugs & carafes",             "Servies",           "Serveergoed",          "Melkkannen"),
    ("Dinnerware", "milk/cream jugs",            "Servies",           "Serveergoed",          "Melkkannen"),
    ("Dinnerware", "saucers",                    "Servies",           "Borden",               "Onderborden"),
    ("Dinnerware", "small storage & organisers", "Keuken & Eetkamer", "Keukenorganisatie",    "Voorraadpotten"),
    ("Dinnerware", "storage & organisation",     "Keuken & Eetkamer", "Keukenorganisatie",    "Voorraadpotten"),
    ("Dinnerware", "dishes",                     "Servies",           "Schalen",              "Serveerschalen"),
    ("Dinnerware", "ovenware",                   "Keuken & Eetkamer", "Keuken & Bereiding",   "Ovenschalen"),
    ("Dinnerware", "egg cups",                   "Servies",           "Serveergoed",          "Eierdoppen"),
    ("Dinnerware", "glasses",                    "Glazen",            "Water & Thee",         "Drinkglazen"),
    ("Dinnerware", "tea pots",                   "Servies",           "Serveergoed",          "Theepotten"),

    # ── Glassware uitbreidingen ────────────────────────────────────────────────
    # 'Glassware/glasses' en 'Glassware/jugs & carafes' bestaan al in de oude mapping
]


def main():
    from supabase import create_client
    sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

    rows = [
        {
            "leverancier_category": r[0],
            "leverancier_item_cat": r[1],
            "hoofdcategorie":       r[2],
            "subcategorie":         r[3],
            "sub_subcategorie":     r[4],
        }
        for r in NEW_MAPPINGS
    ]

    print(f"Upserten van {len(rows)} mappings naar seo_category_mapping...\n")
    sb.table("seo_category_mapping").upsert(
        rows,
        on_conflict="leverancier_category,leverancier_item_cat",
    ).execute()

    # Verifieer: hoeveel producten matchen nu wel?
    all_maps = sb.table("seo_category_mapping").select("leverancier_category, leverancier_item_cat").execute().data
    mapping_set = {(m["leverancier_category"], m["leverancier_item_cat"]) for m in all_maps}
    print(f"Totaal mappings in tabel: {len(mapping_set)}\n")

    # Coverage check
    all_prods = []
    offset = 0
    while True:
        res = sb.table("seo_products").select("leverancier_category, leverancier_item_cat").range(offset, offset + 999).execute()
        if not res.data:
            break
        all_prods.extend(res.data)
        if len(res.data) < 1000:
            break
        offset += 1000

    matched = sum(
        1 for p in all_prods
        if (p.get("leverancier_category") or "?", p.get("leverancier_item_cat") or "?") in mapping_set
    )
    print(f"Coverage: {matched} / {len(all_prods)} producten matchen nu een mapping ({matched / len(all_prods) * 100:.1f}%)")


if __name__ == "__main__":
    main()
