"""
Masterbestand laden en koppelen aan Supabase.

Detecteert automatisch de kolomindeling van het leveranciers-bestand,
toont een mapping-preview voor goedkeuring, en slaat de mapping op
zodat je dit niet elke keer opnieuw hoeft te doen.

Gebruik:
    python execution/setup_masterdata.py --file masterdata.xlsx [--leverancier serax]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

CONFIG_DIR = Path("config")


class SetupError(Exception):
    """Raised when masterdata setup cannot proceed."""


@dataclass
class SetupResult:
    mapping: dict
    loaded_count: int = 0
    warnings: list[str] = None


# Doelkolommen in onze database + alle bekende aliassen per leverancier
SCHEMA = {
    "sku": {
        "label": "SKU / Artikelnummer",
        "aliassen": ["SKU", "brand_id", "Artikel", "Artikelnummer", "Product ID", "Item Number", "Code",
                     "Articlecode", "Article code", "Article Code", "Artikelcode"],
    },
    "ean_piece": {
        "label": "EAN Stuk",
        "aliassen": ["EAN Piece", "EAN Code Piece", "EAN stuk", "Barcode Piece", "EAN los",
                     "EAN-UCC _Code", "EAN-UCC Code", "EAN Code", "EAN", "Barcode",
                     "EAN code per stuk"],
    },
    "ean_shopify": {
        "label": "EAN Packaging / Giftbox",
        "aliassen": ["EAN Packaging/Giftbox", "EAN Packaging", "EAN Giftbox", "EAN Box", "EAN verpakking", "Barcode GB",
                     "EAN-UCC _Code", "EAN-UCC Code", "EAN Code", "EAN", "Barcode",
                     "EAN code per stuk"],
    },
    "product_name_raw": {
        "label": "Productnaam (EN)",
        "aliassen": [
            "Product Name", "Name", "Title", "Omschrijving", "Naam", "Description EN",
            "Short Product Name Piece (English)", "Short Product name Piece (English)",
            "Short Product Name Piece (Dutch)", "Short Product name Piece (Dutch)",
            "Description", "Productnaam",
        ],
    },
    "designer": {
        "label": "Designer",
        "aliassen": ["Designer", "Design", "Ontwerper", "Brand Designer"],
    },
    "kleur_en": {
        "label": "Kleur (EN)",
        "aliassen": ["Color", "Colour", "Kleur", "Color EN", "Kleur EN", "Color Group", "Color Group ",
                     "Color name", "Color name ", "Colour name", "Kleur (1)"],
    },
    "materiaal_nl": {
        "label": "Materiaal",
        "aliassen": ["Product Material", "Material", "Materiaal", "Material EN", "Materiaal (1)"],
    },
    "leverancier_category": {
        "label": "Hoofdcategorie leverancier",
        "aliassen": ["Category", "Product Category", "Serax Category", "Categorie", "Main Category",
                     "Collection", "Collectie"],
    },
    "leverancier_item_cat": {
        "label": "Subcategorie leverancier",
        "aliassen": ["Item Category", "Item Cat.", "Serax Item Cat.", "Sub Category", "Subcategorie",
                     "Form"],
    },
    "giftbox": {
        "label": "Giftbox (YES/NO)",
        "aliassen": ["Giftbox", "Gift Box", "GB", "Is Giftbox", "Verpakking Type",
                     "Giftbox availble?", "Giftbox available?"],
    },
    "giftbox_qty": {
        "label": "Giftbox Hoeveelheid",
        "aliassen": ["Giftbox Quantity", "GB Quantity", "Giftbox Qty", "GB Qty", "Stuks per GB",
                     "Giftbox quantity", "Giftbox qty"],
    },
    "rrp_stuk_eur": {
        "label": "RRP Stuk (EUR)",
        "aliassen": ["RRP Stuk EUR", "RRP Piece EUR", "RRP stuk", "Adviesprijs stuk",
                     "Retail Price Piece EUR", "Verkoopprijs stuk", "RRP",
                     "Retail adviesprijs incl. btw", "Adviesprijs incl. btw"],
    },
    "rrp_gb_eur": {
        "label": "RRP Giftbox (EUR)",
        "aliassen": ["RRP GB EUR", "RRP Giftbox EUR", "RRP GB", "Adviesprijs GB", "Retail Price GB EUR"],
    },
    "inkoopprijs_stuk_eur": {
        "label": "Inkoopprijs Stuk (EUR)",
        "aliassen": ["Inkoopprijs Stuk EUR", "Purchase Price Piece EUR", "Cost Price Piece EUR",
                     "Inkoopprijs stuk", "Cost Piece"],
        "prefix_match": ["pricelist", "price list", "nettoprijs"],  # datum-varianten
    },
    "inkoopprijs_gb_eur": {
        "label": "Inkoopprijs Giftbox (EUR)",
        "aliassen": ["Inkoopprijs GB EUR", "Purchase Price GB EUR", "Cost Price GB EUR",
                     "Inkoopprijs GB", "Cost GB"],
    },
    "hoogte_cm": {
        "label": "Hoogte (cm)",
        "aliassen": ["Height", "Hoogte", "H (cm)", "Hoogte cm", "Height CM",
                     "Height single item (cm)", "Height (cm)"],
    },
    "lengte_cm": {
        "label": "Lengte (cm)",
        "aliassen": ["Length", "Lengte", "L (cm)", "Lengte cm", "Length CM",
                     "Length single item (cm)", "Length (cm)"],
    },
    "breedte_cm": {
        "label": "Breedte (cm)",
        "aliassen": ["Width", "Breedte", "B (cm)", "Breedte cm", "Width CM",
                     "Width single item (cm)", "Width (cm)"],
    },
}

VERPLICHT = ["sku", "ean_shopify", "ean_piece"]


def get_supabase():
    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def detecteer_header_rij(file_path: str) -> int:
    """
    Sommige Serax-bestanden hebben een sectie-header op rij 1 en de echte
    kolomnamen op rij 2. Detecteer dit door te kijken hoeveel kolommen
    'Unnamed: N' heten — dat betekent lege cellen op de headerrij.
    Geeft 0 (eerste rij) of 1 (tweede rij) terug.
    """
    df_test = pd.read_excel(file_path, nrows=0, dtype=str)
    unnamed_count = sum(1 for c in df_test.columns if str(c).startswith("Unnamed:"))
    ratio = unnamed_count / max(len(df_test.columns), 1)
    return 1 if ratio > 0.3 else 0


def config_pad(leverancier: str) -> Path:
    CONFIG_DIR.mkdir(exist_ok=True)
    return CONFIG_DIR / f"kolom_mapping_{leverancier.lower()}.json"


def laad_opgeslagen_mapping(leverancier: str) -> dict | None:
    pad = config_pad(leverancier)
    if pad.exists():
        with open(pad, encoding="utf-8") as f:
            return json.load(f)
    return None


def sla_mapping_op(leverancier: str, mapping: dict):
    pad = config_pad(leverancier)
    with open(pad, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)


def detect_mapping(kolommen: list[str]) -> dict:
    """
    Auto-detecteer welke kolom in het bestand overeenkomt met welk DB-veld.
    Normaliseert intern witruimte/newlines zodat multi-line kolomnamen ook matchen.
    Geeft {db_veld: kolom_in_bestand} terug.
    """
    import re as _re

    def norm(s: str) -> str:
        """Collapse alle whitespace (incl. newlines) naar één spatie, strip."""
        return _re.sub(r"\s+", " ", str(s)).strip().lower()

    kolommen_norm = {norm(k): k for k in kolommen}
    mapping = {}

    for db_veld, info in SCHEMA.items():
        # 1. Exacte alias-match (na normalisatie)
        for alias in info["aliassen"]:
            if norm(alias) in kolommen_norm:
                mapping[db_veld] = kolommen_norm[norm(alias)]
                break

        # 2. Prefix-match voor velden met datum-varianten (bijv. "Pricelist per 02-12-2025")
        if db_veld not in mapping:
            for prefix in info.get("prefix_match", []):
                match = next((orig for n, orig in kolommen_norm.items() if n.startswith(prefix)), None)
                if match:
                    mapping[db_veld] = match
                    break

    return mapping


def detect_ambiguous(kolommen: list[str]) -> dict[str, list[str]]:
    """
    Detecteer velden waar meerdere aliassen in het bestand voorkomen.
    Geeft {db_veld: [kandidaten]} voor elk veld waar > 1 alias matcht.
    """
    import re as _re

    def norm(s: str) -> str:
        return _re.sub(r"\s+", " ", str(s)).strip().lower()

    kolommen_norm = {norm(k): k for k in kolommen}
    ambiguous = {}

    for db_veld, info in SCHEMA.items():
        hits = []
        for alias in info["aliassen"]:
            if norm(alias) in kolommen_norm:
                match = kolommen_norm[norm(alias)]
                if match not in hits:
                    hits.append(match)
        if len(hits) > 1:
            ambiguous[db_veld] = hits

    return ambiguous


def toon_mapping_preview(mapping: dict, kolommen: list[str], log: Callable[[str], None]) -> None:
    """Print een overzichtstabel van de gedetecteerde mapping."""
    log("\nGedetecteerde kolom-koppeling:\n")
    log(f"  {'DB Veld':<25} {'Label':<30} {'Kolom in bestand':<35} {'Status'}")
    log(f"  {'-'*25} {'-'*30} {'-'*35} {'-'*10}")

    for db_veld, info in SCHEMA.items():
        gevonden = mapping.get(db_veld)
        is_verplicht = db_veld in VERPLICHT
        if gevonden:
            status = "OK" if not is_verplicht else "OK (verplicht)"
        else:
            status = "ONTBREEKT (verplicht!)" if is_verplicht else "niet gevonden"

        label = info["label"]
        kolom_display = gevonden or "-"
        log(f"  {db_veld:<25} {label:<30} {kolom_display:<35} {status}")

    niet_gekoppeld = [k for k in kolommen if k not in mapping.values()]
    if niet_gekoppeld:
        log(f"\n  Kolommen in bestand NIET gekoppeld ({len(niet_gekoppeld)}):")
        for k in niet_gekoppeld[:10]:
            log(f"    - {k}")
        if len(niet_gekoppeld) > 10:
            log(f"    ... en {len(niet_gekoppeld) - 10} meer")


def detect_and_store_mapping(
    file_path: str,
    leverancier: str | None = None,
    on_confirm: Optional[Callable[[dict], bool]] = None,
    on_ambiguous: Optional[Callable[[str, list[str]], str]] = None,
    logger: Optional[Callable[[str], None]] = None,
) -> dict:
    """
    Detecteer de kolom-mapping voor `file_path`, laat ambiguiteit oplossen via
    `on_ambiguous`, en sla op (na `on_confirm`).

    Args:
        file_path: pad naar .xlsx
        leverancier: voor bestandsnaam van opgeslagen mapping; default afgeleid
        on_confirm: callback(mapping) -> bool. True = opslaan, False = abort.
            None = altijd opslaan (non-interactief).
        on_ambiguous: callback(db_veld, kandidaten) -> gekozen kolom (of "" om te skippen).
            None = kies eerste kandidaat.
        logger: callback(message)

    Returns:
        de opgeslagen mapping dict
    """
    log = logger or (lambda m: print(m))
    leverancier = leverancier or "onbekend"

    pad = Path(file_path)
    if not pad.exists():
        raise SetupError(f"Bestand niet gevonden: {file_path}")

    header_rij = detecteer_header_rij(file_path)
    if header_rij > 0:
        log(f"  Dubbele headerrij gedetecteerd — echte kolomnamen op rij {header_rij + 1}")
    df_head = pd.read_excel(file_path, header=header_rij, nrows=0, dtype=str)
    kolommen = list(df_head.columns)
    log(f"Kolommen gevonden in bestand: {len(kolommen)}")

    mapping = detect_mapping(kolommen)

    # Ambiguïteiten: velden met meerdere mogelijke matches
    ambiguous = detect_ambiguous(kolommen)
    for db_veld, kandidaten in ambiguous.items():
        if on_ambiguous:
            chosen = on_ambiguous(db_veld, kandidaten)
            if chosen and chosen in kolommen:
                mapping[db_veld] = chosen
            elif chosen == "":
                mapping.pop(db_veld, None)
        # zo niet: laat detect_mapping de eerste-alias-winnaar staan

    toon_mapping_preview(mapping, kolommen, log)

    ontbrekend_verplicht = [v for v in VERPLICHT if v not in mapping]
    if ontbrekend_verplicht:
        log(f"\nLET OP: Verplichte velden niet gedetecteerd: {ontbrekend_verplicht}")

    # Bevestiging
    if on_confirm is not None:
        ok = on_confirm(mapping)
        if not ok:
            raise SetupError("Mapping niet goedgekeurd door gebruiker.")

    sla_mapping_op(leverancier, mapping)
    log(f"\nMapping opgeslagen in: {config_pad(leverancier)}")

    return mapping


def laad_masterdata(file_path: str, leverancier: str, fase: str, mapping: dict,
                    logger: Callable[[str], None] | None = None) -> int:
    """Laad producten in Supabase op basis van de mapping. Geeft aantal geladen producten."""
    log = logger or (lambda m: print(m))

    header_rij = detecteer_header_rij(file_path)
    df = pd.read_excel(file_path, header=header_rij, dtype=str)

    # Waarden die universeel "leeg" betekenen in spreadsheets
    LEGE_WAARDEN = {"", "nan", "-", "n/a", "n.a.", "n.v.t.", "nvt", "none", "null"}

    def get_val(row, db_veld):
        """Lees waarde exact zoals die in het bestand staat — geen transformatie.
        Geeft None voor universele lege-waarde-placeholders zoals '-' en 'n/a'."""
        kolom = mapping.get(db_veld)
        if not kolom or kolom not in row.index:
            return None
        val = row.get(kolom, "")
        if pd.isna(val):
            return None
        s = str(val).strip()
        return None if s.lower() in LEGE_WAARDEN else s

    rows = []
    warnings = []

    for idx, (_, row) in enumerate(df.iterrows()):
        sku = get_val(row, "sku") or ""
        if not sku:
            continue

        ean_shopify = get_val(row, "ean_shopify") or ""
        ean_piece   = get_val(row, "ean_piece") or ""

        if not ean_shopify and not ean_piece:
            warnings.append(f"SKU {sku}: geen EAN gevonden, overgeslagen")
            continue

        if not ean_shopify:
            ean_shopify = ean_piece
            warnings.append(f"SKU {sku}: geen Packaging EAN, EAN Piece als fallback")

        def naar_getal(veld):
            """Minimale coercitie: komma→punt voor Supabase NUMERIC-velden.
            Geen inhoudelijke transformatie — '99,00' en '99.00' zijn hetzelfde getal."""
            val = get_val(row, veld)
            if val is None:
                return None
            return val.replace(",", ".")

        rows.append({
            "sku":                   sku,
            "ean_shopify":           ean_shopify,
            "ean_piece":             ean_piece,
            "product_name_raw":      get_val(row, "product_name_raw") or "",
            "designer":              get_val(row, "designer") or "",
            "kleur_en":              get_val(row, "kleur_en") or "",
            "materiaal_nl":          get_val(row, "materiaal_nl") or "",
            "leverancier_category":  get_val(row, "leverancier_category") or "",
            "leverancier_item_cat":  get_val(row, "leverancier_item_cat") or "",
            "giftbox":               get_val(row, "giftbox") or "",
            "giftbox_qty":           naar_getal("giftbox_qty"),
            "rrp_stuk_eur":          naar_getal("rrp_stuk_eur"),
            "rrp_gb_eur":            naar_getal("rrp_gb_eur"),
            "inkoopprijs_stuk_eur":  naar_getal("inkoopprijs_stuk_eur"),
            "inkoopprijs_gb_eur":    naar_getal("inkoopprijs_gb_eur"),
            "hoogte_cm":             naar_getal("hoogte_cm"),
            "lengte_cm":             naar_getal("lengte_cm"),
            "breedte_cm":            naar_getal("breedte_cm"),
            "status":                "raw",
            "fase":                  str(fase),
        })

    if warnings:
        log(f"\n  Waarschuwingen ({len(warnings)}):")
        for w in warnings[:10]:
            log(f"    - {w}")
        if len(warnings) > 10:
            log(f"    ... en {len(warnings) - 10} meer")

    if not rows:
        log("FOUT: Geen geldige producten gevonden.")
        return 0

    sb = get_supabase()
    log(f"\n  Uploaden: {len(rows)} producten naar Supabase...")

    for i in range(0, len(rows), 100):
        batch = rows[i:i + 100]
        sb.table("seo_products").insert(batch).execute()

    sb.table("seo_import_runs").insert({
        "bestandsnaam":     Path(file_path).name,
        "fase":             str(fase),
        "aantal_producten": len(rows),
        "aantal_warnings":  len(warnings),
        "fouten":           "; ".join(warnings[:5]) if warnings else None,
    }).execute()

    return len(rows)


def setup_masterdata(file_path: str, leverancier: str, fase: str, auto: bool = False):
    """CLI-georiënteerde wrapper die detect_and_store_mapping + laad_masterdata combineert."""
    pad = Path(file_path)
    if not pad.exists():
        print(f"FOUT: Bestand niet gevonden: {file_path}", file=sys.stderr)
        sys.exit(1)

    print(f"\nMasterbestand: {pad.name}")
    print(f"Leverancier:   {leverancier}")
    print(f"Fase:          {fase}\n")

    # Check op opgeslagen mapping
    opgeslagen = laad_opgeslagen_mapping(leverancier)
    use_saved = False
    if opgeslagen:
        if auto:
            use_saved = True
            print(f"Opgeslagen mapping voor '{leverancier}' geladen uit config/")
        else:
            antwoord = input(f"\nEr is een opgeslagen mapping voor '{leverancier}'. Gebruik die? [j/n]: ").strip().lower()
            use_saved = antwoord != "n"

    if use_saved:
        mapping = opgeslagen
        # Toch preview tonen
        header_rij = detecteer_header_rij(file_path)
        df_head = pd.read_excel(file_path, header=header_rij, nrows=0, dtype=str)
        toon_mapping_preview(mapping, list(df_head.columns), lambda m: print(m))
        if not auto:
            antwoord = input("\nMapping goedgekeurd? Start laden in Supabase? [j/n]: ").strip().lower()
            if antwoord != "j":
                print("Gestopt. Pas de mapping aan en probeer opnieuw.")
                sys.exit(0)
    else:
        # Via callback-flow
        if auto:
            on_confirm = None
        else:
            on_confirm = lambda m: input("\nMapping goedgekeurd? Start laden in Supabase? [j/n]: ").strip().lower().startswith("j")

        try:
            mapping = detect_and_store_mapping(
                file_path=file_path,
                leverancier=leverancier,
                on_confirm=on_confirm,
                on_ambiguous=None,
                logger=lambda m: print(m),
            )
        except SetupError as e:
            print(f"Gestopt: {e}")
            sys.exit(0)

    # Laden
    n = laad_masterdata(file_path, leverancier, fase, mapping)
    if n > 0:
        print(f"\nKlaar: {n} producten geladen in Supabase (fase {fase}, status='raw')")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file",       required=True, help="Pad naar masterbestand (.xlsx)")
    parser.add_argument("--leverancier", default="serax", help="Naam leverancier (voor opslaan mapping)")
    parser.add_argument("--fase",       required=True, help="Fasecode, bijv. 3")
    parser.add_argument("--auto",       action="store_true", help="Gebruik opgeslagen mapping zonder te vragen")
    args = parser.parse_args()

    setup_masterdata(args.file, args.leverancier, args.fase, args.auto)
