"""
Stap 3: SOP-stappen uitvoeren per product via Claude agent.
Zie directives/transform.md voor volledige instructies.

Gebruik:
    python execution/transform.py --fase 3
"""

import argparse
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Callable

from dotenv import load_dotenv

load_dotenv()


@dataclass
class TransformResult:
    """Return value of ``transform_batch``."""
    ready: int = 0
    review: int = 0
    errors: int = 0
    total: int = 0
    new_filter_values: list[str] = field(default_factory=list)
    twijfelgevallen: list[dict] = field(default_factory=list)
    learnings_applied: int = 0
    processed_ids: list[int] = field(default_factory=list)


class TransformError(RuntimeError):
    pass

SUPABASE_URL    = os.getenv("SUPABASE_URL")
SUPABASE_KEY    = os.getenv("SUPABASE_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

# ── Vertaaltabellen (SOP Stap 5a / 5b) ───────────────────────────────────────

# Materiaal: ALTIJD mappen naar een van de basismaterialen die op de website staan.
# Website-filters: Aardewerk, Beton, Fiberclay, Fiberstone, Ficonstone, Glas,
#   Glasvezel, Keramiek, Metaal, Papier, Porselein, Steen, Steengoed, Terracotta
MATERIAAL_NL = {
    # Keramiek & steen
    "stoneware":          "Steengoed",
    "ceramic":            "Keramiek",
    "ceramics":           "Keramiek",
    "fiberstone":         "Fiberstone",
    "fibercite":          "Fiberstone",
    "fiberclay":          "Fiberclay",
    "ficonstone":         "Ficonstone",
    "sandstone":          "Steen",
    "porcelain":          "Porselein",
    "fine bone china":    "Porselein",
    "bone china porcelain": "Porselein",
    "bone china":         "Porselein",
    "terracotta":         "Terracotta",
    "earthenware":        "Aardewerk",
    "concrete":           "Beton",
    "cement":             "Beton",
    "marble":             "Steen",
    "stone":              "Steen",
    # Glas
    "glass":              "Glas",
    "borosilicate glass": "Glas",
    "potassium glass":    "Glas",
    "crystal":            "Glas",
    "fiberglass":         "Glasvezel",
    "glass fiber":        "Glasvezel",
    "glass fibre":        "Glasvezel",
    # Metaal
    "metal":              "Metaal",
    "steel":              "Metaal",
    "stainless steel":    "Metaal",
    "aluminium":          "Metaal",
    "aluminum":           "Metaal",
    "cast iron":          "Metaal",
    "iron":               "Metaal",
    "copper":             "Metaal",
    "brass":              "Metaal",
    "zinc":               "Metaal",
    # Hout (niet in website-filters, maar bewaren als apart)
    "wood":               "Hout",
    "ash":                "Hout",
    "carbonised ash":     "Hout",
    "oak":                "Hout",
    "walnut":             "Hout",
    "acacia":             "Hout",
    # Textiel
    "linen":              "Linnen",
    "cotton":             "Katoen",
    "velvet":             "Fluweel",
    "leather":            "Leer",
    "silk and polyester": "Polyester",
    "polyester":          "Polyester",
    # Kunststof
    "plastic":            "Kunststof",
    "polyethylene":       "Kunststof",
    "polypropylene":      "Kunststof",
    "resin":              "Kunststof",
    # Natuurlijk
    "rattan":             "Rotan",
    "bamboo":             "Bamboe",
    # Papier
    "paper mache":        "Papier",
    "paper":              "Papier",
    "cardboard":          "Papier",
    # Overig
    "paint":              "Verf",
    "soy wax":            "Kaarsvet",
    "parafine":           "Kaarsvet",
    "paraffin":           "Kaarsvet",
    "pot-feet":           "Kunststof",
    "other":              "Overig",
    "cement-bamboo":     "Beton",
    "fiber-cement":      "Fiberstone & Beton",
    "ro-cement":         "Beton",
    # NL termen (Printworks data is al vertaald)
    "papier":            "Papier",
    "hout":              "Hout",
    "karton":            "Papier",
    "metaal":            "Metaal",
    "kunststof":         "Kunststof",
    "glas":              "Glas",
    "steengoed":         "Steengoed",
    "porselein":         "Porselein",
    "steen":             "Steen",
    "beton":             "Beton",
    "keramiek":          "Keramiek",
    "katoen":            "Katoen",
    "linnen":            "Linnen",
    "abs":               "Kunststof",
    "imitatieleer":      "Leer",
    "polycarbonaat":     "Kunststof",
    "polypropyleen":     "Kunststof",
    "mdf":               "Hout",
    "houtskool beuken":  "Hout",
    "iron wire":         "IJzer",
    "lead free crystal glass": "Kristal",
    "maple":             "Hout",
    "terrazzo":          "Steen",
    "imitation leather": "Leer",
    "new bone china":    "Porselein",
    "iron;wood":         "IJzer & Hout",
    "perfume":           "Overig",
    "polyurethane":      "Kunststof",
    "leer":              "Leer",
    "bamboe":            "Bamboe",
    "overig":            "Overig",
}

# Kleur: filter-weergave (metafield) — SOP Stap 5a tabel
# Kleur: filter-weergave (metafield) — ALTIJD de simpele basiskleurnaam.
# "Imperial Brown" → "Bruin", "Clouded Grey" → "Grijs", "Pine Green" → "Groen" etc.
KLEUR_FILTER = {
    # Wit
    "white":          "Wit",
    "off white":      "Wit",
    "off-white":      "Wit",
    "white matt":     "Wit",
    "matte white":    "Wit",
    "glossy white":   "Wit",
    "natural white":  "Wit",
    # Zwart
    "black":          "Zwart",
    "volcano black":  "Zwart",
    "weathered black": "Zwart",
    # Zwart & Wit
    "white black":    "Zwart & Wit",
    "black white":    "Zwart & Wit",
    # Grijs
    "grey":           "Grijs",
    "gray":           "Grijs",
    "dark grey":      "Grijs",
    "light grey":     "Grijs",
    "indi grey":      "Grijs",
    "clouded grey":   "Grijs",
    "anthracite":     "Grijs",
    # Beige
    "beige":          "Beige",
    "beige washed":   "Beige",
    "travertine beige": "Beige",
    "sand":           "Beige",
    "cream":          "Beige",
    "ecru":           "Beige",
    "taupe":          "Beige",
    # Bruin
    "brown":          "Bruin",
    "imperial brown": "Bruin",
    # Blauw
    "blue":           "Blauw",
    "dark blue":      "Blauw",
    "navy":           "Blauw",
    "midnight blue":  "Blauw",
    "light blue":     "Blauw",
    # Groen
    "green":          "Groen",
    "pine green":     "Groen",
    "olive":          "Groen",
    "sage":           "Groen",
    "dark green":     "Groen",
    "camo green":     "Groen",
    # Rood
    "red":            "Rood",
    "venetian red":   "Rood",
    "rust":           "Rood",
    "red white":      "Rood & Wit",
    # Roze
    "pink":           "Roze",
    # Geel
    "yellow":         "Geel",
    "mustard":        "Geel",
    # Oranje
    "orange":         "Oranje",
    # Paars
    "purple":         "Paars",
    # Metallic
    "gold":           "Goud",
    "silver":         "Zilver",
    "copper":         "Koper",
    # Transparant
    "transparent":    "Transparant",
    "transparant":    "Transparant",
    "clear":          "Transparant",
    # Overig
    "ivory":          "Ivoor",
    "mix":            "Multi",
    "multi":          "Multi",
    "terracotta":     "Terracotta",
    "chalk white":       "Wit",
    "clay washed":       "Bruin",
    "diorite grey":      "Grijs",
    "grey washed":       "Grijs",
    "ivory washed":      "Beige",
    "light grey (vertically ridged)": "Grijs",
    "mocha washed":      "Bruin",
    "rustic green":      "Groen",
    "silk white":        "Wit",
    "smoky umber":       "Bruin",
    "wabi beige":        "Beige",
    "black washed":      "Zwart",
    "midnight black":    "Zwart",
    "satin black":       "Zwart",
    "imperial white":    "Wit",
    "chalk beige":       "Beige",
    "mossy beige":       "Beige",
    "sahara sand":       "Beige",
    "sage green":        "Groen",
    "umber brown":       "Bruin",
    "root brown":        "Bruin",
    "powder pink":       "Roze",
    "brick orange":      "Oranje",
    "turquoise":         "Blauw",
    "bordeaux":          "Rood",
    "white stripe":      "Wit",
    "bamboo":            "Beige",
    "straw grass":       "Beige",
    "antracite - clear": "Grijs",
    "dark grey (horizontally ridged)": "Grijs",
    "light grey (horizontally ridged)": "Grijs",
    "dark grey (vertically ridged)": "Grijs",
    # NL kleurnamen (sommige leveranciers leveren al vertaalde data)
    "wit":               "Wit",
    "zwart":             "Zwart",
    "grijs":             "Grijs",
    "bruin":             "Bruin",
    "blauw":             "Blauw",
    "groen":             "Groen",
    "rood":              "Rood",
    "roze":              "Roze",
    "geel":              "Geel",
    "oranje":            "Oranje",
    "paars":             "Paars",
    "goud":              "Goud",
    "zilver":            "Zilver",
    "antraciet":         "Grijs",
    "lichtblauw":        "Blauw",
    "donkerblauw":       "Blauw",
    "lichtgrijs":        "Grijs",
    "ivoor":             "Ivoor",
    "amber":             "Bruin",
    "amber;black":       "Bruin",
    "blue;green":        "Blauw",
    "gold;black":        "Goud",
}

# Kleuren die bewaard blijven in de titel (niet vertaald, wel herkenbaar)
KLEUR_PRESERVE_IN_TITLE = {
    "indi grey", "venetian red", "camo green",
}

# SOP Stap 4: tag slug-overrides
TAG_OVERRIDES = {
    "wijn & champagne":   "wijn_champagne",
    "peper & zoutmolens": "peper-_zoutmolens",
}

# Lamps die de kleur uit de productnaam halen (niet uit Color-veld)
LAMP_EXCEPTIONS = ["PALOMA", "CATHERINE"]


def get_supabase():
    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_KEY)


# ── Learnings integration ────────────────────────────────────────────────────

def load_active_learnings(sb, stap: str | None = None) -> list[dict]:
    """Read all ``status='applied'`` rows from ``seo_learnings``.

    Returns a list of dicts: ``{rule_type, action, scope, id}``. The transform
    loop uses these to extend hardcoded base rules without ever having to be
    edited directly.

    If ``stap`` is None, returns all steps (caller filters). Otherwise filters
    at the DB level.
    """
    try:
        q = sb.table("seo_learnings").select("id,rule_type,action,scope,stap") \
              .eq("status", "applied")
        if stap is not None:
            q = q.eq("stap", stap)
        res = q.execute()
        return res.data or []
    except Exception:
        return []


def apply_name_rules(product: dict, updates: dict, learnings: list[dict]) -> int:
    """Apply ``name_rule`` + ``name_rule_bulk`` learnings to one product's updates dict.

    Modifies ``updates`` in place. Returns the number of rules that matched.

    Rule semantics (same as old learnings.json):
      - if ``is_extra`` is True: the learning's sub_subcategorie is ADDED to
        the product's ``_extra_tags`` list (additional tag alongside primary).
      - if ``is_extra`` is False: the learning OVERWRITES ``sub_subcategorie``.
    """
    naam = (product.get("product_name_raw") or "").lower()
    if not naam:
        return 0

    applied = 0
    extra_tags = list(updates.get("_extra_tags") or [])

    def _apply_one(rule: dict) -> None:
        nonlocal applied, extra_tags
        zoek = (rule.get("zoekwoord") or "").strip().lower()
        sub_sub = rule.get("sub_subcategorie") or ""
        if not zoek or not sub_sub:
            return
        if zoek not in naam:
            return
        if rule.get("is_extra"):
            if sub_sub not in extra_tags:
                extra_tags.append(sub_sub)
                applied += 1
        else:
            updates["sub_subcategorie"] = sub_sub
            applied += 1

    for L in learnings:
        rt = L.get("rule_type")
        act = L.get("action") or {}
        if rt == "name_rule":
            _apply_one(act)
        elif rt == "name_rule_bulk":
            for regel in (act.get("regels") or []):
                _apply_one(regel)

    if extra_tags:
        updates["_extra_tags"] = extra_tags
    return applied


def apply_translation_learnings(learnings: list[dict]) -> tuple[dict, dict]:
    """Fold 'translation' learnings into extra material/color dicts.

    Returns (extra_materiaal, extra_kleur) — merged into MATERIAAL_NL / KLEUR_NL
    on the fly inside translate_material / translate_color, without editing the
    module-level globals.
    """
    extra_mat, extra_kl = {}, {}
    for L in learnings:
        if L.get("rule_type") != "translation":
            continue
        act = L.get("action") or {}
        veld = (act.get("veld") or "").lower()
        en = (act.get("en") or "").strip().lower()
        nl = (act.get("nl") or "").strip()
        if not en or not nl:
            continue
        if veld == "materiaal":
            extra_mat[en] = nl
        elif veld == "kleur":
            extra_kl[en] = nl
    return extra_mat, extra_kl


def get_claude():
    import anthropic
    return anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


# ── Hulpfuncties ─────────────────────────────────────────────────────────────

def _fix_set_namen(naam: str) -> str:
    """Startset(s)→Serviesset(s), Giftset(s)/Geschenkset(s)→Cadeauset(s) — altijd, overal."""
    naam = re.sub(r"\bStartsets\b", "Serviessets", naam, flags=re.IGNORECASE)
    naam = re.sub(r"\bStartset\b", "Serviesset", naam, flags=re.IGNORECASE)
    naam = re.sub(r"\bGiftsets\b", "Cadeausets", naam, flags=re.IGNORECASE)
    naam = re.sub(r"\bGiftset\b", "Cadeauset", naam, flags=re.IGNORECASE)
    naam = re.sub(r"\bGeschenksets\b", "Cadeausets", naam, flags=re.IGNORECASE)
    naam = re.sub(r"\bGeschenkset\b", "Cadeauset", naam, flags=re.IGNORECASE)
    return naam


def clean_decimal(value) -> str | None:
    """22.50 -> '22.5', 4.00 -> '4', 22,50 -> '22.5'. Geeft None als leeg."""
    if value is None:
        return None
    s = str(value).replace(",", ".")
    try:
        f = float(s)
        result = f"{f:.10f}".rstrip("0").rstrip(".")
        return result
    except ValueError:
        return s


def slug_for_tag(s: str) -> str:
    lower = s.lower().strip()
    if lower in TAG_OVERRIDES:
        return TAG_OVERRIDES[lower]
    # & en , vervangen door spatie, dan ALLE whitespace collapsen tot één _
    cleaned = re.sub(r"[&,]", " ", lower)
    cleaned = re.sub(r"\s+", "_", cleaned).strip("_")
    return cleaned


def build_tags(hoofdcat: str, subcat: str, subsubcat: str, fase: str = "",
               batch_tag: str = "", extra_tags: list = None) -> str:
    """Tags voor Shopify, comma-zonder-spatie zoals in fase 3 referentie.

    `batch_tag` overschrijft de standaard `structuur_fase{fase}` als die is meegegeven.
    `extra_tags` — lijst van extra tags die worden toegevoegd (bv. voor dual indoor/outdoor).
    """
    parts = [
        f"cat_{slug_for_tag(hoofdcat)}" if hoofdcat else "",
        f"cat_{slug_for_tag(subcat)}"   if subcat   else "",
        f"cat_{slug_for_tag(subsubcat)}" if subsubcat else "",
    ]
    # Extra tags toevoegen (bv. cat_bloempotten_binnen bij outdoor potten)
    if extra_tags:
        for t in extra_tags:
            tag = f"cat_{slug_for_tag(t)}" if not t.startswith("cat_") else t
            if tag not in parts:
                parts.append(tag)
    parts.append(batch_tag if batch_tag else (f"structuur_fase{fase}" if fase else ""))
    return ",".join(p for p in parts if p)


def build_meta_description(product: dict) -> str:
    """
    Bouwt meta description deterministisch op basis van productdata.

    Format:
        {Productnaam NL} van {Designer} by {Merk}. {Materiaal NL}, {kleur NL}. {Subcategorie}. H {h} x L {l} x B {b} cm

    Voorbeelden:
        Kop + schotel lichtblauw rêves de rêves van Waww La Table by Serax. Porselein, blauw. Kommen, Mokken & Bekers. H 8.2 x L 7.9 x B 7.9 cm
        Bucket XS van Pottery Pots. Steengoed, zwart. Potten. H 5 x L 13.8 x B 13.8 cm

    Afgekapt op 160 tekens (Shopify SEO limiet).
    """
    # Productnaam: voorkeur NL, fallback raw
    naam = _fix_set_namen((product.get("_product_name_nl") or product.get("product_name_raw") or "").strip())
    designer = (product.get("designer") or "").strip()
    merk = (product.get("_merk") or product.get("vendor") or "Serax").strip()
    materiaal = (product.get("_materiaal_nl_translated") or product.get("materiaal_nl") or "").strip()
    kleur = (product.get("_kleur_nl_translated") or "").strip()
    subcat = (product.get("subcategorie") or "").strip()

    h = product.get("hoogte_cm")
    l = product.get("lengte_cm")
    b = product.get("breedte_cm")

    # Bouw delen op
    delen = []

    # Deel 1: "{naam} van {designer} by {merk}" of "{naam} van {merk}"
    if designer:
        delen.append(f"{naam} van {designer} by {merk}")
    elif naam:
        delen.append(f"{naam} van {merk}")
    else:
        delen.append(merk)

    # Deel 2: "{materiaal}, {kleur}" of alleen materiaal of alleen kleur
    mat_kleur_parts = [p for p in [materiaal, kleur.lower() if kleur else ""] if p]
    if mat_kleur_parts:
        delen.append(", ".join(mat_kleur_parts))

    # Deel 3: subcategorie
    if subcat:
        delen.append(subcat)

    # Deel 4: afmetingen
    if h and l and b:
        delen.append(f"H {clean_decimal(h)} x L {clean_decimal(l)} x B {clean_decimal(b)} cm")

    meta = ". ".join(delen)

    # Afkappen op 160 tekens
    if len(meta) > 160:
        meta = meta[:157] + "..."

    return meta


def build_page_title(product: dict) -> str:
    """
    Shopify SEO page title, max 70 tekens.
    Format: "{productnaam NL} | Interieur Shop"

    Als de productnaam te lang is wordt die afgekapt zodat het totaal ≤ 70 tekens is.
    """
    suffix = " | Interieur Shop"  # 17 tekens
    max_naam = 70 - len(suffix)   # 53 tekens

    nl_name = _fix_set_namen((product.get("_product_name_nl") or product.get("product_name_raw") or "").strip())

    if len(nl_name) > max_naam:
        nl_name = nl_name[:max_naam - 3].rstrip() + "..."

    return f"{nl_name}{suffix}"


def generate_handle(title_nl: str) -> str:
    """'Serax - Marie Michielssen - SPIEGEL 01 WIT ROSIE' -> 'marie-michielssen-spiegel-01-wit-rosie'"""
    h = title_nl.lower()
    h = re.sub(r"^serax\s*-\s*", "", h)
    h = re.sub(r"[^\w\s-]", "", h)
    h = re.sub(r"[\s_]+", "-", h).strip("-")
    return h


def translate_material(raw: str, claude) -> str:
    if not raw:
        return ""
    lower = raw.lower().strip()

    # Composiet materialen
    if "+" in lower or "&" in lower:
        parts = re.split(r"[+&]", lower)
        translated = []
        for p in parts:
            p = p.strip()
            t = MATERIAAL_NL.get(p) or ask_claude_translate(p, "materiaalsoort", claude)
            translated.append(t)
        return " & ".join(translated)

    if lower in MATERIAAL_NL:
        return MATERIAAL_NL[lower]

    return ask_claude_translate(raw, "materiaalsoort", claude)


def translate_color(raw_en: str, product_name_raw: str, claude) -> tuple[str, str]:
    """
    Geeft (kleur_filter, kleur_titel).
    Lamp-uitzondering: Paloma/Catherine -> Color-veld negeren.
    """
    name_upper = (product_name_raw or "").upper()

    if any(lamp in name_upper for lamp in LAMP_EXCEPTIONS):
        # Kleur staat in productnaam, niet in Color-veld — transform.py zet dit op review
        return "", ""

    if not raw_en or raw_en.strip() == "":
        return "", ""

    # Multi-kleur: split op "/"
    if "/" in raw_en:
        parts = [p.strip() for p in raw_en.split("/")]
        filters = []
        titels  = []
        for p in parts:
            f, t = _single_color(p, claude)
            filters.append(f)
            titels.append(t)
        return " / ".join(filters), " / ".join(titels)

    return _single_color(raw_en.strip(), claude)


def _single_color(raw: str, claude) -> tuple[str, str]:
    lower = raw.lower()
    if lower in KLEUR_FILTER:
        kleur_filter = KLEUR_FILTER[lower]
        # Bewaar specifieke naam in titel als die in de override-lijst staat
        kleur_titel = raw.upper() if lower in KLEUR_PRESERVE_IN_TITLE else kleur_filter.upper()
        return kleur_filter, kleur_titel

    # Claude fallback
    nl = ask_claude_translate(raw, "kleur", claude)
    return nl, nl.upper()


def ask_claude_translate(term: str, context: str, claude) -> str:
    response = claude.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=60,
        messages=[{
            "role": "user",
            "content": f"Vertaal dit {context} naar het Nederlands (alleen het vertaalde woord/begrip, geen uitleg): {term}"
        }],
    )
    return response.content[0].text.strip()


def vertaal_productnamen_batch(namen: list[str], claude=None) -> dict[str, str]:
    """
    Vertaal een lijst Engelse productnamen naar het Nederlands in ÉÉN Claude-call.

    Gebruikt Haiku 4.5 omdat productnaam-vertaling een eenvoudige taak is — orden
    van magnitude goedkoper dan Opus voor batch-verwerking. Voor 50 producten:
    ~700 input + 500 output tokens = ongeveer €0,003 per call.

    Returns: dict {EN_naam: NL_naam}. Eigennamen (collectie-namen, designer-namen)
    worden bewaard. Als parsing faalt geeft de functie een ValueError.
    """
    if not namen:
        return {}
    if claude is None:
        claude = get_claude()

    # Dedupliceer met behoud van volgorde
    uniek = list(dict.fromkeys(n.strip() for n in namen if n and n.strip()))
    if not uniek:
        return {}

    prompt = (
        f"Vertaal deze {len(uniek)} Engelse productnamen naar het Nederlands voor "
        "een Belgische webshop (Serax/homeware).\n\n"
        "REGELS:\n"
        "- BEHOUD eigennamen (collectie- of designer-namen) zoals Sophia, Rosie, "
        "Carte Blanche, Nabucho, Charly, Tarte de Bobonne onveranderd\n"
        "- Gebruik Title Case (eerste letter van elk woord groot, behalve van/de/het/en)\n"
        "- Maatcodes blijven uppercase: XS, S, M, L, XL\n"
        "- Vertaal generieke productwoorden: Plate→Bord, Deep Plate→Diep Bord, "
        "Dessert Plate→Dessertbord, Breakfast Plate→Ontbijtbord, Bowl→Kom, "
        "Mirror→Spiegel, Cup→Kopje, Mug→Mok, Jug→Kan, Pot→Pot, "
        "Storage Pot→Voorraadpot, Vase→Vaas, Glass→Glas, Candle→Kaars, "
        "Tray→Dienblad, Startset→Serviesset, Starter Set→Serviesset, "
        "Giftset→Cadeauset, Gift Set→Cadeauset, etc.\n"
        "- Vertaal kleuren: White→Wit, Black→Zwart, Beige→Beige, Blue→Blauw, "
        "Green→Groen, Red→Rood, Yellow→Geel, Grey→Grijs, Brown→Bruin, Pink→Roze\n"
        "- Schuine streep tussen meerdere kleuren (Beige Blue → Beige/Blauw, "
        "White/Black → Wit/Zwart)\n"
        "- Vul afkortingen aan tot vol woord (Soph → Sophia)\n"
        "- Output: ÉÉN regel per productnaam, in DEZELFDE volgorde, "
        "ZONDER nummering of commentaar of lege regels ertussen\n\n"
        "INPUT:\n"
        + "\n".join(uniek)
        + "\n\nOUTPUT:"
    )

    # ~12 tokens per vertaalde naam + marge; minimum 4000
    estimated_output = max(4000, len(uniek) * 16)
    response = claude.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=estimated_output,
        messages=[{"role": "user", "content": prompt}],
    )

    output_text = response.content[0].text.strip()
    output_lines = [line.strip() for line in output_text.split("\n") if line.strip()]

    if len(output_lines) != len(uniek):
        raise ValueError(
            f"Vertaling-mismatch: {len(uniek)} input-namen maar {len(output_lines)} "
            f"output-regels. Eerste 500 tekens van Claude output:\n{output_text[:500]}"
        )

    return {k: _fix_set_namen(v) for k, v in zip(uniek, output_lines)}


def _safe_int(value, default: int = 0) -> int:
    """Robuuste int-conversie: handelt strings als '4.0' / '4,5' / None / 'nan' aan."""
    if value is None:
        return default
    s = str(value).strip().replace(",", ".")
    if s == "" or s.lower() == "nan":
        return default
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return default


def _smart_title(s: str) -> str:
    """
    Title Case maar dan slim:
      - Eerste woord altijd met hoofdletter
      - Kleine woorden (van, de, het, en, of, met, in, op, &) blijven lowercase
      - Maatcodes (XS, S, M, L, XL) blijven uppercase, ook met komma eraan
      - Slash-gescheiden woorden worden apart getitlecased (White/Black)
    """
    if not s:
        return s
    kleine_woorden = {"van", "de", "het", "en", "of", "met", "in", "op", "for", "the", "a", "an", "&"}
    maat_codes = {"XS", "S", "M", "L", "XL", "XXL", "XXS"}

    def _title_word(w, is_first):
        # Strip leestekens om maatcodes te herkennen (bv. "XS," → "XS")
        stripped = w.rstrip(".,;:!?")
        suffix = w[len(stripped):]
        if stripped.upper() in maat_codes:
            return stripped.upper() + suffix
        if stripped.lower() in kleine_woorden and not is_first:
            return stripped.lower() + suffix
        # Slash-gescheiden: White/Black
        if "/" in w:
            parts = w.split("/")
            return "/".join(p.capitalize() for p in parts)
        return w.capitalize()

    woorden = s.split()
    return " ".join(_title_word(w, i == 0) for i, w in enumerate(woorden))


def build_title(product: dict) -> str:
    """
    Producttitel in Title Case (professioneel/luxe):
        Serax - Marie Michielssen - Dessertbord Wit/Zwart Carte Blanche - Set van 2
        Pottery Pots - Bloempot Binnen Bucket XS, Black

    Voorkeur voor productnaam:
      1. `_product_name_nl`  — vertaalde NL versie
      2. `product_name_raw`  — raw EN naam uit masterdata, fallback

    Set-segment komt aan het eind, met " - " separator.
    """
    nl_name = (product.get("_product_name_nl") or "").strip()
    raw_name = (product.get("product_name_raw") or "").strip()
    name_for_title = _fix_set_namen(_smart_title(nl_name or raw_name))
    designer    = (product.get("designer") or "").strip()
    collectie   = (product.get("collectie") or "").strip()

    # Verwijder Tableware/Glassware suffix uit collectie (legacy SOP)
    collectie = re.sub(r"\b(Tableware|Glassware)\b", "", collectie, flags=re.IGNORECASE).strip()

    # OWL VASE uitzondering
    name_check = (nl_name or raw_name).upper()
    if "OWL VASE" in name_check:
        product["designer"] = "Marni"
        designer = "Marni"
        product_deel = name_for_title.replace("Owl Vase", "Uil Vaas").replace("OWL VASE", "Uil Vaas").strip()
    # Lamp-uitzondering
    elif any(lamp in name_check for lamp in LAMP_EXCEPTIONS):
        lamp_type = "Wandlamp" if "WALL" in name_check else "Tafellamp"
        product_deel = f"{lamp_type} {name_for_title}".strip()
    else:
        product_deel = name_for_title

    # Sub-subcategorie toevoegen als producttype niet duidelijk uit de naam.
    # Check op twee manieren:
    #   1. Bekende producttype-woorden in de naam (plate, bowl, pot, etc.)
    #   2. De sub-subcat naam (of stam ervan) zit al in de productnaam
    #      → voorkomt "Broodtrommels Broodtrommel" of "Placemats Placemat"
    subsubcat = _fix_set_namen((product.get("sub_subcategorie") or "").strip())
    extra_tags = product.get("_extra_tags") or []
    if subsubcat and product_deel:
        producttype_hints = {
            "plate", "bord", "bowl", "kom", "cup", "kopje", "mug", "mok",
            "glass", "glas", "vase", "vaas", "pot", "lamp", "mirror", "spiegel",
            "jug", "kan", "tray", "dienblad", "candle", "kaars", "schaal",
            "stoel", "stool", "chair", "sofa", "tafel", "table", "spel", "game",
            "album", "boek", "book", "puzzel", "puzzle", "placemat", "onderzetter",
            "broodtrommel", "voorraadpot", "zeeppompje", "zeep", "karaf",
            "theepot", "suikerpot", "eierdop", "botervloot", "kandelaar",
            "serviesset", "cadeauset",
        }
        naam_lower = product_deel.lower()
        producttype_duidelijk = any(hint in naam_lower for hint in producttype_hints)

        # Extra check: zit de stam van de sub-subcat al in de productnaam?
        # "Broodtrommels" → stam "broodtrommel" → zit in "broodtrommel 33,5x21"
        if not producttype_duidelijk:
            subsubcat_lower = subsubcat.lower()
            # Neem de stam: verwijder trailing s/en/n voor meervoud
            stam = re.sub(r"(en|s|n)$", "", subsubcat_lower).strip()
            if stam and len(stam) > 3 and stam in naam_lower:
                producttype_duidelijk = True

        if not producttype_duidelijk:
            titel_subsubcat = subsubcat
            if extra_tags and subsubcat.lower().startswith("bloempotten"):
                titel_subsubcat = "Bloempot"
            product_deel = f"{_smart_title(titel_subsubcat)} {product_deel}"

    # Giftbox set — robuste qty conversie
    qty = _safe_int(product.get("giftbox_qty"))
    is_set = (
        str(product.get("giftbox") or "").upper() == "YES"
        and qty > 1
    )

    # Merknaam: uit het product-record, default "Serax"
    merk = (product.get("_merk") or product.get("vendor") or "Serax").strip()

    # Drie/vier segmenten samengevoegd met " - "
    delen = [merk]
    if designer:
        delen.append(designer)
    if product_deel:
        delen.append(product_deel)
    if is_set:
        delen.append(f"Set van {qty}")
    return " - ".join(delen)


def resolve_pricing(product: dict) -> tuple[float | None, float | None]:
    """Verkoopprijs + inkoopprijs.

    BELANGRIJK: voor giftbox-sets (qty > 1) moet de prijs in seo_products AL de
    giftbox-prijs zijn, niet de stuksprijs. Dit wordt afgedwongen bij het inladen
    van de prijslijst (setup_masterdata.py / Masterdata tab). De resolve_pricing
    functie returnt altijd rrp_stuk_eur/inkoopprijs_stuk_eur — die bevatten al de
    juiste prijs (stuk OF giftbox, afhankelijk van het product).
    """
    return product.get("rrp_stuk_eur"), product.get("inkoopprijs_stuk_eur")


def lookup_category(sb, leverancier_category: str, leverancier_item_cat: str):
    """Zoek exacte match in seo_category_mapping. Geeft None als geen match."""
    result = sb.table("seo_category_mapping").select("*").eq(
        "leverancier_category", leverancier_category
    ).eq(
        "leverancier_item_cat", leverancier_item_cat
    ).execute()
    return result.data[0] if result.data else None


def validate_against_website(sb, field_type: str, waarde: str) -> bool:
    """Controleert of een filterwaarde al bestaat op de website."""
    if not waarde:
        return True
    result = sb.table("seo_filter_values").select("id").eq("type", field_type).eq(
        "waarde", waarde
    ).execute()
    return bool(result.data)


# ── Hoofdfunctie ─────────────────────────────────────────────────────────────

def transform(fase: str, limit: int | None = None):
    sb     = get_supabase()
    claude = get_claude()

    query = sb.table("seo_products").select("*").eq("status", "raw").eq("fase", fase)
    if limit:
        query = query.limit(limit)
    result = query.execute()
    products = result.data

    if not products:
        print(f"Geen producten met status='raw' gevonden voor fase {fase}.")
        return

    if limit:
        print(f"TESTRUN: max {limit} producten\n")
    print(f"Verwerken: {len(products)} producten (fase {fase})\n")

    ready = review = errors = 0
    nieuwe_filterwaarden = []
    twijfelgevallen = []

    for product in products:
        pid = product["id"]
        sku = product.get("sku", pid)
        updates = {}
        review_redenen = []

        try:
            # 1. Categorie
            cat_row = lookup_category(
                sb,
                product.get("leverancier_category", ""),
                product.get("leverancier_item_cat", ""),
            )
            if cat_row:
                updates["hoofdcategorie"]   = cat_row["hoofdcategorie"]
                updates["subcategorie"]     = cat_row["subcategorie"]
                updates["sub_subcategorie"] = cat_row["sub_subcategorie"]
                updates["collectie"]        = cat_row["subcategorie"]  # collectie = subcategorie
            else:
                review_redenen.append("categorie niet gevonden in mapping-tabel")
                twijfelgevallen.append({
                    "type": "categorie",
                    "sku":  sku,
                    "info": f"Leverancier: {product.get('leverancier_category')} / {product.get('leverancier_item_cat')}",
                    "pid":  pid,
                })

            # 2. Tags
            updates["tags"] = build_tags(
                updates.get("hoofdcategorie", ""),
                updates.get("subcategorie", ""),
                updates.get("sub_subcategorie", ""),
                fase,
            )

            # 3. Materiaal vertalen
            materiaal_nl = translate_material(product.get("materiaal_nl", ""), claude)
            updates["materiaal_nl"] = materiaal_nl
            if materiaal_nl and not validate_against_website(sb, "materiaal", materiaal_nl):
                nieuwe_filterwaarden.append(f"materiaal: {materiaal_nl} (SKU: {sku})")

            # 4. Kleur vertalen
            kleur_filter, kleur_titel = translate_color(
                product.get("kleur_en", ""),
                product.get("product_name_raw", ""),
                claude,
            )
            updates["kleur_nl"] = kleur_filter
            updates["_kleur_titel"] = kleur_titel  # tijdelijk, voor titel
            if kleur_filter and not validate_against_website(sb, "kleur", kleur_filter):
                nieuwe_filterwaarden.append(f"kleur: {kleur_filter} (SKU: {sku})")

            # Lamp-uitzondering: kleur uit productnaam -> review
            name_upper = (product.get("product_name_raw") or "").upper()
            if any(lamp in name_upper for lamp in LAMP_EXCEPTIONS):
                review_redenen.append(
                    "lamp-uitzondering (Paloma/Catherine): controleer kleur en titel handmatig"
                )

            # 5. Producttitel
            product.update(updates)
            titel = build_title(product)
            updates["product_title_nl"] = titel
            updates["handle"] = generate_handle(titel)

            # 6. Prijslogica
            verkoopprijs, inkoopprijs = resolve_pricing(product)
            if verkoopprijs is None or verkoopprijs == 0:
                review_redenen.append("verkoopprijs is 0 of ontbreekt")
            updates["verkoopprijs"] = verkoopprijs
            updates["inkoopprijs"]  = inkoopprijs

            # 7. Decimalen opschonen
            for dim in ["hoogte_cm", "lengte_cm", "breedte_cm"]:
                val = product.get(dim)
                if val is not None:
                    cleaned = clean_decimal(val)
                    updates[dim] = float(cleaned) if cleaned else None

            # 8. Meta description (via Claude)
            h = product.get("hoogte_cm") or updates.get("hoogte_cm") or ""
            l = product.get("lengte_cm") or updates.get("lengte_cm") or ""
            b = product.get("breedte_cm") or updates.get("breedte_cm") or ""
            afm = f"{h} x {l} x {b} cm" if all([h, l, b]) else ""

            meta_prompt = (
                f"Schrijf een Nederlandse SEO meta description voor dit product. "
                f"Strikt max 160 tekens. Schrijf een natuurlijke zin, geen opsomming.\n"
                f"Formaat: [Producttype] van [Designer] by Serax. [Materiaal], [kleur]. [Subcategorie]. {afm}\n\n"
                f"Producttype: {updates.get('sub_subcategorie', '')}\n"
                f"Designer: {product.get('designer', '')}\n"
                f"Materiaal: {materiaal_nl}\n"
                f"Kleur: {kleur_filter}\n"
                f"Subcategorie: {updates.get('subcategorie', '')}\n"
                f"Afmetingen: {afm}\n\n"
                f"Geef alleen de meta description terug."
            )
            meta_resp = claude.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=200,
                messages=[{"role": "user", "content": meta_prompt}],
            )
            updates["meta_description"] = meta_resp.content[0].text.strip()[:160]

            # Status
            if review_redenen:
                updates["status"]       = "review"
                updates["review_reden"] = "; ".join(review_redenen)
                review += 1
            else:
                updates["status"] = "ready"
                ready += 1

            # Verwijder tijdelijk veld
            updates.pop("_kleur_titel", None)

            sb.table("seo_products").update(updates).eq("id", pid).execute()
            symbol = "~" if updates["status"] == "review" else "+"
            print(f"  {symbol} {sku} -> {updates['status']}")

        except Exception as e:
            print(f"  ! {sku}: fout -- {e}", file=sys.stderr)
            sb.table("seo_products").update({
                "status":       "review",
                "review_reden": f"Technische fout: {e}",
            }).eq("id", pid).execute()
            errors += 1

    # Eindrapport
    print(f"""
Transform fase {fase}:
  + {ready} producten klaar (ready)
  ~ {review} producten voor controle (review)
  ! {errors} technische fouten
""")

    if nieuwe_filterwaarden:
        print(f"  LET OP: {len(nieuwe_filterwaarden)} nieuwe filterwaarden — aanmaken in Shopify voor import:")
        for w in nieuwe_filterwaarden:
            print(f"    - {w}")
        print()

    if twijfelgevallen:
        print(f"  {len(twijfelgevallen)} categorie-twijfelgeval(len):")
        for t in twijfelgevallen:
            print(f"    - SKU: {t['sku']} | {t['info']}")
        print("  Voeg de juiste mapping toe aan seo_category_mapping en herrun transform.py.")


def transform_batch(
    ids: list[int] | None = None,
    fase: str | None = None,
    limit: int | None = None,
    progress: Callable[[int, int, str], None] | None = None,
    logger: Callable[[str], None] | None = None,
) -> TransformResult:
    """Pure-function variant of :func:`transform` for the Streamlit dashboard.

    Two modes of selection:
      - ``ids=[...]`` → transform only these product IDs (any fase). This is
        the mode the dashboard uses to enforce the "max 25 per batch" cap.
      - ``ids=None, fase='4'`` → transform all products with status='raw' in
        that fase. Equivalent to the CLI call.

    Learnings from ``seo_learnings`` are applied on top of the hardcoded
    base rules. Returns a :class:`TransformResult` with counters.
    """
    log = logger or print
    sb = get_supabase()
    claude = get_claude()

    # Select products
    if ids is not None:
        if not ids:
            return TransformResult()
        res = sb.table("seo_products").select("*").in_("id", ids).execute()
        products = res.data or []
    else:
        if not fase:
            raise TransformError("Either ids or fase must be given")
        q = sb.table("seo_products").select("*").eq("status", "raw").eq("fase", fase)
        if limit:
            q = q.limit(limit)
        products = q.execute().data or []

    result = TransformResult(total=len(products))
    if not products:
        log("Geen producten om te transformeren.")
        return result

    # Load learnings once
    all_learnings = load_active_learnings(sb)
    cat_learnings = [L for L in all_learnings if L.get("stap") == "categorie"]
    extra_mat, extra_kl = apply_translation_learnings(
        [L for L in all_learnings if L.get("stap") == "vertaling"]
    )
    log(f"Actieve learnings: {len(all_learnings)} totaal "
        f"(categorie: {len(cat_learnings)}, vertalingen: {len(extra_mat) + len(extra_kl)})")

    for idx, product in enumerate(products):
        if progress:
            try:
                progress(idx, len(products), f"SKU {product.get('sku')}")
            except Exception:
                pass

        pid = product["id"]
        sku = product.get("sku") or pid
        updates: dict = {}
        review_redenen: list[str] = []

        try:
            # 1. Categorie (mapping-tabel)
            cat_row = lookup_category(
                sb,
                product.get("leverancier_category", ""),
                product.get("leverancier_item_cat", ""),
            )
            if cat_row:
                updates["hoofdcategorie"] = cat_row["hoofdcategorie"]
                updates["subcategorie"] = cat_row["subcategorie"]
                updates["sub_subcategorie"] = cat_row["sub_subcategorie"]
                updates["collectie"] = cat_row["subcategorie"]
            else:
                review_redenen.append("categorie niet gevonden in mapping-tabel")
                result.twijfelgevallen.append({
                    "type": "categorie",
                    "sku": sku,
                    "info": f"Leverancier: {product.get('leverancier_category')} / {product.get('leverancier_item_cat')}",
                    "pid": pid,
                })

            # 1b. Apply name_rule learnings (may overwrite or add extra sub_subcategorie)
            applied = apply_name_rules(product, updates, cat_learnings)
            result.learnings_applied += applied

            # 2. Tags
            updates["tags"] = build_tags(
                updates.get("hoofdcategorie", ""),
                updates.get("subcategorie", ""),
                updates.get("sub_subcategorie", ""),
                product.get("fase", ""),
            )

            # 3. Materiaal vertalen (met learnings-extensie)
            raw_mat = product.get("materiaal_nl", "") or ""
            if extra_mat and raw_mat.strip().lower() in extra_mat:
                materiaal_nl = extra_mat[raw_mat.strip().lower()]
            else:
                materiaal_nl = translate_material(raw_mat, claude)
            updates["materiaal_nl"] = materiaal_nl
            if materiaal_nl and not validate_against_website(sb, "materiaal", materiaal_nl):
                result.new_filter_values.append(f"materiaal: {materiaal_nl} (SKU: {sku})")

            # 4. Kleur vertalen
            raw_kl = product.get("kleur_en", "") or ""
            if extra_kl and raw_kl.strip().lower() in extra_kl:
                kleur_filter = extra_kl[raw_kl.strip().lower()]
                kleur_titel = kleur_filter
            else:
                kleur_filter, kleur_titel = translate_color(
                    raw_kl, product.get("product_name_raw", ""), claude,
                )
            updates["kleur_nl"] = kleur_filter
            updates["_kleur_titel"] = kleur_titel
            if kleur_filter and not validate_against_website(sb, "kleur", kleur_filter):
                result.new_filter_values.append(f"kleur: {kleur_filter} (SKU: {sku})")

            name_upper = (product.get("product_name_raw") or "").upper()
            if any(lamp in name_upper for lamp in LAMP_EXCEPTIONS):
                review_redenen.append("lamp-uitzondering (Paloma/Catherine): controleer kleur en titel handmatig")

            # 5. Producttitel
            product.update(updates)
            titel = build_title(product)
            updates["product_title_nl"] = titel
            updates["handle"] = generate_handle(titel)

            # 6. Prijslogica
            verkoopprijs, inkoopprijs = resolve_pricing(product)
            if verkoopprijs is None or verkoopprijs == 0:
                review_redenen.append("verkoopprijs is 0 of ontbreekt")
            updates["verkoopprijs"] = verkoopprijs
            updates["inkoopprijs"] = inkoopprijs

            # 7. Decimalen opschonen
            for dim in ["hoogte_cm", "lengte_cm", "breedte_cm"]:
                val = product.get(dim)
                if val is not None:
                    cleaned = clean_decimal(val)
                    updates[dim] = float(cleaned) if cleaned else None

            # 8. Meta description
            h = product.get("hoogte_cm") or updates.get("hoogte_cm") or ""
            l = product.get("lengte_cm") or updates.get("lengte_cm") or ""
            b = product.get("breedte_cm") or updates.get("breedte_cm") or ""
            afm = f"{h} x {l} x {b} cm" if all([h, l, b]) else ""
            meta_prompt = (
                f"Schrijf een Nederlandse SEO meta description voor dit product. "
                f"Strikt max 160 tekens. Schrijf een natuurlijke zin, geen opsomming.\n"
                f"Formaat: [Producttype] van [Designer] by Serax. [Materiaal], [kleur]. [Subcategorie]. {afm}\n\n"
                f"Producttype: {updates.get('sub_subcategorie', '')}\n"
                f"Designer: {product.get('designer', '')}\n"
                f"Materiaal: {materiaal_nl}\n"
                f"Kleur: {kleur_filter}\n"
                f"Subcategorie: {updates.get('subcategorie', '')}\n"
                f"Afmetingen: {afm}\n\n"
                f"Geef alleen de meta description terug."
            )
            meta_resp = claude.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=200,
                messages=[{"role": "user", "content": meta_prompt}],
            )
            updates["meta_description"] = meta_resp.content[0].text.strip()[:160]

            # Status
            if review_redenen:
                updates["status"] = "review"
                updates["review_reden"] = "; ".join(review_redenen)
                result.review += 1
            else:
                updates["status"] = "ready"
                result.ready += 1

            # Cleanup tijdelijke velden vóór write
            updates.pop("_kleur_titel", None)
            updates.pop("_extra_tags", None)

            sb.table("seo_products").update(updates).eq("id", pid).execute()
            result.processed_ids.append(pid)
            symbol = "~" if updates["status"] == "review" else "+"
            log(f"  {symbol} {sku} -> {updates['status']}")

        except Exception as e:
            log(f"  ! {sku}: fout -- {e}")
            try:
                sb.table("seo_products").update({
                    "status": "review",
                    "review_reden": f"Technische fout: {e}",
                }).eq("id", pid).execute()
            except Exception:
                pass
            result.errors += 1

    if progress:
        try:
            progress(len(products), len(products), "klaar")
        except Exception:
            pass

    log(f"\nTransform klaar — ready: {result.ready}, review: {result.review}, "
        f"errors: {result.errors}, learnings toegepast: {result.learnings_applied}")

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fase", required=True, help="Fasecode, bijv. 3")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max aantal producten verwerken (testrun)")
    args = parser.parse_args()

    transform(args.fase, args.limit)
