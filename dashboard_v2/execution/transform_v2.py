"""Transform v2 — werkt op nieuwe Supabase schema (products_raw + products_curated).

Stappen per product:
  1. Categorisatie via seo_category_mapping (leverancier_category + leverancier_item_cat → onze categorie)
  2. Materiaal vertalen (lookup-tabel + Sonnet fallback)
  3. Kleur vertalen
  4. Producttitel bouwen (NL)
  5. Tags genereren
  6. Meta description (Sonnet)
  7. Validatie + status

Leest uit: products_raw + bestaande products_curated (voor pipeline_status)
Schrijft naar: products_curated (upsert op sku of raw_id)

Gebruikt seo_category_mapping en seo_learnings uit dezelfde Supabase.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Callable

from dotenv import load_dotenv

load_dotenv()


@dataclass
class TransformResult:
    ready: int = 0
    review: int = 0
    errors: int = 0
    total: int = 0
    new_filter_values: list[str] = field(default_factory=list)
    twijfelgevallen: list[dict] = field(default_factory=list)
    learnings_applied: int = 0
    processed_skus: list[str] = field(default_factory=list)


# ── Vertaaltabellen (overgenomen uit transform.py) ────────────────────────────

MATERIAAL_NL = {
    "stoneware": "Steengoed", "ceramic": "Keramiek", "ceramics": "Keramiek",
    "fiberstone": "Fiberstone", "fibercite": "Fiberstone", "fiberclay": "Fiberclay",
    "ficonstone": "Ficonstone", "sandstone": "Steen", "porcelain": "Porselein",
    "fine bone china": "Porselein", "bone china porcelain": "Porselein",
    "bone china": "Porselein", "terracotta": "Terracotta", "earthenware": "Aardewerk",
    "concrete": "Beton", "cement": "Beton", "marble": "Steen", "stone": "Steen",
    "glass": "Glas", "borosilicate glass": "Glas", "potassium glass": "Glas",
    "crystal": "Glas", "fiberglass": "Glasvezel", "glass fiber": "Glasvezel",
    "glass fibre": "Glasvezel", "metal": "Metaal", "steel": "Metaal",
    "stainless steel": "Metaal", "aluminium": "Metaal", "aluminum": "Metaal",
    "cast iron": "Metaal", "iron": "Metaal", "copper": "Metaal", "brass": "Metaal",
    "zinc": "Metaal", "wood": "Hout", "ash": "Hout", "carbonised ash": "Hout",
    "oak": "Hout", "walnut": "Hout", "acacia": "Hout", "linen": "Linnen",
    "cotton": "Katoen", "velvet": "Fluweel", "leather": "Leer",
    "silk and polyester": "Polyester", "polyester": "Polyester",
    "plastic": "Kunststof", "polyethylene": "Kunststof", "polypropylene": "Kunststof",
    "resin": "Kunststof", "rattan": "Rotan", "bamboo": "Bamboe",
    "paper mache": "Papier", "paper": "Papier", "cardboard": "Papier",
    "paint": "Verf", "soy wax": "Kaarsvet", "parafine": "Kaarsvet",
    "paraffin": "Kaarsvet", "pot-feet": "Kunststof", "other": "Overig",
    "cement-bamboo": "Beton", "fiber-cement": "Fiberstone & Beton",
    "ro-cement": "Beton", "papier": "Papier", "hout": "Hout", "karton": "Papier",
    "metaal": "Metaal", "kunststof": "Kunststof", "glas": "Glas",
    "steengoed": "Steengoed", "porselein": "Porselein", "steen": "Steen",
    "beton": "Beton", "keramiek": "Keramiek", "katoen": "Katoen",
    "linnen": "Linnen", "abs": "Kunststof", "imitatieleer": "Leer",
    "polycarbonaat": "Kunststof", "polypropyleen": "Kunststof",
    "mdf": "Hout", "houtskool beuken": "Hout", "iron wire": "IJzer",
    "lead free crystal glass": "Kristal", "maple": "Hout", "terrazzo": "Steen",
    "imitation leather": "Leer", "new bone china": "Porselein",
    "iron;wood": "IJzer & Hout", "perfume": "Overig", "polyurethane": "Kunststof",
    "leer": "Leer", "bamboe": "Bamboe", "overig": "Overig",
}

KLEUR_FILTER = {
    "white": "Wit", "off white": "Wit", "off-white": "Wit", "white matt": "Wit",
    "matte white": "Wit", "glossy white": "Wit", "natural white": "Wit",
    "black": "Zwart", "volcano black": "Zwart", "weathered black": "Zwart",
    "white black": "Zwart & Wit", "black white": "Zwart & Wit",
    "grey": "Grijs", "gray": "Grijs", "dark grey": "Grijs", "light grey": "Grijs",
    "indi grey": "Grijs", "clouded grey": "Grijs", "anthracite": "Grijs",
    "beige": "Beige", "beige washed": "Beige", "travertine beige": "Beige",
    "sand": "Beige", "cream": "Beige", "ecru": "Beige", "taupe": "Beige",
    "brown": "Bruin", "imperial brown": "Bruin",
    "blue": "Blauw", "dark blue": "Blauw", "navy": "Blauw",
    "midnight blue": "Blauw", "light blue": "Blauw",
    "green": "Groen", "pine green": "Groen", "olive": "Groen", "sage": "Groen",
    "dark green": "Groen", "camo green": "Groen",
    "red": "Rood", "venetian red": "Rood", "rust": "Rood",
    "red white": "Rood & Wit",
    "pink": "Roze", "yellow": "Geel", "mustard": "Geel",
    "orange": "Oranje", "purple": "Paars",
    "gold": "Goud", "silver": "Zilver", "copper": "Koper",
    "transparent": "Transparant", "transparant": "Transparant", "clear": "Transparant",
    "ivory": "Ivoor", "mix": "Multi", "multi": "Multi", "terracotta": "Terracotta",
    "chalk white": "Wit", "clay washed": "Bruin", "diorite grey": "Grijs",
    "grey washed": "Grijs", "ivory washed": "Beige",
    "light grey (vertically ridged)": "Grijs", "mocha washed": "Bruin",
    "rustic green": "Groen", "silk white": "Wit", "smoky umber": "Bruin",
    "wabi beige": "Beige", "black washed": "Zwart", "midnight black": "Zwart",
    "satin black": "Zwart", "imperial white": "Wit", "chalk beige": "Beige",
    "mossy beige": "Beige", "sahara sand": "Beige", "sage green": "Groen",
    "umber brown": "Bruin", "root brown": "Bruin", "powder pink": "Roze",
    "brick orange": "Oranje", "turquoise": "Blauw", "bordeaux": "Rood",
    "white stripe": "Wit", "bamboo": "Beige", "straw grass": "Beige",
    "antracite - clear": "Grijs",
    "dark grey (horizontally ridged)": "Grijs",
    "light grey (horizontally ridged)": "Grijs",
    "dark grey (vertically ridged)": "Grijs",
    "wit": "Wit", "zwart": "Zwart", "grijs": "Grijs", "bruin": "Bruin",
    "blauw": "Blauw", "groen": "Groen", "rood": "Rood", "roze": "Roze",
    "geel": "Geel", "oranje": "Oranje", "paars": "Paars",
    "goud": "Goud", "zilver": "Zilver", "antraciet": "Grijs",
    "lichtblauw": "Blauw", "donkerblauw": "Blauw", "lichtgrijs": "Grijs",
    "ivoor": "Ivoor", "amber": "Bruin", "amber;black": "Bruin",
    "blue;green": "Blauw", "gold;black": "Goud",
}

KLEUR_PRESERVE_IN_TITLE = {"indi grey", "venetian red", "camo green"}

TAG_OVERRIDES = {
    "wijn & champagne": "wijn_champagne",
    "peper & zoutmolens": "peper-_zoutmolens",
}

LAMP_EXCEPTIONS = ["PALOMA", "CATHERINE"]


# ── Supabase + Claude clients ─────────────────────────────────────────────────

def get_supabase():
    from supabase import create_client
    url = os.getenv("SUPABASE_NEW_URL", "")
    key = os.getenv("SUPABASE_NEW_SERVICE_KEY", "") or os.getenv("SUPABASE_NEW_KEY", "")
    if not url or not key:
        raise RuntimeError("SUPABASE_NEW_URL/KEY ontbreekt.")
    return create_client(url, key)


def get_claude():
    import anthropic
    return anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))


# ── Learnings ─────────────────────────────────────────────────────────────────

def load_active_learnings(sb, stap: str | None = None) -> list[dict]:
    try:
        q = sb.table("seo_learnings").select("id,rule_type,action,scope,stap").eq("status", "applied")
        if stap is not None:
            q = q.eq("stap", stap)
        return q.execute().data or []
    except Exception:
        return []


def apply_name_rules(product: dict, updates: dict, learnings: list[dict]) -> int:
    naam = (product.get("product_name_raw") or "").lower()
    if not naam:
        return 0
    applied = 0
    extra_tags = list(updates.get("_extra_tags") or [])

    def _apply_one(rule: dict) -> None:
        nonlocal applied, extra_tags
        zoek = (rule.get("zoekwoord") or "").strip().lower()
        sub_sub = rule.get("sub_subcategorie") or ""
        if not zoek or not sub_sub or zoek not in naam:
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
            for r in (act.get("regels") or []):
                _apply_one(r)

    if extra_tags:
        updates["_extra_tags"] = extra_tags
    return applied


def apply_translation_learnings(learnings: list[dict]) -> tuple[dict, dict]:
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


def apply_title_learnings(naam: str, learnings: list[dict]) -> str:
    """Past title_strip + title_replace learnings toe op één naam.

    title_strip:    {"strip": ["Ferd Ridge", "Horace Ridge"]}
    title_replace:  {"replace": [{"from": "...", "to": "..."}]}
    title_instruction wordt apart in de Haiku-prompt geïnjecteerd via collect_title_instructions().
    """
    if not naam:
        return naam
    out = naam
    for L in learnings:
        if L.get("stap") != "titel":
            continue
        rt = L.get("rule_type")
        act = L.get("action") or {}
        if rt == "title_strip":
            for w in (act.get("strip") or []):
                if not w:
                    continue
                # case-insensitive verwijderen + opruimen van dubbele spaties/streepjes
                out = re.sub(rf"\s*[-–—]?\s*{re.escape(w)}\s*[-–—]?\s*", " ", out, flags=re.IGNORECASE)
        elif rt == "title_replace":
            for r in (act.get("replace") or []):
                fr, to = (r.get("from") or "").strip(), (r.get("to") or "").strip()
                if fr:
                    out = re.sub(re.escape(fr), to, out, flags=re.IGNORECASE)
    # opruimen
    out = re.sub(r"\s{2,}", " ", out).strip(" -–—")
    return out


def collect_title_instructions(learnings: list[dict]) -> list[str]:
    """Verzamelt title_instruction regels (vrije tekst voor Haiku-prompt)."""
    out: list[str] = []
    for L in learnings:
        if L.get("stap") != "titel" or L.get("rule_type") != "title_instruction":
            continue
        inst = (L.get("action") or {}).get("instruction", "").strip()
        if inst:
            out.append(inst)
    return out
    return extra_mat, extra_kl


# ── Hulpfuncties ──────────────────────────────────────────────────────────────

def _fix_set_namen(naam: str) -> str:
    naam = re.sub(r"\bStartsets\b", "Serviessets", naam, flags=re.IGNORECASE)
    naam = re.sub(r"\bStartset\b", "Serviesset", naam, flags=re.IGNORECASE)
    naam = re.sub(r"\bGiftsets\b", "Cadeausets", naam, flags=re.IGNORECASE)
    naam = re.sub(r"\bGiftset\b", "Cadeauset", naam, flags=re.IGNORECASE)
    naam = re.sub(r"\bGeschenksets\b", "Cadeausets", naam, flags=re.IGNORECASE)
    naam = re.sub(r"\bGeschenkset\b", "Cadeauset", naam, flags=re.IGNORECASE)
    return naam


def clean_decimal(value) -> str | None:
    if value is None:
        return None
    s = str(value).replace(",", ".")
    try:
        f = float(s)
        return f"{f:.10f}".rstrip("0").rstrip(".")
    except ValueError:
        return s


def slug_for_tag(s: str) -> str:
    lower = s.lower().strip()
    if lower in TAG_OVERRIDES:
        return TAG_OVERRIDES[lower]
    cleaned = re.sub(r"[&,]", " ", lower)
    cleaned = re.sub(r"\s+", "_", cleaned).strip("_")
    return cleaned


def build_tags(hoofdcat: str, subcat: str, subsubcat: str, fase: str = "",
               extra_tags: list = None) -> str:
    parts = [
        f"cat_{slug_for_tag(hoofdcat)}" if hoofdcat else "",
        f"cat_{slug_for_tag(subcat)}" if subcat else "",
        f"cat_{slug_for_tag(subsubcat)}" if subsubcat else "",
    ]
    if extra_tags:
        for t in extra_tags:
            tag = f"cat_{slug_for_tag(t)}" if not t.startswith("cat_") else t
            if tag not in parts:
                parts.append(tag)
    if fase:
        parts.append(f"structuur_fase{fase}")
    return ",".join(p for p in parts if p)


def _safe_int(value, default: int = 0) -> int:
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
    if not s:
        return s
    kleine = {"van", "de", "het", "en", "of", "met", "in", "op", "for", "the", "a", "an", "&"}
    maten = {"XS", "S", "M", "L", "XL", "XXL", "XXS"}

    def _w(w, first):
        stripped = w.rstrip(".,;:!?")
        suf = w[len(stripped):]
        if stripped.upper() in maten:
            return stripped.upper() + suf
        if stripped.lower() in kleine and not first:
            return stripped.lower() + suf
        if "/" in w:
            return "/".join(p.capitalize() for p in w.split("/"))
        return w.capitalize()

    return " ".join(_w(w, i == 0) for i, w in enumerate(s.split()))


def ask_claude_translate(term: str, context: str, claude) -> str:
    response = claude.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=60,
        messages=[{"role": "user",
                   "content": f"Vertaal dit {context} naar het Nederlands (alleen het vertaalde woord/begrip, geen uitleg): {term}"}],
    )
    return response.content[0].text.strip()


def translate_material(raw: str, claude, extra_mat: dict | None = None) -> str:
    if not raw:
        return ""
    lower = raw.lower().strip()
    if extra_mat and lower in extra_mat:
        return extra_mat[lower]
    if "+" in lower or "&" in lower:
        parts = re.split(r"[+&]", lower)
        translated = []
        for p in parts:
            p = p.strip()
            t = MATERIAAL_NL.get(p) or (extra_mat or {}).get(p) or ask_claude_translate(p, "materiaalsoort", claude)
            translated.append(t)
        return " & ".join(translated)
    if lower in MATERIAAL_NL:
        return MATERIAAL_NL[lower]
    return ask_claude_translate(raw, "materiaalsoort", claude)


def _single_color(raw: str, claude, extra_kl: dict | None = None) -> tuple[str, str]:
    lower = raw.lower()
    if extra_kl and lower in extra_kl:
        nl = extra_kl[lower]
        return nl, nl.upper()
    if lower in KLEUR_FILTER:
        f = KLEUR_FILTER[lower]
        t = raw.upper() if lower in KLEUR_PRESERVE_IN_TITLE else f.upper()
        return f, t
    nl = ask_claude_translate(raw, "kleur", claude)
    return nl, nl.upper()


def translate_color(raw_en: str, product_name_raw: str, claude,
                    extra_kl: dict | None = None) -> tuple[str, str]:
    name_upper = (product_name_raw or "").upper()
    if any(lamp in name_upper for lamp in LAMP_EXCEPTIONS):
        return "", ""
    if not raw_en or not raw_en.strip():
        return "", ""
    if "/" in raw_en:
        parts = [p.strip() for p in raw_en.split("/")]
        fs, ts = [], []
        for p in parts:
            f, t = _single_color(p, claude, extra_kl)
            fs.append(f)
            ts.append(t)
        return " / ".join(fs), " / ".join(ts)
    return _single_color(raw_en.strip(), claude, extra_kl)


def build_title(product: dict) -> str:
    """Producttitel: Merk - Designer - Naam - Set van X"""
    nl_name = (product.get("_product_name_nl") or "").strip()
    raw_name = (product.get("product_name_raw") or "").strip()
    name_for_title = _fix_set_namen(_smart_title(nl_name or raw_name))
    designer = (product.get("designer") or "").strip()
    name_check = (nl_name or raw_name).upper()

    if "OWL VASE" in name_check:
        designer = "Marni"
        product_deel = name_for_title.replace("Owl Vase", "Uil Vaas").replace("OWL VASE", "Uil Vaas").strip()
    elif any(lamp in name_check for lamp in LAMP_EXCEPTIONS):
        lamp_type = "Wandlamp" if "WALL" in name_check else "Tafellamp"
        product_deel = f"{lamp_type} {name_for_title}".strip()
    else:
        product_deel = name_for_title

    subsubcat = _fix_set_namen((product.get("sub_subcategorie") or "").strip())
    extra_tags = product.get("_extra_tags") or []
    if subsubcat and product_deel:
        hints = {
            "plate", "bord", "bowl", "kom", "cup", "kopje", "mug", "mok",
            "glass", "glas", "vase", "vaas", "pot", "lamp", "mirror", "spiegel",
            "jug", "kan", "tray", "dienblad", "candle", "kaars", "schaal",
            "stoel", "stool", "chair", "tafel", "table", "spel", "game",
            "album", "boek", "book", "puzzel", "puzzle", "placemat", "onderzetter",
            "broodtrommel", "voorraadpot", "zeeppompje", "karaf",
            "theepot", "suikerpot", "eierdop", "botervloot", "kandelaar",
            "serviesset", "cadeauset",
        }
        nl = product_deel.lower()
        duidelijk = any(h in nl for h in hints)
        if not duidelijk:
            stam = re.sub(r"(en|s|n)$", "", subsubcat.lower()).strip()
            if stam and len(stam) > 3 and stam in nl:
                duidelijk = True
        if not duidelijk:
            t = subsubcat
            if extra_tags and subsubcat.lower().startswith("bloempotten"):
                t = "Bloempot"
            product_deel = f"{_smart_title(t)} {product_deel}"

    qty = _safe_int(product.get("giftbox_qty"))
    is_set = str(product.get("giftbox") or "").upper() == "YES" and qty > 1
    merk = (product.get("supplier") or "Serax").strip()

    delen = [merk]
    if designer:
        delen.append(designer)
    if product_deel:
        delen.append(product_deel)
    if is_set:
        delen.append(f"Set van {qty}")
    return " - ".join(delen)


def generate_handle(title_nl: str) -> str:
    h = title_nl.lower()
    h = re.sub(r"^serax\s*-\s*", "", h)
    h = re.sub(r"[^\w\s-]", "", h)
    h = re.sub(r"[\s_]+", "-", h).strip("-")
    return h


# ── Categorie lookup ──────────────────────────────────────────────────────────

def lookup_category(sb, leverancier_category: str, leverancier_item_cat: str):
    """Zoek match in seo_category_mapping.

    Strategie:
    1. Exacte match op (leverancier_category, leverancier_item_cat)
    2. Als item_cat leeg/null: match op alleen leverancier_category (eerste treffer)
    3. Als item_cat gevuld maar geen exacte match: ook proberen op alleen leverancier_category
    """
    if not leverancier_category:
        return None
    try:
        # 1. Exacte match
        if leverancier_item_cat:
            res = sb.table("seo_category_mapping").select("*") \
                .eq("leverancier_category", leverancier_category) \
                .eq("leverancier_item_cat", leverancier_item_cat) \
                .execute()
            if res.data:
                return res.data[0]

        # 2. Fallback: alleen op leverancier_category (negeert item_cat)
        res2 = sb.table("seo_category_mapping").select("*") \
            .eq("leverancier_category", leverancier_category) \
            .execute()
        if res2.data:
            return res2.data[0]

        return None
    except Exception:
        return None


def validate_against_website(sb, field_type: str, waarde: str) -> bool:
    if not waarde:
        return True
    try:
        result = sb.table("seo_filter_values").select("id") \
            .eq("type", field_type).eq("waarde", waarde).execute()
        return bool(result.data)
    except Exception:
        return True  # niet falen als tabel leeg/onbeschikbaar


# ── Naam-vertaling (batch via Haiku) ──────────────────────────────────────────

def vertaal_productnamen_batch(
    namen: list[str],
    claude=None,
    title_learnings: list[dict] | None = None,
) -> dict[str, str]:
    if not namen:
        return {}
    if claude is None:
        claude = get_claude()
    uniek = list(dict.fromkeys(n.strip() for n in namen if n and n.strip()))
    if not uniek:
        return {}

    extra_regels = ""
    if title_learnings:
        instructies = collect_title_instructions(title_learnings)
        if instructies:
            extra_regels = "\nEXTRA REGELS (uit eerdere feedback):\n- " + "\n- ".join(instructies)

    prompt = (
        f"Vertaal deze {len(uniek)} Engelse productnamen naar het Nederlands voor "
        "een Belgische webshop (homeware/design).\n\n"
        "REGELS:\n"
        "- Behoud eigennamen onveranderd\n"
        "- Title Case (eerste letter groot, behalve van/de/het/en)\n"
        "- Maatcodes uppercase: XS, S, M, L, XL\n"
        "- Plate→Bord, Deep Plate→Diep Bord, Dessert Plate→Dessertbord, "
        "Bowl→Kom, Mirror→Spiegel, Cup→Kopje, Mug→Mok, Jug→Kan, "
        "Pot→Pot, Vase→Vaas, Glass→Glas, Tray→Dienblad\n"
        "- Kleuren: White→Wit, Black→Zwart, Beige→Beige, Blue→Blauw, "
        "Green→Groen, Red→Rood, Yellow→Geel, Grey→Grijs, Brown→Bruin, Pink→Roze\n"
        "- Schuine streep tussen kleuren (Beige Blue → Beige/Blauw)\n"
        "- Output: één regel per naam, dezelfde volgorde, geen nummering"
        f"{extra_regels}\n\n"
        "INPUT:\n" + "\n".join(uniek) + "\n\nOUTPUT:"
    )
    estimated = max(4000, len(uniek) * 16)
    response = claude.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=estimated,
        messages=[{"role": "user", "content": prompt}],
    )
    output = response.content[0].text.strip()
    lines = [l.strip() for l in output.split("\n") if l.strip()]
    if len(lines) != len(uniek):
        # mismatch: val terug op identity-mapping voor onbekende namen
        return {}
    result = {k: _fix_set_namen(v) for k, v in zip(uniek, lines)}
    # Pas strip/replace learnings post-hoc toe (dubbele garantie)
    if title_learnings:
        result = {k: apply_title_learnings(v, title_learnings) for k, v in result.items()}
    return result


# ── Hoofdfunctie: transform_batch ─────────────────────────────────────────────

def transform_batch(
    skus: list[str] | None = None,
    pipeline_status: str = "raw",
    fase: str | None = None,
    limit: int | None = None,
    progress: Callable[[int, int, str], None] | None = None,
    logger: Callable[[str], None] | None = None,
) -> TransformResult:
    """
    Transformeert producten op de NIEUWE schema (products_raw + products_curated).

    Parameters:
      - skus: list van SKUs om te transformeren (overschrijft pipeline_status filter)
      - pipeline_status: filter — alleen producten in products_curated met deze status
        (default 'raw'). Als pipeline_status leeg/None: producten ZONDER curated record.
      - fase: optioneel filter op products_raw.fase
      - limit: max aantal producten

    Schrijft naar products_curated (upsert op sku).
    """
    log = logger or print
    sb = get_supabase()
    claude = get_claude()

    # ── Selecteer producten ──
    raw_query = sb.table("products_raw").select("*")
    if skus:
        raw_query = raw_query.in_("sku", skus)
    if fase:
        raw_query = raw_query.eq("fase", fase)
    if limit:
        raw_query = raw_query.limit(limit)
    raw_rows = raw_query.execute().data or []

    if not raw_rows:
        log("Geen producten gevonden in products_raw.")
        return TransformResult()

    # Haal bestaande curated records op om te beslissen update vs insert
    sku_list = [r["sku"] for r in raw_rows if r.get("sku")]
    curated_existing: dict[str, dict] = {}
    if sku_list:
        # in batches van 200 (PostgREST IN-limit)
        for i in range(0, len(sku_list), 200):
            chunk = sku_list[i:i + 200]
            res = sb.table("products_curated").select("*").in_("sku", chunk).execute().data or []
            for c in res:
                curated_existing[c["sku"]] = c

    # Filter op pipeline_status indien geen explicit skus opgegeven
    if not skus and pipeline_status:
        if pipeline_status == "raw":
            # Producten zonder curated record OF met pipeline_status='raw'
            raw_rows = [r for r in raw_rows if
                        r["sku"] not in curated_existing or
                        curated_existing[r["sku"]].get("pipeline_status") == "raw"]
        else:
            raw_rows = [r for r in raw_rows if
                        r["sku"] in curated_existing and
                        curated_existing[r["sku"]].get("pipeline_status") == pipeline_status]

    result = TransformResult(total=len(raw_rows))
    if not raw_rows:
        log("Geen producten matchen het pipeline_status filter.")
        return result

    log(f"Transform: {len(raw_rows)} producten")

    # ── Learnings laden ──
    all_learnings = load_active_learnings(sb)
    cat_learnings = [L for L in all_learnings if L.get("stap") == "categorie"]
    extra_mat, extra_kl = apply_translation_learnings(
        [L for L in all_learnings if L.get("stap") == "vertaling"]
    )
    log(f"Actieve learnings: {len(all_learnings)} (cat: {len(cat_learnings)}, vertaling: {len(extra_mat) + len(extra_kl)})")
    result.learnings_applied = 0

    # ── Batch naam-vertaling (Haiku, 1 call) ──
    namen_raw = [r.get("product_name_raw", "") for r in raw_rows]
    log("Productnamen vertalen via Haiku...")
    try:
        naam_map = vertaal_productnamen_batch(namen_raw, claude)
    except Exception as e:
        log(f"Naam-vertaling mislukt, val terug op originele namen: {e}")
        naam_map = {}

    # ── Per product transformeren ──
    for idx, raw in enumerate(raw_rows):
        if progress:
            try:
                progress(idx, len(raw_rows), f"SKU {raw.get('sku')}")
            except Exception:
                pass

        sku = raw.get("sku") or f"#{idx}"
        existing = curated_existing.get(sku, {})
        updates: dict = {"sku": sku, "raw_id": raw.get("id"), "supplier": raw.get("supplier", "")}
        if raw.get("fase"):
            updates["fase"] = raw["fase"]
        review_redenen: list[str] = []

        # NL naam koppelen
        raw_naam = (raw.get("product_name_raw") or "").strip()
        product_voor_titel = {
            **raw,
            "_product_name_nl": naam_map.get(raw_naam, ""),
        }

        try:
            # 1. Categorie via mapping-tabel
            cat_row = lookup_category(
                sb,
                raw.get("leverancier_category", ""),
                raw.get("leverancier_item_cat", ""),
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
                    "info": f"{raw.get('leverancier_category')} / {raw.get('leverancier_item_cat')}",
                })

            # 1b. Name-rule learnings
            applied = apply_name_rules(raw, updates, cat_learnings)
            result.learnings_applied += applied

            # 2. Tags
            updates["tags"] = build_tags(
                updates.get("hoofdcategorie", ""),
                updates.get("subcategorie", ""),
                updates.get("sub_subcategorie", ""),
                raw.get("fase", ""),
                extra_tags=updates.get("_extra_tags"),
            )

            # 3. Materiaal vertalen
            raw_mat = raw.get("materiaal_raw", "") or ""
            materiaal_nl = translate_material(raw_mat, claude, extra_mat) if raw_mat else ""
            updates["materiaal_nl"] = materiaal_nl
            if materiaal_nl and not validate_against_website(sb, "materiaal", materiaal_nl):
                result.new_filter_values.append(f"materiaal: {materiaal_nl} (SKU: {sku})")

            # 4. Kleur vertalen
            raw_kl = raw.get("kleur_en", "") or ""
            kleur_filter, kleur_titel = translate_color(raw_kl, raw_naam, claude, extra_kl)
            updates["kleur_nl"] = kleur_filter
            if kleur_filter and not validate_against_website(sb, "kleur", kleur_filter):
                result.new_filter_values.append(f"kleur: {kleur_filter} (SKU: {sku})")

            # Lamp-uitzondering
            name_upper = raw_naam.upper()
            if any(lamp in name_upper for lamp in LAMP_EXCEPTIONS):
                review_redenen.append("lamp-uitzondering: controleer kleur en titel handmatig")

            # 5. Producttitel
            product_voor_titel.update(updates)
            titel = build_title(product_voor_titel)
            updates["product_title_nl"] = titel
            updates["handle"] = generate_handle(titel)

            # 6. Prijslogica (uit products_raw)
            verkoopprijs = raw.get("rrp_stuk_eur")
            inkoopprijs = raw.get("inkoopprijs_stuk_eur")
            if verkoopprijs is None or verkoopprijs == 0:
                review_redenen.append("verkoopprijs is 0 of ontbreekt")
            updates["verkoopprijs"] = verkoopprijs
            updates["inkoopprijs"] = inkoopprijs

            # 7. Meta description (Sonnet)
            h = raw.get("hoogte_cm") or ""
            l = raw.get("lengte_cm") or ""
            b = raw.get("breedte_cm") or ""
            afm = f"{h} x {l} x {b} cm" if all([h, l, b]) else ""

            extra_lines = []
            if updates.get("sub_subcategorie"):
                extra_lines.append(f"Producttype: {updates['sub_subcategorie']}")
            if raw.get("designer"):
                extra_lines.append(f"Designer: {raw['designer']}")
            if materiaal_nl:
                extra_lines.append(f"Materiaal: {materiaal_nl}")
            if kleur_filter:
                extra_lines.append(f"Kleur: {kleur_filter}")
            if updates.get("subcategorie"):
                extra_lines.append(f"Subcategorie: {updates['subcategorie']}")
            if afm:
                extra_lines.append(f"Afmetingen: {afm}")
            extra = "\n".join(extra_lines)

            meta_prompt = (
                f"Schrijf een Nederlandse SEO meta description (120-155 tekens) voor:\n"
                f"Product: {titel}\nMerk: {raw.get('supplier', 'Serax')}\n{extra}\n\n"
                "Regels: gebruik 'je'-vorm, eindig met CTA, vermeld gratis verzending €75 als dat past.\n"
                "Geef alleen de meta description terug, geen uitleg."
            )
            try:
                meta_resp = claude.messages.create(
                    model="claude-sonnet-4-6",
                    max_tokens=200,
                    messages=[{"role": "user", "content": meta_prompt}],
                )
                updates["meta_description"] = meta_resp.content[0].text.strip()[:160]
            except Exception as e:
                updates["meta_description"] = ""
                review_redenen.append(f"meta description fout: {e}")

            # 8. Status
            if review_redenen:
                updates["pipeline_status"] = "review"
                updates["review_reden"] = "; ".join(review_redenen)
                result.review += 1
            else:
                updates["pipeline_status"] = "ready"
                result.ready += 1

            # Cleanup tijdelijke velden
            updates.pop("_extra_tags", None)

            # Upsert in products_curated
            if sku in curated_existing:
                sb.table("products_curated").update(updates).eq("sku", sku).execute()
            else:
                sb.table("products_curated").insert(updates).execute()

            result.processed_skus.append(sku)
            symbol = "~" if updates["pipeline_status"] == "review" else "+"
            log(f"  {symbol} {sku} -> {updates['pipeline_status']}")

        except Exception as e:
            log(f"  ! {sku}: {e}")
            try:
                upd = {"sku": sku, "raw_id": raw.get("id"),
                       "pipeline_status": "review",
                       "review_reden": f"Technische fout: {e}"}
                if sku in curated_existing:
                    sb.table("products_curated").update(upd).eq("sku", sku).execute()
                else:
                    sb.table("products_curated").insert(upd).execute()
            except Exception:
                pass
            result.errors += 1

    if progress:
        try:
            progress(len(raw_rows), len(raw_rows), "klaar")
        except Exception:
            pass

    log(f"\nKlaar — ready: {result.ready}, review: {result.review}, errors: {result.errors}, "
        f"learnings toegepast: {result.learnings_applied}")
    return result


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--fase", help="Fase filter")
    parser.add_argument("--limit", type=int, help="Max producten")
    parser.add_argument("--sku", action="append", help="SKU(s) om te transformeren (kan herhaald)")
    args = parser.parse_args()
    transform_batch(skus=args.sku, fase=args.fase, limit=args.limit)
