"""
meta_audit_generate.py — Genereer meta title + description voorstellen voor
producten uit `shopify_meta_audit`.

Title pipeline:
  1. Normaliseer productnaam:
     - ALL CAPS (>70% upper) -> Title Case
     - Vendor-prefix strippen als naam begint met vendor
  2. Probeer format `{naam} | {vendor} – Interieur Shop` (max 60)
  3. Als te lang: Claude kort naam in met behoud SEO-keywords, probeer opnieuw
  4. Als nog te lang: alleen `{naam}`

Description:
  Claude genereert natuurlijke NL tekst (120-155 chars).
  Context bronnen (read-only):
    - primair:  Supabase `seo_products` (curated onboarding data)
    - fallback: Excel `Product description without HTML` (alleen voor producten
                die niet in seo_products staan)

Gebruik:
    python execution/meta_audit_generate.py --test
    python execution/meta_audit_generate.py --test --write
    python execution/meta_audit_generate.py --ids 123,456
"""

import argparse
import os
import sys
import re
from pathlib import Path
from dotenv import load_dotenv

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

TITLE_MAX = 58  # 2 chars safety marge onder Google's ~60 char pixel-limiet
DESC_MAX = 155
DESC_MIN = 120
MODEL = "claude-opus-4-7"

EXCEL_PATH = "master files/Alle Active Producten.xlsx"
RESEARCH_CACHE_PATH = ".tmp/product_research_cache.json"


def get_supabase():
    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def get_anthropic():
    from anthropic import Anthropic
    return Anthropic(api_key=ANTHROPIC_API_KEY)


# -------- Productnaam normalisatie --------

def is_mostly_caps(text: str) -> bool:
    letters = [c for c in text if c.isalpha()]
    if len(letters) < 3:
        return False
    upper = sum(1 for c in letters if c.isupper())
    return upper / len(letters) > 0.70


def to_title_case_nl(text: str) -> str:
    """Title case, maar laat al bestaande lowercase woorden met lengte<=3 met rust."""
    words = text.split()
    out = []
    small_words = {"en", "of", "de", "het", "een", "van", "met", "in", "op", "the", "and", "x"}
    for i, w in enumerate(words):
        if i > 0 and w.lower() in small_words:
            out.append(w.lower())
        else:
            out.append(w.capitalize())
    return " ".join(out)


def strip_vendor_prefix(name: str, vendor: str) -> str:
    if not vendor:
        return name
    pattern = re.compile(rf"^\s*{re.escape(vendor)}\s*[-–—:|]\s*", re.IGNORECASE)
    cleaned = pattern.sub("", name)
    # Strip ook eventueel een tweede segment als "Serax - Uncharted - ..." (collectie-prefix)
    # Heuristiek: als na strippen nog een deel vóór " - " staat met <20 chars, laat het staan
    # om collectie-info niet kapot te maken. Simpel: één vendor-strip is genoeg.
    return cleaned.strip()


def normalize_product_name(name: str, vendor: str) -> str:
    name = (name or "").strip()
    vendor = (vendor or "").strip()
    if not name:
        return ""
    if is_mostly_caps(name):
        name = to_title_case_nl(name)
    name = strip_vendor_prefix(name, vendor)
    return name


# -------- Title building --------

def build_title(clean_name: str, vendor: str) -> tuple[str, str]:
    """Return (title, reason)."""
    if not vendor:
        return clean_name[:TITLE_MAX], "no_vendor"
    full = f"{clean_name} | {vendor} – Interieur Shop"
    if len(full) <= TITLE_MAX:
        return full, "formatted"
    return "", "needs_shortening"


def shorten_name_via_claude(client, long_name: str, vendor: str, suffix_len: int) -> str:
    """Vraag Claude om de productnaam in te korten met behoud van SEO-keywords."""
    max_name_len = TITLE_MAX - suffix_len
    prompt = f"""Kort deze productnaam in tot maximaal {max_name_len} tekens.

Behouden (hoge prioriteit):
- Type product (bv. Placemat, Kan, Vaas, Serveerkom, Sierpot, Schotel)
- Kleur en materiaal (bv. Roze Marmer, Gouden, Keramiek) — belangrijke SEO-keywords
- Het woord "Set" als het een set is
- Collectie-/seriesnamen (bv. Boulangerie, Brasserie de Paris, Uncharted, Out of Lines)
- Afmetingen in cm als ze in de naam staan (bv. 30x45)

Mag weg:
- Generieke woorden: "Assorti", "Traditionnelle", "Van", "Set van", "Met"
- Beschrijvende bijvoeglijke naamwoorden zonder SEO-waarde (bv. "Gebogen")
- Merknaam (komt apart in title)
- Overbodige leestekens

Productnaam: {long_name}

Antwoord met alleen de ingekorte naam, niets anders."""
    resp = client.messages.create(
        model=MODEL,
        max_tokens=100,
        messages=[{"role": "user", "content": prompt}],
    )
    short = resp.content[0].text.strip().strip('"').strip("'")
    return short


def make_title(client, product_title: str, vendor: str) -> tuple[str, str]:
    clean = normalize_product_name(product_title, vendor)
    title, reason = build_title(clean, vendor)
    if reason == "formatted" or reason == "no_vendor":
        return title, reason

    # Te lang — Claude inkorten
    suffix = f" | {vendor} – Interieur Shop"
    shorter = shorten_name_via_claude(client, clean, vendor, len(suffix))
    title, reason = build_title(shorter, vendor)
    if reason == "formatted":
        return title, "shortened"
    # Laatste fallback: alleen de shortened naam
    return shorter[:TITLE_MAX], "fallback_name_only"


# -------- Product context (read-only) --------

_excel_cache = None


def load_excel_fallback() -> dict:
    global _excel_cache
    if _excel_cache is not None:
        return _excel_cache
    import pandas as pd
    df = pd.read_excel(EXCEL_PATH)[[
        "Product ID", "Product description without HTML"
    ]].drop_duplicates(subset=["Product ID"])
    out = {}
    for _, r in df.iterrows():
        pid = r["Product ID"]
        if pd.isna(pid):
            continue
        try:
            pid = str(int(float(pid)))
        except Exception:
            continue
        desc = r["Product description without HTML"]
        if pd.notna(desc) and str(desc).strip():
            out[pid] = str(desc).strip()
    _excel_cache = out
    return out


_research_cache = None


def load_research_cache() -> dict:
    global _research_cache
    if _research_cache is not None:
        return _research_cache
    import json
    p = Path(RESEARCH_CACHE_PATH)
    if p.exists():
        _research_cache = json.loads(p.read_text(encoding="utf-8"))
    else:
        _research_cache = {}
    return _research_cache


def fetch_context(sb, product: dict) -> dict:
    """Haal product-context op. Prioriteit: seo_products > Excel > web-research cache. Read-only."""
    pid = product["shopify_product_id"]
    handle = product.get("handle") or ""
    ctx = {"source": None}

    # 1. Match op shopify_product_id
    r = sb.table("seo_products").select(
        "product_title_nl, meta_description, materiaal_nl, kleur_nl, "
        "hoofdcategorie, subcategorie, sub_subcategorie, "
        "hoogte_cm, lengte_cm, breedte_cm, designer"
    ).eq("shopify_product_id", pid).limit(1).execute().data

    # 2. Fallback op handle
    if not r and handle:
        r = sb.table("seo_products").select(
            "product_title_nl, meta_description, materiaal_nl, kleur_nl, "
            "hoofdcategorie, subcategorie, sub_subcategorie, "
            "hoogte_cm, lengte_cm, breedte_cm, designer"
        ).eq("handle", handle).limit(1).execute().data

    if r:
        row = r[0]
        ctx.update(row)
        ctx["source"] = "seo_products"
        return ctx

    # 3. Fallback Excel
    excel = load_excel_fallback()
    if pid in excel:
        ctx["product_description_raw"] = excel[pid]
        ctx["source"] = "excel"
        return ctx

    # 4. Fallback web-research cache (handmatig of via research module gevuld)
    cache = load_research_cache()
    if pid in cache:
        ctx["product_description_raw"] = cache[pid]["description"]
        ctx["source"] = f"web:{cache[pid].get('source_url', '')}"
    return ctx


# -------- Description generation --------

DESC_SYSTEM_PROMPT = """Je schrijft Nederlandse meta descriptions voor een interieur-webshop (Interieur Shop).

Harde regels:
- Tussen 120 en 155 tekens (tel spaties en leestekens mee)
- Nederlands, natuurlijk, prettig leesbaar
- GEEN template-zinnen ("bij Interieur-shop.nl", "stijlvol design", "snelle levering")
- GEEN clichés ("ontdek nu", "must-have", "geniet van")
- Focus op wat het product is, materiaal/stijl, en waar het past in huis
- Noem het merk NIET expliciet (staat al in de title)
- Geen quotes, labels of prefix — alleen de description tekst
- Geen emoji

Als de context summier is, baseer je alleen op wat je zeker weet. Verzin geen features."""


def build_context_block(ctx: dict, product_title: str) -> str:
    lines = [f"Productnaam: {product_title}"]
    if ctx.get("source") == "seo_products":
        for k, label in [
            ("materiaal_nl", "Materiaal"),
            ("kleur_nl", "Kleur"),
            ("hoofdcategorie", "Categorie"),
            ("subcategorie", "Subcategorie"),
            ("sub_subcategorie", "Subsub"),
            ("designer", "Designer"),
        ]:
            v = ctx.get(k)
            if v:
                lines.append(f"{label}: {v}")
        dims = []
        for k, label in [("hoogte_cm", "H"), ("lengte_cm", "L"), ("breedte_cm", "B")]:
            v = ctx.get(k)
            if v:
                dims.append(f"{label}:{v}cm")
        if dims:
            lines.append("Afmetingen: " + " ".join(dims))
        if ctx.get("meta_description"):
            lines.append(f"Bestaande omschrijving (curated): {ctx['meta_description']}")
    elif ctx.get("source") == "excel":
        raw = ctx["product_description_raw"][:600]
        lines.append(f"Productomschrijving (Shopify): {raw}")
    elif ctx.get("source", "").startswith("web:"):
        raw = ctx["product_description_raw"][:800]
        lines.append(f"Productomschrijving (van fabrikant-website, herschrijven voor originaliteit): {raw}")
    else:
        lines.append("(geen aanvullende context beschikbaar)")
    return "\n".join(lines)


def _call_desc(client, user_msg: str) -> str:
    resp = client.messages.create(
        model=MODEL,
        max_tokens=300,
        system=DESC_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
    )
    text = resp.content[0].text.strip()
    if text.startswith('"') and text.endswith('"'):
        text = text[1:-1].strip()
    return text


def generate_desc(client, product_title: str, ctx: dict) -> str:
    context_block = build_context_block(ctx, product_title)
    user_msg = f"{context_block}\n\nSchrijf één meta description (120-155 tekens). Alleen de tekst."
    text = _call_desc(client, user_msg)

    # Harde cap: als >155, vraag Claude het in te korten (max 2 retries)
    for _ in range(2):
        if len(text) <= DESC_MAX:
            break
        retry_msg = (
            f"Deze description is {len(text)} tekens, te lang.\n"
            f"Kort in tot maximaal {DESC_MAX} tekens, minimaal {DESC_MIN}, "
            f"behoud de belangrijkste info (wat het product is, materiaal, toepassing).\n\n"
            f"Te lange versie: {text}\n\nAlleen de ingekorte tekst."
        )
        text = _call_desc(client, retry_msg)

    # Laatste vangnet: hard truncate op laatste zin-einde voor 155
    if len(text) > DESC_MAX:
        cut = text[:DESC_MAX]
        last_period = max(cut.rfind("."), cut.rfind("!"), cut.rfind("?"))
        if last_period > DESC_MIN:
            text = cut[:last_period + 1]
        else:
            # Op woordgrens afbreken
            text = cut.rsplit(" ", 1)[0]
    return text


# -------- Test product selection --------

def pick_test_products(sb) -> list[dict]:
    picks = []
    # 1. Serax (dominante vendor)
    r = sb.table("shopify_meta_audit").select("*").eq("vendor", "Serax")\
        .eq("title_status", "missing").limit(10).execute().data
    # pak er eentje met niet-extreme naam
    if r:
        mid = [p for p in r if 20 < len(p["product_title"]) < 55]
        picks.append(mid[0] if mid else r[0])

    # 2. Lange naam (triggert shortening)
    r = sb.table("shopify_meta_audit").select("*")\
        .eq("title_status", "missing").limit(200).execute().data
    long_ones = [p for p in r if len(p.get("product_title") or "") > 55]
    if long_ones: picks.append(long_ones[0])

    # 3. Pottery Pots
    r = sb.table("shopify_meta_audit").select("*").eq("vendor", "Pottery Pots")\
        .limit(1).execute().data
    if r: picks.append(r[0])

    # 4. Template-desc voorbeeld
    r = sb.table("shopify_meta_audit").select("*").eq("desc_status", "too_short")\
        .limit(50).execute().data
    templated = [p for p in r if "Stijlvol design" in (p.get("current_meta_description") or "")]
    if templated: picks.append(templated[0])

    # 5. Printworks
    r = sb.table("shopify_meta_audit").select("*").eq("vendor", "Printworks")\
        .limit(1).execute().data
    if r: picks.append(r[0])

    seen = set()
    out = []
    for p in picks:
        if p["shopify_product_id"] not in seen:
            seen.add(p["shopify_product_id"])
            out.append(p)
    return out[:5]


# -------- Display --------

def display_side_by_side(p: dict, new_title: str, title_reason: str,
                         new_desc: str, ctx_source: str) -> None:
    print("=" * 90)
    print(f"[{p['vendor'] or '(geen vendor)'}] {p['product_title']}")
    print(f"  handle:     {p['handle']}")
    print(f"  ctx bron:   {ctx_source or '(geen)'}")
    print()
    print(f"  HUIDIG TITLE  ({p['current_title_length']:>3} chars) [{p['title_status']:9}]:")
    print(f"    {p['current_meta_title'] or '(leeg)'}")
    print(f"  VOORSTEL TITLE ({len(new_title):>3} chars) [{title_reason}]:")
    print(f"    {new_title}")
    print()
    print(f"  HUIDIG DESC   ({p['current_desc_length']:>3} chars) [{p['desc_status']:9}]:")
    print(f"    {p['current_meta_description'] or '(leeg)'}")
    print(f"  VOORSTEL DESC ({len(new_desc):>3} chars):")
    print(f"    {new_desc}")
    print()


# -------- Main --------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--test", action="store_true")
    ap.add_argument("--ids", help="Comma-separated shopify_product_ids")
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()

    sb = get_supabase()
    client = get_anthropic()

    if args.ids:
        ids = [i.strip() for i in args.ids.split(",")]
        products = sb.table("shopify_meta_audit").select("*")\
            .in_("shopify_product_id", ids).execute().data
    elif args.test:
        products = pick_test_products(sb)
    else:
        sys.exit("Gebruik --test of --ids")

    if not products:
        sys.exit("Geen producten gevonden.")

    print(f"\n{len(products)} producten -> voorstellen genereren...\n")

    for p in products:
        ctx = fetch_context(sb, p)
        new_title, reason = make_title(client, p["product_title"], p["vendor"])
        new_desc = generate_desc(client, p["product_title"], ctx)
        display_side_by_side(p, new_title, reason, new_desc, ctx.get("source"))

        if args.write:
            sb.table("shopify_meta_audit").update({
                "suggested_meta_title":       new_title,
                "suggested_meta_description": new_desc,
                "suggested_title_length":     len(new_title),
                "suggested_desc_length":      len(new_desc),
            }).eq("shopify_product_id", p["shopify_product_id"]).execute()

    if args.write:
        print("Suggesties opgeslagen.")
    else:
        print("(niet opgeslagen — gebruik --write om te persisteren)")


if __name__ == "__main__":
    main()
