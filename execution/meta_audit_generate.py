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
    python execution/meta_audit_generate.py --bulk --exclude-vendor "Pottery Pots" --write
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

# ── Vendor config ──────────────────────────────────────────────────────────
# Vendors die NIET als brand in de title mogen verschijnen. Voor deze producten
# gebruiken we `{name} | Interieur Shop` zonder vendor-middelstuk.
VENDOR_SKIP_IN_TITLE = {
    "valerie_objects",
    "valerie objects",
}


def should_skip_vendor_in_title(vendor: str) -> bool:
    return (vendor or "").strip().lower() in {v.lower() for v in VENDOR_SKIP_IN_TITLE}

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
    """Vraag Claude om de productnaam in te korten met behoud van SEO-keywords.
    Retry als Claude te lang antwoordt."""
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
    def _ask(msg):
        resp = client.messages.create(
            model=MODEL, max_tokens=100,
            messages=[{"role": "user", "content": msg}],
        )
        return resp.content[0].text.strip().strip('"').strip("'")

    short = _ask(prompt)
    # Strict retry als over budget
    for attempt in range(2):
        if len(short) <= max_name_len:
            break
        retry_msg = (
            f"Je antwoord is {len(short)} tekens, te lang. "
            f"Maak het STRIKT MAXIMAAL {max_name_len} tekens. "
            f"Je huidige antwoord: {short}\n"
            f"Geef een korter alternatief, alleen de naam."
        )
        short = _ask(retry_msg)
    return short[:max_name_len]  # harde cap als Claude nog steeds overschrijdt


def build_title_shop_only(clean_name: str) -> tuple[str, str] | None:
    """Fallback zonder vendor: '{name} | Interieur Shop'. Returns None als niet past."""
    full = f"{clean_name} | Interieur Shop"
    if len(full) <= TITLE_MAX:
        return full, "shop_only"
    return None


def make_title(client, product_title: str, vendor: str) -> tuple[str, str]:
    clean = normalize_product_name(product_title, vendor)

    # Skip-list vendors: direct shop-only format, geen vendor in title
    if should_skip_vendor_in_title(vendor):
        so = build_title_shop_only(clean)
        if so:
            return so
        # Te lang → Claude inkorten dan retry shop-only
        suffix_shop = " | Interieur Shop"
        shorter = shorten_name_via_claude(client, clean, vendor, len(suffix_shop))
        so = build_title_shop_only(shorter)
        if so:
            return so
        return shorter[:TITLE_MAX], "fallback_name_only"

    title, reason = build_title(clean, vendor)
    if reason == "formatted" or reason == "no_vendor":
        return title, reason

    # Te lang met vendor-suffix → Claude inkorten
    suffix = f" | {vendor} – Interieur Shop"
    shorter = shorten_name_via_claude(client, clean, vendor, len(suffix))
    title, reason = build_title(shorter, vendor)
    if reason == "formatted":
        return title, "shortened"

    # Middle fallback: probeer '{shorter} | Interieur Shop' (zonder vendor)
    so = build_title_shop_only(shorter)
    if so:
        return so

    # Probeer het nog een keer met extreem ingekorte naam voor shop-only
    suffix_shop = " | Interieur Shop"
    shorter2 = shorten_name_via_claude(client, clean, vendor, len(suffix_shop))
    so = build_title_shop_only(shorter2)
    if so:
        return so

    # Laatste vangnet: alleen de naam
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

DESC_SYSTEM_PROMPT = """Je schrijft Nederlandse meta descriptions voor een interieur-webshop (Interieur Shop / interieur-shop.nl).

HARDE REGELS:
- Tussen 120 en 155 tekens (tel spaties en leestekens mee) — mobiel toont vaak max 120 dus belangrijkste info voorop
- Nederlands, natuurlijk, actief en wervend geschreven
- Spreek de lezer aan met "je" (niet "u")
- Geen passieve zinnen ("hier wordt verteld over..." → fout)
- Geen clichés ("stijlvol design", "ontdek nu", "must-have", "geniet van", "bij Interieur-shop.nl")
- Noem het merk NIET expliciet (staat al in title)
- Geen quotes, labels, prefix — alleen de description tekst zelf
- Geen emoji
- Per product UNIEK (duplicate content is slecht voor SEO)

VERPLICHTE INGREDIËNTEN (in 155 chars passen):
1. ACTIEVE OPENING: begin de zin met een werkwoord dat de lezer aanspreekt (bv. "Bescherm je tafel met...", "Breng sfeer in je woonkamer met...", "Speel...", "Serveer...", "Dek je tafel met..."). NIET beginnen met een opsomming van specs.
2. FOCUS-KEYWORD: product-type/categorie (bv. "sierpot", "placemat", "serveerkom") natuurlijk verwerkt — Google maakt deze bold in SERP
3. KORT WAT HET IS: materiaal/kleur/afmeting, eerlijk en feitelijk
4. USP: gebruik de hieronder gegeven USP, exact zoals opgegeven
5. CTA: gebruik de hieronder gegeven CTA. Als de CTA `{product}` bevat, vervang die door het producttype uit de zin (bv. "deze placemat", "deze set", "deze sierpot").

De CTA staat altijd aan het eind, de USP er direct voor. Voorbeeld structuur:
"{actieve werkwoord-opening met product + materiaal + maat + feature}. {USP}. {CTA}!"

Schrijf vanuit zoekintentie: wat wil iemand weten die dit zoekt? Match dat eerlijk, geen clickbait.
Als context summier is, baseer je alleen op wat je zeker weet. Verzin geen features."""


# USP + CTA rotatie (officieel van interieur-shop.nl). {product} wordt door Claude
# vervangen door het producttype (bv. "deze sierpot", "deze set", "deze placemat").
USPS = [
    "Gratis verzending vanaf €75",
    "Voor 16:00 besteld, morgen in huis",
    "Beoordeeld met een 9.0",
]
CTAS = [
    "Bestel direct!",
    "Bekijk {product} nu",
    "Ontdek het aanbod",
    "Shop {product} hier",
]


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
    elif (ctx.get("source") or "").startswith("web:"):
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


def pick_usp_cta(pid: str) -> tuple[str, str]:
    """Deterministische rotatie op basis van Product ID voor afwisseling binnen batches."""
    try:
        seed = int(pid[-4:])
    except (ValueError, TypeError):
        seed = 0
    return USPS[seed % len(USPS)], CTAS[seed % len(CTAS)]


def generate_desc(client, product_title: str, ctx: dict, pid: str) -> str:
    context_block = build_context_block(ctx, product_title)
    usp, cta = pick_usp_cta(pid)
    user_msg = (
        f"{context_block}\n\n"
        f"USP om te gebruiken: {usp}\n"
        f"CTA om te gebruiken (aan het einde): {cta}\n\n"
        f"Schrijf één meta description (120-155 tekens). Alleen de tekst."
    )
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

def fetch_bulk_products(sb, exclude_vendors: list[str]) -> list[dict]:
    """
    Alle producten die nog geen suggested_meta_title hebben EN niet 'ok' zijn op
    title of desc. Skip producten van uitgesloten vendors (bv. Pottery Pots —
    die behandelen we apart via web-research).
    """
    rows, offset, page = [], 0, 1000
    while True:
        q = sb.table("shopify_meta_audit").select("*")\
            .is_("suggested_meta_title", "null")
        batch = q.range(offset, offset + page - 1).execute().data
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page:
            break
        offset += page

    # Filter: exclude vendors, en title of desc is niet ok
    filtered = []
    for r in rows:
        if r.get("vendor") in exclude_vendors:
            continue
        if r.get("title_status") == "ok" and r.get("desc_status") == "ok":
            continue
        filtered.append(r)
    return filtered


def process_product(sb, client, p: dict, write: bool,
                    regen_ok_desc: bool = False) -> dict:
    """
    Verwerk één product. Geeft een status-dict terug voor logging.
    - Title: altijd genereren als title_status != 'ok'
    - Desc:  genereren tenzij desc_status == 'ok' (jouw regel: goede descs niet aanraken)
    """
    ctx = fetch_context(sb, p)

    update = {}
    status = {"pid": p["shopify_product_id"], "title": None, "desc": None,
              "ctx": ctx.get("source") or "none", "error": None}

    try:
        needs_title = (
            p.get("title_status") != "ok" and not p.get("suggested_meta_title")
        )
        needs_desc = (
            p.get("desc_status") != "ok"
            and not p.get("suggested_meta_description")
        ) or regen_ok_desc

        if needs_title:
            new_title, reason = make_title(client, p["product_title"], p["vendor"])
            update["suggested_meta_title"] = new_title
            update["suggested_title_length"] = len(new_title)
            status["title"] = f"{len(new_title)}c/{reason}"
        elif p.get("suggested_meta_title"):
            status["title"] = "kept"

        if needs_desc:
            new_desc = generate_desc(
                client, p["product_title"], ctx, p["shopify_product_id"]
            )
            update["suggested_meta_description"] = new_desc
            update["suggested_desc_length"] = len(new_desc)
            status["desc"] = f"{len(new_desc)}c"
        else:
            status["desc"] = "kept-ok"

        if write and update:
            sb.table("shopify_meta_audit").update(update)\
                .eq("shopify_product_id", p["shopify_product_id"]).execute()
    except Exception as e:
        status["error"] = str(e)[:200]
    return status


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--test", action="store_true")
    ap.add_argument("--ids", help="Comma-separated shopify_product_ids")
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--bulk", action="store_true",
                    help="Process all products without suggested_meta_title")
    ap.add_argument("--exclude-vendor", action="append", default=[],
                    help="Vendor names to skip (can be used multiple times)")
    ap.add_argument("--log", help="Progress log file path")
    args = ap.parse_args()

    sb = get_supabase()
    client = get_anthropic()

    log_f = open(args.log, "a", encoding="utf-8") if args.log else None
    def log(msg):
        print(msg, flush=True)
        if log_f:
            log_f.write(msg + "\n")
            log_f.flush()

    if args.bulk:
        products = fetch_bulk_products(sb, args.exclude_vendor)
        log(f"\nBULK mode: {len(products)} producten te verwerken "
            f"(exclude vendors: {args.exclude_vendor or 'geen'})\n")
        from datetime import datetime
        start = datetime.now()
        ok, err = 0, 0
        for i, p in enumerate(products, 1):
            s = process_product(sb, client, p, args.write)
            if s["error"]:
                err += 1
                log(f"  [{i}/{len(products)}] ERROR {s['pid']} {p.get('vendor')}: {s['error']}")
            else:
                ok += 1
                if i % 10 == 0 or i <= 5:
                    elapsed = (datetime.now() - start).total_seconds()
                    rate = i / elapsed if elapsed > 0 else 0
                    eta = (len(products) - i) / rate if rate > 0 else 0
                    log(f"  [{i}/{len(products)}] OK "
                        f"{p.get('vendor','?'):15} | "
                        f"title={s['title']:20} desc={s['desc']:10} ctx={s['ctx']:15} "
                        f"| rate={rate:.2f}/s ETA={eta/60:.1f}min")
        log(f"\nKlaar: {ok} OK, {err} errors")
        if log_f:
            log_f.close()
        return

    if args.ids:
        ids = [i.strip() for i in args.ids.split(",")]
        products = sb.table("shopify_meta_audit").select("*")\
            .in_("shopify_product_id", ids).execute().data
    elif args.test:
        products = pick_test_products(sb)
    else:
        sys.exit("Gebruik --test, --ids of --bulk")

    if not products:
        sys.exit("Geen producten gevonden.")

    print(f"\n{len(products)} producten -> voorstellen genereren...\n")

    for p in products:
        ctx = fetch_context(sb, p)
        new_title, reason = make_title(client, p["product_title"], p["vendor"])
        new_desc = generate_desc(client, p["product_title"], ctx, p["shopify_product_id"])
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
