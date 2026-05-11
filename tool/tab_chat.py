"""Tab — Chat-assistent.

Claude beantwoordt vragen over de product-data en stelt aanpassingen voor.
Aanpassingen worden NOOIT automatisch doorgevoerd — altijd eerst bevestigen.

Voorbeeldvragen:
  "Welke producten hebben 2x 'Serax' in de naam?"
  "Toon alle producten van Pottery Pots zonder meta description"
  "Welke meta titles zijn langer dan 58 tekens?"
  "Verwijder het dubbele 'Serax' uit de gevonden titels"
"""
from __future__ import annotations

import io
import os
import re

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from client import get_client_id

load_dotenv()

# ── Tools die Claude mag aanroepen ────────────────────────────────────────────

TOOLS = [
    {
        "name": "zoek_producten",
        "description": (
            "Zoek producten in shopify_meta_audit — de LIVE Shopify-data (alleen gesyncte producten). "
            "Gebruik dit voor vragen over SEO-titels, meta descriptions, en live Shopify-status. "
            "product_status waarden: 'active', 'draft', 'archived' (Engels). "
            "Geeft handle, SKU, product_title, vendor, current_meta_title, "
            "current_meta_description, product_status terug. "
            "Alle filters zijn optioneel — laat product_status leeg om ALLE statussen te zoeken."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "product_title_bevat":  {"type": "string", "description": "Zoek in product_title (case-insensitive)"},
                "meta_title_bevat":     {"type": "string", "description": "Zoek in current_meta_title"},
                "meta_desc_bevat":      {"type": "string", "description": "Zoek in current_meta_description"},
                "vendor":               {"type": "string", "description": "Vendor-naam (gedeeltelijk, case-insensitive)"},
                "title_status":         {"type": "string", "enum": ["ok", "missing", "too_long", "too_short", "duplicate"]},
                "desc_status":          {"type": "string", "enum": ["ok", "missing", "too_long", "too_short", "templated", "duplicate"]},
                "product_status":       {"type": "string", "enum": ["active", "draft", "archived"], "description": "Laat leeg om alle statussen te zoeken"},
                "limit":                {"type": "integer", "default": 200},
            },
        },
    },
    {
        "name": "zoek_pipeline",
        "description": (
            "Zoek producten in products_curated — onze INTERNE pipeline database. "
            "Bevat alle verwerkte producten. "
            "Voor 'gearchiveerd': gebruik shopify_status='archived' (Engels). "
            "supplier = het merk/leverancier (bijv. 'Pottery Pots', 'Serax'). "
            "pipeline_status waarden: 'raw', 'matched', 'ready', 'review', 'exported'. "
            "shopify_status waarden (uit shopify_meta_audit): 'active', 'archived', 'draft'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "supplier":              {"type": "string", "description": "Leverancier/merk (gedeeltelijk, case-insensitive)"},
                "shopify_status":        {"type": "string", "enum": ["active", "archived", "draft"], "description": "Shopify-status — archived = gearchiveerd"},
                "pipeline_status":       {"type": "string", "enum": ["raw", "matched", "ready", "review", "exported"]},
                "fase":                  {"type": "string", "description": "Fase nummer: '1' t/m '6'"},
                "titel_bevat":           {"type": "string", "description": "Zoek in product_title_nl"},
                "hoofdcategorie_bevat":  {"type": "string", "description": "Zoek in hoofdcategorie"},
                "limit":                 {"type": "integer", "default": 200},
            },
        },
    },
    {
        "name": "stel_updates_voor",
        "description": (
            "Stel een lijst van veld-updates voor. "
            "De gebruiker moet ze bevestigen voordat ze worden weggeschreven. "
            "Gebruik dit nadat je producten hebt gevonden die aangepast moeten worden."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "samenvatting": {
                    "type": "string",
                    "description": "Korte uitleg van wat er wordt aangepast en waarom.",
                },
                "updates": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "handle":        {"type": "string"},
                            "sku":           {"type": "string"},
                            "product_title": {"type": "string"},
                            "veld":          {"type": "string", "enum": ["current_meta_title", "current_meta_description"]},
                            "oude_waarde":   {"type": "string"},
                            "nieuwe_waarde": {"type": "string"},
                            "reden":         {"type": "string"},
                        },
                        "required": ["handle", "veld", "nieuwe_waarde"],
                    },
                },
            },
            "required": ["updates", "samenvatting"],
        },
    },
    {
        "name": "status_voor_skus",
        "description": (
            "Bekijk de complete status van een lijst SKU's in één keer. "
            "Gebruik dit ALTIJD als de gebruiker vraagt 'hoe zit het met deze producten?' "
            "of vergelijkbaar, vooral na een bestand-upload met SKU's. "
            "Geeft per SKU: vendor, shopify-status (active/archived/draft), heeft meta title, "
            "heeft meta description, hoofdcategorie ingevuld, pipeline_status. "
            "Plus een aggregaat-overzicht (hoeveel online, hoeveel zonder meta, etc.)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "skus": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Lijst SKU's, maximaal 500.",
                },
            },
            "required": ["skus"],
        },
    },
    {
        "name": "open_in_pipeline",
        "description": (
            "Stel voor om de gebruiker over te zetten naar een specifieke pijplijn-pagina, "
            "met geüploade SKU's al voorgeladen. Gebruik dit ALTIJD na een bestand-upload "
            "wanneer duidelijk is welke pijplijn past, of wanneer de gebruiker expliciet vraagt "
            "om iets in een specifieke pagina te openen.\n\n"
            "Beschikbare pijplijnen:\n"
            "- 'herverwerk' = Archief herverwerken (SKU-lijst van bestaande producten die opnieuw moeten)\n"
            "- 'pipeline'   = Volledige pipeline (nieuwe leveranciersexport — Excel met naam/prijs/foto's)\n"
            "- 'nieuwe'     = Nieuwe producten (legacy upload-flow)\n"
            "- 'prijzen'    = Prijzen bijwerken\n"
            "- 'collectie'  = Collectie SEO-teksten\n\n"
            "Na deze tool verschijnt een primary 'Open in {pijplijn}'-knop in de UI. "
            "Bevestig kort, geef geen lange uitleg."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "pijplijn": {
                    "type": "string",
                    "enum": ["herverwerk", "pipeline", "nieuwe", "prijzen", "collectie"],
                    "description": "Welke pijplijn-pagina openen.",
                },
                "skus": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "SKU's om voor te laden. Voor 'herverwerk' worden deze in de "
                        "herverwerk-tab automatisch opgehaald uit Supabase. Voor andere "
                        "pijplijnen wordt de lijst als context aangeboden. Optioneel."
                    ),
                },
                "reden": {
                    "type": "string",
                    "description": "Eén zin: waarom is dit de juiste pijplijn? (Bv. 'leveranciersexport met 247 nieuwe producten — past in volledige pipeline')",
                },
            },
            "required": ["pijplijn", "reden"],
        },
    },
    {
        "name": "bouw_hextom_export",
        "description": (
            "Bouw een Hextom-formaat Excel-bestand voor download. "
            "Gebruik dit ALTIJD wanneer de gebruiker om een download/export/Excel/Hextom vraagt — "
            "schrijf NOOIT een CSV of tabel als tekst in je antwoord. "
            "De tool haalt alle benodigde data uit Supabase (naam, prijs, categorie, materiaal, "
            "foto's, EAN, afmetingen) en bouwt het Hextom-Excel klaar. Na deze tool verschijnt "
            "een download-knop in de UI — bevestig kort dat het klaarstaat, geef geen inhoud terug."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "skus": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "Lijst SKU's voor de export. Verzamel deze eerst via zoek_pipeline "
                        "of zoek_producten. SKU's die niet bestaan in products_raw worden overgeslagen."
                    ),
                },
                "bestandsnaam": {
                    "type": "string",
                    "description": (
                        "Voorgestelde bestandsnaam zonder extensie (bv. 'hextom_pottery_archief'). "
                        "Aantal producten en .xlsx worden automatisch toegevoegd."
                    ),
                },
            },
            "required": ["skus"],
        },
    },
]

SYSTEM = """Je bent een data-assistent voor een Nederlandse webshop (SEOkitchen).
Je hebt toegang tot twee databases via tools.

## Welke tool gebruik je wanneer?

**zoek_pipeline** (products_curated — onze interne database):
- Vragen over gearchiveerde / niet-live producten
- Vragen over hoeveel producten er zijn per merk, status, fase
- Vragen over inkoopprijs, verkoopprijs, categorieën
- Altijd als de gebruiker vraagt naar 'archief' of 'gearchiveerd' → gebruik shopify_status='archived' (Engels!)
- supplier = het merk (bijv. 'Pottery Pots', 'Serax')
- Altijd als de gebruiker vraagt naar een merk zonder specifieke SEO-vraag

**zoek_producten** (shopify_meta_audit — live Shopify data):
- Vragen over meta titles, meta descriptions, SEO-kwaliteit
- Vragen over live Shopify status (active/draft/archived in het Engels)
- Vragen over producten die al live staan

## Gedragsregels
- Gebruik altijd eerst een zoek-tool om data op te halen voordat je conclusies trekt.
- Stel updates voor via stel_updates_voor — schrijf NOOIT direct zonder bevestiging.
- Voor downloads/exports/Excel-bestanden: ALTIJD bouw_hextom_export aanroepen, NOOIT een CSV of tabel als tekst in je antwoord plakken. De gebruiker wil een echt bestand.
- Wees bondig: geef een korte samenvatting + de data, geen lange uitleg.
- Taal: Nederlands.
- Als een vraag onduidelijk is: zoek in beide databases en combineer de resultaten.

## Bestand-uploads
Als de gebruiker een bestand bijvoegt, ontvang je een systeem-context met bestandsnaam, kolommen
en gedetecteerde SKU's.

Stappenplan na een SKU-lijst:
1. Vraag de gebruiker NIET wat hij wil — als hij niets zegt, roep `status_voor_skus` aan met de
   geüploade SKU's en geef een korte samenvatting (zoveel online, zoveel zonder meta, etc.).
2. Bij vragen als "hoe zit het met deze producten?", "staan ze al online?", "zijn ze
   gecategoriseerd?": ALTIJD eerst `status_voor_skus` aanroepen.
3. Als de gebruiker zegt "fix het", "los het op", "ga aan de slag": stel `open_in_pipeline`
   met pijplijn='herverwerk' voor zodat hij de batch in de herverwerk-tab kan corrigeren
   (daar zit al een bulk-correctie-chat).

Pijplijn-routing bij bestand-upload:
- SKU-lijst van bestaande producten → 'herverwerk'
- Leveranciersexport met naam/prijs/foto's voor NIEUWE producten → 'pipeline'
- Prijslijst (SKU + nieuwe prijs) → 'prijzen'
- Collectie SEO-teksten → 'collectie'

Bij twijfel: vraag eerst, route niet automatisch.
"""


# ── Supabase helpers ──────────────────────────────────────────────────────────

@st.cache_resource
def _sb():
    """Nieuwe Supabase — shopify_meta_audit, shopify_sync, etc."""
    from supabase import create_client
    url = os.getenv("SUPABASE_NEW_URL", "")
    key = os.getenv("SUPABASE_NEW_SERVICE_KEY", "") or os.getenv("SUPABASE_NEW_KEY", "")
    if not url or not key:
        return None
    return create_client(url, key)


@st.cache_resource
def _sb_pipeline():
    """Oude Supabase — seo_products, seo_import_runs, etc."""
    from supabase import create_client
    url = os.getenv("SUPABASE_URL") or os.getenv("SUPABASE_NEW_URL", "")
    key = (
        os.getenv("SUPABASE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_NEW_SERVICE_KEY")
        or os.getenv("SUPABASE_NEW_KEY", "")
    )
    if not url or not key:
        return None
    return create_client(url, key)


@st.cache_data(ttl=60, show_spinner=False)
def _sku_map() -> dict[str, str]:
    try:
        sb = _sb()
        if not sb:
            return {}
        # Primair: shopify_meta_audit (gevuld door foto-sync, alle 2225 producten)
        res = sb.table("shopify_meta_audit").select("handle,sku").execute()
        mapping = {r["handle"]: r["sku"] for r in (res.data or []) if r.get("handle") and r.get("sku")}
        # Fallback: shopify_sync
        if len(mapping) < 100:
            res2 = sb.table("shopify_sync").select("handle,sku").execute()
            for r in (res2.data or []):
                if r.get("handle") and r.get("sku") and r["handle"] not in mapping:
                    mapping[r["handle"]] = r["sku"]
        return mapping
    except Exception:
        return {}


# ── File-upload parsing ───────────────────────────────────────────────────────

# Pages worden zo gemapt (zie tool/app.py NAV) — gebruikt voor st.switch_page().
_PIJPLIJN_PAGES = {
    "herverwerk": "pages/08_Herverwerk.py",
    "pipeline":   "pages/10_Pipeline.py",
    "nieuwe":     "pages/01_Nieuwe.py",
    "prijzen":    "pages/02_Prijzen.py",
    "collectie":  "pages/03_Collectie.py",
}


def _parse_uploaded_file(uploaded) -> dict:
    """Lees een geüpload bestand en haal context eruit voor Claude.

    Returns: {"naam": str, "type": str, "rijen": int, "kolommen": [str],
              "skus": [str], "sample": [dict], "fout": str | None}
    """
    naam = getattr(uploaded, "name", "?")
    info: dict = {"naam": naam, "type": "?", "rijen": 0, "kolommen": [],
                  "skus": [], "sample": [], "fout": None}
    try:
        raw = uploaded.read()
        uploaded.seek(0)  # zodat de gebruiker 'm later opnieuw kan lezen
        lower = naam.lower()

        if lower.endswith((".xlsx", ".xls")):
            df = pd.read_excel(io.BytesIO(raw), dtype=str, engine="openpyxl")
            info["type"] = "excel"
        elif lower.endswith(".csv"):
            for enc in ("utf-8-sig", "utf-8", "cp1252"):
                try:
                    df = pd.read_csv(io.BytesIO(raw), dtype=str, encoding=enc,
                                     keep_default_na=False)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                info["fout"] = "CSV niet te lezen — onbekende encoding."
                return info
            info["type"] = "csv"
        elif lower.endswith(".txt"):
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                text = raw.decode("cp1252", errors="ignore")
            regels = [ln.strip() for ln in text.splitlines() if ln.strip()]
            info.update({
                "type": "txt", "rijen": len(regels),
                "skus": regels[:1000],
                "sample": [{"regel": r} for r in regels[:5]],
            })
            return info
        else:
            info["fout"] = f"Bestandstype niet ondersteund: {lower.split('.')[-1]}"
            return info

        df = df.fillna("")
        info["rijen"] = len(df)
        info["kolommen"] = list(df.columns)

        # Zoek SKU-kolom
        for kandidaat in ("sku", "SKU", "Sku", "brand_id", "Variant SKU",
                           "artikelnummer", "Artikelnummer"):
            if kandidaat in df.columns:
                info["skus"] = [str(s).strip() for s in df[kandidaat].tolist()
                                 if str(s).strip()][:1000]
                break
        # Sample: eerste 5 rijen, alleen tekst-velden, ingekort
        for _, row in df.head(5).iterrows():
            info["sample"].append({
                k: (str(v)[:80] if v else "") for k, v in row.items()
            })
    except Exception as e:
        info["fout"] = f"Parse-fout: {e}"
    return info


def _bestand_context_voor_claude(parsed_files: list[dict]) -> str:
    """Bouw een context-bericht over de geüploade bestanden voor Claude."""
    if not parsed_files:
        return ""
    delen = ["📎 **Bijgevoegde bestanden:**"]
    for f in parsed_files:
        if f.get("fout"):
            delen.append(f"\n- `{f['naam']}` — FOUT: {f['fout']}")
            continue
        regel = f"\n- `{f['naam']}` ({f['type']}, {f['rijen']} rijen)"
        if f.get("kolommen"):
            kols = ", ".join(f["kolommen"][:15])
            regel += f"\n  Kolommen: {kols}"
        if f.get("skus"):
            sample_skus = ", ".join(f["skus"][:5])
            extra = f" … (+{len(f['skus']) - 5} meer)" if len(f["skus"]) > 5 else ""
            regel += f"\n  SKU's gedetecteerd ({len(f['skus'])}): {sample_skus}{extra}"
        if f.get("sample"):
            regel += f"\n  Eerste rij: {f['sample'][0]}"
        delen.append(regel)
    return "\n".join(delen)


# ── Tool uitvoering ───────────────────────────────────────────────────────────

def _uitvoer_zoek(params: dict) -> list[dict]:
    sb = _sb()
    if not sb:
        return []
    q = sb.table("shopify_meta_audit").select(
        "handle,product_title,vendor,current_meta_title,current_meta_description,"
        "title_status,desc_status,product_status"
    )
    if params.get("product_status"):
        q = q.eq("product_status", params["product_status"])
    # geen standaard-filter — laat alle statussen door als niet opgegeven

    if params.get("product_title_bevat"):
        q = q.ilike("product_title", f"%{params['product_title_bevat']}%")
    if params.get("meta_title_bevat"):
        q = q.ilike("current_meta_title", f"%{params['meta_title_bevat']}%")
    if params.get("meta_desc_bevat"):
        q = q.ilike("current_meta_description", f"%{params['meta_desc_bevat']}%")
    if params.get("vendor"):
        q = q.ilike("vendor", f"%{params['vendor']}%")
    if params.get("title_status"):
        q = q.eq("title_status", params["title_status"])
    if params.get("desc_status"):
        q = q.eq("desc_status", params["desc_status"])

    limit = min(int(params.get("limit", 200)), 500)
    rows = q.limit(limit).execute().data or []

    sku_m = _sku_map()
    for r in rows:
        r["sku"] = sku_m.get(r.get("handle", ""), "—")
    return rows


def _uitvoer_zoek_pipeline(params: dict) -> list[dict]:
    sb = _sb()
    if not sb:
        return []

    # Als shopify_status filter: haal handles op uit shopify_meta_audit
    handle_whitelist: list[str] | None = None
    if params.get("shopify_status"):
        q_audit = sb.table("shopify_meta_audit").select("handle,vendor") \
            .eq("product_status", params["shopify_status"])
        if params.get("supplier"):
            q_audit = q_audit.ilike("vendor", f"%{params['supplier']}%")
        audit_rows = q_audit.limit(1000).execute().data or []
        handle_whitelist = [r["handle"] for r in audit_rows if r.get("handle")]
        if not handle_whitelist:
            return []

    q = sb.table("products_curated").select(
        "id,sku,supplier,fase,product_title_nl,handle,"
        "hoofdcategorie,sub_subcategorie,pipeline_status,"
        "verkoopprijs,meta_description"
    )
    if params.get("supplier") and not params.get("shopify_status"):
        q = q.ilike("supplier", f"%{params['supplier']}%")
    if params.get("pipeline_status"):
        q = q.eq("pipeline_status", params["pipeline_status"])
    if params.get("fase"):
        q = q.eq("fase", str(params["fase"]))
    if params.get("titel_bevat"):
        q = q.ilike("product_title_nl", f"%{params['titel_bevat']}%")
    if params.get("hoofdcategorie_bevat"):
        q = q.ilike("hoofdcategorie", f"%{params['hoofdcategorie_bevat']}%")
    if handle_whitelist is not None:
        q = q.in_("handle", handle_whitelist[:500])

    limit = min(int(params.get("limit", 200)), 500)
    rows = q.limit(limit).execute().data or []

    # Voeg shopify_status toe als label
    if params.get("shopify_status"):
        for r in rows:
            r["shopify_status"] = params["shopify_status"]
    return rows


def _uitvoer_status_voor_skus(skus: list[str]) -> dict:
    """Geef per SKU een status-overzicht + aggregaat. Maximaal 500 SKU's."""
    sb = _sb()
    if not sb:
        return {"fout": "Geen Supabase-verbinding."}
    skus = [s for s in skus if s][:500]
    if not skus:
        return {"fout": "Lege SKU-lijst."}

    # products_curated → handle, hoofdcategorie, pipeline_status, supplier
    curated: dict[str, dict] = {}
    for i in range(0, len(skus), 200):
        chunk = skus[i:i + 200]
        try:
            res = sb.table("products_curated").select(
                "sku,handle,supplier,hoofdcategorie,pipeline_status,product_title_nl"
            ).in_("sku", chunk).execute().data or []
            for r in res:
                curated[r["sku"]] = r
        except Exception:
            pass

    # shopify_meta_audit → product_status, current_meta_title, current_meta_description
    handles = [c["handle"] for c in curated.values() if c.get("handle")]
    audit_by_handle: dict[str, dict] = {}
    if handles:
        for i in range(0, len(handles), 200):
            chunk = handles[i:i + 200]
            try:
                res = sb.table("shopify_meta_audit").select(
                    "handle,product_status,current_meta_title,current_meta_description,vendor"
                ).in_("handle", chunk).execute().data or []
                for r in res:
                    audit_by_handle[r["handle"]] = r
            except Exception:
                pass

    # Per SKU samenvoegen
    per_sku: list[dict] = []
    aggr = {
        "totaal":            len(skus),
        "niet_in_pipeline":  0,
        "active":            0,
        "archived":          0,
        "draft":             0,
        "niet_in_shopify":   0,
        "zonder_meta_title": 0,
        "zonder_meta_desc":  0,
        "zonder_categorie":  0,
        "pipeline_ready":    0,
    }
    vendor_telling: dict[str, int] = {}

    for sku in skus:
        cur = curated.get(sku)
        if not cur:
            aggr["niet_in_pipeline"] += 1
            per_sku.append({"sku": sku, "status": "niet in pipeline"})
            continue
        handle = cur.get("handle", "")
        aud = audit_by_handle.get(handle, {}) if handle else {}
        vendor = aud.get("vendor") or cur.get("supplier", "")
        if vendor:
            vendor_telling[vendor] = vendor_telling.get(vendor, 0) + 1

        shop_status = aud.get("product_status") or "niet_in_shopify"
        if shop_status in aggr:
            aggr[shop_status] += 1
        has_title = bool((aud.get("current_meta_title") or "").strip())
        has_desc  = bool((aud.get("current_meta_description") or "").strip())
        has_cat   = bool((cur.get("hoofdcategorie") or "").strip())
        if not has_title: aggr["zonder_meta_title"] += 1
        if not has_desc:  aggr["zonder_meta_desc"] += 1
        if not has_cat:   aggr["zonder_categorie"] += 1
        if (cur.get("pipeline_status") or "") == "ready":
            aggr["pipeline_ready"] += 1

        per_sku.append({
            "sku":             sku,
            "vendor":          vendor or "—",
            "handle":          handle or "—",
            "shopify":         shop_status,
            "meta_title":      "✓" if has_title else "—",
            "meta_desc":       "✓" if has_desc else "—",
            "categorie":       cur.get("hoofdcategorie") or "—",
            "pipeline_status": cur.get("pipeline_status") or "—",
            "titel_nl":        cur.get("product_title_nl") or "—",
        })

    # Probleem-rijen voor detail (max 30) — als minstens één issue
    problemen = [
        r for r in per_sku
        if r.get("status") == "niet in pipeline"
        or r.get("shopify") == "niet_in_shopify"
        or r.get("meta_title") == "—"
        or r.get("meta_desc") == "—"
        or r.get("categorie") == "—"
    ][:30]

    return {
        "aggregaat":       aggr,
        "vendor_telling":  dict(sorted(vendor_telling.items(), key=lambda x: -x[1])[:15]),
        "probleem_rijen":  problemen,
        "totaal_rijen":    len(per_sku),
    }


def _uitvoer_update(updates: list[dict]) -> int:
    sb = _sb()
    if not sb:
        return 0
    saved = 0
    for u in updates:
        handle = u.get("handle", "")
        veld   = u.get("veld", "")
        waarde = u.get("nieuwe_waarde", "")
        if not handle or veld not in ("current_meta_title", "current_meta_description"):
            continue
        try:
            sb.table("shopify_meta_audit").update({veld: waarde}).eq("handle", handle).execute()
            saved += 1
        except Exception:
            pass
    return saved


# ── Claude agent loop ─────────────────────────────────────────────────────────

def _run_claude(messages: list[dict]) -> tuple[str, list[dict] | None]:
    """
    Voer één Claude-ronde uit met tool use.
    Geeft (antwoord_tekst, voorgestelde_updates_of_None) terug.
    """
    from anthropic import Anthropic
    client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))

    pending_updates: list[dict] | None = None
    tool_results_pending = []
    current_messages = list(messages)

    while True:
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2048,
            system=SYSTEM,
            tools=TOOLS,
            messages=current_messages,
        )

        # Verwerk tool-calls
        tool_calls = [b for b in resp.content if b.type == "tool_use"]

        if not tool_calls:
            # Geen tools meer — haal tekst op
            tekst = " ".join(b.text for b in resp.content if hasattr(b, "text")).strip()
            return tekst, pending_updates

        # Voer tools uit
        tool_results_pending = []
        for tc in tool_calls:
            if tc.name == "zoek_producten":
                try:
                    data = _uitvoer_zoek(tc.input)
                    result_str = f"{len(data)} producten gevonden in shopify_meta_audit.\n" + str(data[:100])
                except Exception as e:
                    result_str = f"Fout: {e}"
                tool_results_pending.append({
                    "type": "tool_result",
                    "tool_use_id": tc.id,
                    "content": result_str,
                })

            elif tc.name == "zoek_pipeline":
                try:
                    data = _uitvoer_zoek_pipeline(tc.input)
                    result_str = f"{len(data)} producten gevonden in seo_products (pipeline).\n" + str(data[:100])
                except Exception as e:
                    result_str = f"Fout: {e}"
                tool_results_pending.append({
                    "type": "tool_result",
                    "tool_use_id": tc.id,
                    "content": result_str,
                })

            elif tc.name == "stel_updates_voor":
                pending_updates = tc.input.get("updates", [])
                sam = tc.input.get("samenvatting", "")
                tool_results_pending.append({
                    "type": "tool_result",
                    "tool_use_id": tc.id,
                    "content": f"Updates klaargezet voor bevestiging: {len(pending_updates)} items. Samenvatting: {sam}",
                })

            elif tc.name == "status_voor_skus":
                try:
                    skus = [s for s in (tc.input.get("skus") or []) if s]
                    if not skus:
                        # Fallback: gebruik laatst-geüploade SKU's als gebruiker
                        # ze niet expliciet noemt.
                        skus = st.session_state.get("chat_geuploade_skus") or []
                    if not skus:
                        result_str = "Geen SKU's opgegeven en geen recent geüploade SKU's gevonden."
                    else:
                        status = _uitvoer_status_voor_skus(skus)
                        result_str = f"Status van {len(skus)} SKU's:\n{status}"
                except Exception as e:
                    result_str = f"Fout: {e}"
                tool_results_pending.append({
                    "type": "tool_result",
                    "tool_use_id": tc.id,
                    "content": result_str,
                })

            elif tc.name == "open_in_pipeline":
                pijplijn = (tc.input.get("pijplijn") or "").strip()
                skus = [s for s in (tc.input.get("skus") or []) if s]
                reden = (tc.input.get("reden") or "").strip()
                if pijplijn not in _PIJPLIJN_PAGES:
                    result_str = f"Onbekende pijplijn '{pijplijn}'."
                else:
                    st.session_state["chat_route_pending"] = {
                        "pijplijn": pijplijn,
                        "page":     _PIJPLIJN_PAGES[pijplijn],
                        "skus":     skus,
                        "reden":    reden,
                    }
                    result_str = (
                        f"Routing voorgesteld: '{pijplijn}' ({len(skus)} SKU's). "
                        "Open-knop verschijnt in UI. Bevestig kort."
                    )
                tool_results_pending.append({
                    "type": "tool_result",
                    "tool_use_id": tc.id,
                    "content": result_str,
                })

            elif tc.name == "bouw_hextom_export":
                try:
                    from tab_herverwerk import _build_hextom_excel, _load_by_skus
                    skus = [s for s in (tc.input.get("skus") or []) if s]
                    if not skus:
                        result_str = "Fout: lege SKU-lijst — geef een lijst SKU's mee."
                    else:
                        rows = _load_by_skus(skus)
                        if not rows:
                            result_str = (
                                f"Geen producten gevonden in products_raw voor de {len(skus)} "
                                "SKU's. Controleer of de SKU's kloppen."
                            )
                        else:
                            xlsx_bytes = _build_hextom_excel(rows)
                            voorgesteld = (tc.input.get("bestandsnaam") or "hextom_export").strip()
                            voorgesteld = re.sub(r"[^A-Za-z0-9_-]+", "_", voorgesteld).strip("_")
                            if not voorgesteld:
                                voorgesteld = "hextom_export"
                            filename = f"{voorgesteld}_{len(rows)}st.xlsx"
                            st.session_state["chat_download_pending"] = {
                                "filename": filename,
                                "data":     xlsx_bytes,
                                "count":    len(rows),
                                "gemist":   len(skus) - len(rows),
                            }
                            result_str = (
                                f"Hextom Excel klaar: {len(rows)} producten in '{filename}'. "
                                f"De download-knop verschijnt in de UI. "
                                "Bevestig kort dat het klaarstaat — plak GEEN inhoud."
                            )
                except Exception as e:
                    result_str = f"Fout bij bouwen Hextom: {e}"
                tool_results_pending.append({
                    "type": "tool_result",
                    "tool_use_id": tc.id,
                    "content": result_str,
                })

        # Voeg assistant-bericht + tool-resultaten toe
        current_messages = current_messages + [
            {"role": "assistant", "content": resp.content},
            {"role": "user",      "content": tool_results_pending},
        ]


# ── Render ────────────────────────────────────────────────────────────────────

def render() -> None:
    st.subheader("Assistent")
    st.caption(
        "Stel vragen over de productdata. Claude zoekt in de database en stelt "
        "aanpassingen voor — jij bevestigt altijd voordat er iets wordt gewijzigd."
    )

    with st.expander("Voorbeeldvragen"):
        st.markdown(
            "**Pipeline (seo_products):**\n"
            "- Welke Pottery Pots producten staan gearchiveerd?\n"
            "- Hoeveel Serax producten hebben status_shopify 'archief'?\n"
            "- Toon alle producten in fase 4 die nog niet geëxporteerd zijn\n"
            "- Welke producten van Printworks staan op 'nieuw'?\n\n"
            "**SEO / live Shopify (shopify_meta_audit):**\n"
            "- Toon alle Pottery Pots zonder meta description die live staan\n"
            "- Welke meta titles zijn langer dan 58 tekens?\n"
            "- Hoeveel producten hebben een templated description?\n"
            "- Welke producten hebben 2x 'Serax' in de producttitel?\n"
            "- Verwijder het dubbele 'Serax' uit de gevonden titels"
        )

    # Chat-geschiedenis
    if "chat_history" not in st.session_state:
        st.session_state["chat_history"] = []
    if "chat_pending_updates" not in st.session_state:
        st.session_state["chat_pending_updates"] = None
    if "chat_download_pending" not in st.session_state:
        st.session_state["chat_download_pending"] = None

    # Toon berichten
    for msg in st.session_state["chat_history"]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            # Toon bijgevoegde data-tabel als die er is
            if msg.get("data"):
                df = pd.DataFrame(msg["data"])
                shop_store = os.getenv("SHOPIFY_STORE", "")
                col_cfg = {}
                if "sku" in df.columns:
                    col_cfg["sku"] = st.column_config.TextColumn("SKU", width="small")
                if "URL" in df.columns and shop_store:
                    col_cfg["URL"] = st.column_config.LinkColumn("", width="small", display_text="🔗")
                st.dataframe(df, hide_index=True, use_container_width=True,
                             column_config=col_cfg if col_cfg else None)

    # Wachtende updates tonen + bevestigen
    if st.session_state["chat_pending_updates"]:
        updates = st.session_state["chat_pending_updates"]
        st.divider()
        st.markdown(f"### ✋ Bevestig {len(updates)} aanpassing(en)")
        shop_store = os.getenv("SHOPIFY_STORE", "")

        preview = []
        for u in updates:
            h = u.get("handle", "")
            preview.append({
                "SKU":           u.get("sku", "—"),
                "Product":       u.get("product_title", h),
                "Veld":          u.get("veld", ""),
                "Oud":           (u.get("oude_waarde") or "—")[:60],
                "Nieuw":         u.get("nieuwe_waarde", "")[:60],
                "Reden":         u.get("reden", "")[:60],
                "URL":           f"https://{shop_store}/products/{h}" if shop_store else "",
            })
        col_cfg_p = {
            "SKU":     st.column_config.TextColumn("SKU",     width="small"),
            "Product": st.column_config.TextColumn("Product", width="medium"),
            "Veld":    st.column_config.TextColumn("Veld",    width="small"),
            "Oud":     st.column_config.TextColumn("Oud",     width="medium"),
            "Nieuw":   st.column_config.TextColumn("Nieuw",   width="medium"),
            "Reden":   st.column_config.TextColumn("Reden",   width="medium"),
        }
        if shop_store:
            col_cfg_p["URL"] = st.column_config.LinkColumn("", width="small", display_text="🔗")
        st.dataframe(pd.DataFrame(preview), hide_index=True, use_container_width=True,
                     column_config=col_cfg_p)

        col_ok, col_af, col_dl = st.columns([2, 2, 2])
        with col_ok:
            if st.button("✅ Ja, doorvoeren", type="primary", key="chat_confirm"):
                n = _uitvoer_update(updates)
                st.session_state["chat_pending_updates"] = None
                st.session_state["chat_history"].append({
                    "role": "assistant",
                    "content": f"✅ **{n} producten bijgewerkt** in shopify_meta_audit.",
                })
                st.rerun()
        with col_af:
            if st.button("❌ Annuleer", key="chat_cancel"):
                st.session_state["chat_pending_updates"] = None
                st.session_state["chat_history"].append({
                    "role": "assistant",
                    "content": "Aanpassingen geannuleerd — er is niets gewijzigd.",
                })
                st.rerun()
        with col_dl:
            buf = io.BytesIO()
            pd.DataFrame(preview).to_excel(buf, index=False)
            st.download_button(
                "📥 Download als Excel",
                data=buf.getvalue(),
                file_name="voorgestelde_updates.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="chat_dl",
            )

    # Wachtende Hextom-download
    if st.session_state["chat_download_pending"]:
        dl = st.session_state["chat_download_pending"]
        st.divider()
        gemist_txt = f" ({dl['gemist']} SKU's niet gevonden)" if dl.get("gemist") else ""
        st.markdown(f"### 📥 Hextom Excel klaar — {dl['count']} producten{gemist_txt}")
        c_dl, c_clr = st.columns([3, 1])
        with c_dl:
            st.download_button(
                f"💾 Download {dl['filename']}",
                data=dl["data"],
                file_name=dl["filename"],
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="chat_hextom_dl",
                type="primary",
            )
        with c_clr:
            if st.button("Sluit", key="chat_hextom_clr"):
                st.session_state["chat_download_pending"] = None
                st.rerun()

    # Wachtende routing-suggestie
    if st.session_state.get("chat_route_pending"):
        r = st.session_state["chat_route_pending"]
        st.divider()
        st.markdown(
            f"### 💡 Voorstel: open in **{r['pijplijn']}** "
            f"({len(r.get('skus') or [])} SKU's)"
        )
        if r.get("reden"):
            st.caption(r["reden"])

        c_go, c_no = st.columns([3, 1])
        with c_go:
            if st.button(f"➡ Open in {r['pijplijn']}", type="primary", key="chat_route_go"):
                # Preload SKUs voor herverwerk zodat de pagina meteen vol staat
                if r["pijplijn"] == "herverwerk" and r.get("skus"):
                    try:
                        from tab_herverwerk import _load_by_skus
                        with st.spinner(f"Voorladen van {len(r['skus'])} producten..."):
                            rows = _load_by_skus(r["skus"])
                        st.session_state["hv_geladen"] = True
                        st.session_state["hv_rows_override"] = rows
                    except Exception as e:
                        st.warning(f"Voorladen mislukt: {e}")
                # Voor andere pagina's: laat SKU's beschikbaar als generieke context
                if r.get("skus"):
                    st.session_state["chat_geuploade_skus"] = r["skus"]
                page = r.get("page", "")
                st.session_state["chat_route_pending"] = None
                if page:
                    st.switch_page(page)
        with c_no:
            if st.button("Niet nu", key="chat_route_no"):
                st.session_state["chat_route_pending"] = None
                st.rerun()

    # Chat-input — accepteer bestanden (Excel/CSV/TXT) naast tekst
    vraag = st.chat_input(
        "Stel een vraag of sleep een bestand erin...",
        accept_file="multiple",
        file_type=["xlsx", "xls", "csv", "txt"],
    )
    if vraag:
        # vraag is ofwel een string (geen file-upload) ofwel een ChatInputValue-object
        tekst = getattr(vraag, "text", vraag if isinstance(vraag, str) else "") or ""
        uploaded_files = getattr(vraag, "files", []) or []

        parsed_files: list[dict] = []
        for f in uploaded_files:
            parsed_files.append(_parse_uploaded_file(f))

        delen: list[str] = []
        if tekst.strip():
            delen.append(tekst.strip())
        if parsed_files:
            delen.append(_bestand_context_voor_claude(parsed_files))
        user_msg = "\n\n".join(delen).strip()
        if not user_msg:
            return

        # Zet alle geüploade SKU's in session_state zodat Claude ze kan oppakken
        if parsed_files:
            alle_skus: list[str] = []
            for pf in parsed_files:
                alle_skus.extend(pf.get("skus") or [])
            if alle_skus:
                st.session_state["chat_geuploade_skus"] = alle_skus

        st.session_state["chat_history"].append({"role": "user", "content": user_msg})
        with st.chat_message("user"):
            st.markdown(user_msg)

        with st.chat_message("assistant"):
            with st.spinner("Bezig..."):
                # Bouw API-berichten op (alleen tekst, geen data-objecten)
                api_msgs = [
                    {"role": m["role"], "content": m["content"]}
                    for m in st.session_state["chat_history"]
                    if m["role"] in ("user", "assistant") and isinstance(m["content"], str)
                ]
                try:
                    antwoord, pending = _run_claude(api_msgs)
                except Exception as e:
                    antwoord = f"❌ Fout: {e}"
                    pending = None

            st.markdown(antwoord)
            entry: dict = {"role": "assistant", "content": antwoord}

            if pending:
                st.session_state["chat_pending_updates"] = pending

            st.session_state["chat_history"].append(entry)
            st.rerun()

    # Wis-knop
    if st.session_state["chat_history"]:
        if st.button("🗑 Wis gesprek", key="chat_wis"):
            st.session_state["chat_history"] = []
            st.session_state["chat_pending_updates"] = None
            st.session_state["chat_download_pending"] = None
            st.rerun()
