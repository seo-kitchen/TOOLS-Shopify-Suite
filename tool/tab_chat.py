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
            "Zoek producten in seo_products — onze INTERNE pipeline database. "
            "Bevat ALLE producten die we hebben geïmporteerd, inclusief gearchiveerde, "
            "nog niet live, en producten in bewerking. "
            "Gebruik dit voor vragen over: welke producten zijn gearchiveerd, "
            "hoeveel producten per merk, pipeline-status, prijzen, categorieën. "
            "status_shopify waarden: 'actief', 'archief', 'nieuw', 'onbekend' (Nederlands). "
            "merk waarden: 'Serax', 'Pottery Pots', 'Printworks', 'S&P/Bonbistro'. "
            "status waarden: 'raw', 'matched', 'ready', 'review', 'exported'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "merk":                  {"type": "string", "description": "Leverancier/merk (gedeeltelijk, case-insensitive)"},
                "status_shopify":        {"type": "string", "enum": ["actief", "archief", "nieuw", "onbekend"]},
                "status":                {"type": "string", "enum": ["raw", "matched", "ready", "review", "exported"]},
                "fase":                  {"type": "string", "description": "Fase nummer: '1' t/m '6'"},
                "naam_bevat":            {"type": "string", "description": "Zoek in product_name_raw en product_title_nl"},
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
]

SYSTEM = """Je bent een data-assistent voor een Nederlandse webshop (SEOkitchen).
Je hebt toegang tot twee databases via tools.

## Welke tool gebruik je wanneer?

**zoek_pipeline** (seo_products — onze interne database):
- Vragen over gearchiveerde / niet-live producten
- Vragen over hoeveel producten er zijn per merk, status, fase
- Vragen over inkoopprijs, verkoopprijs, categorieën
- Altijd als de gebruiker vraagt naar 'archief' of 'gearchiveerd' → gebruik status_shopify='archief'
- Altijd als de gebruiker vraagt naar een merk (Pottery Pots, Serax, etc.) zonder specifieke SEO-vraag

**zoek_producten** (shopify_meta_audit — live Shopify data):
- Vragen over meta titles, meta descriptions, SEO-kwaliteit
- Vragen over live Shopify status (active/draft/archived in het Engels)
- Vragen over producten die al live staan

## Gedragsregels
- Gebruik altijd eerst een zoek-tool om data op te halen voordat je conclusies trekt.
- Stel updates voor via stel_updates_voor — schrijf NOOIT direct zonder bevestiging.
- Wees bondig: geef een korte samenvatting + de data, geen lange uitleg.
- Taal: Nederlands.
- Als een vraag onduidelijk is: zoek in beide databases en combineer de resultaten.
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
    sb = _sb_pipeline()
    if not sb:
        return []
    q = sb.table("seo_products").select(
        "id,sku,ean_shopify,product_name_raw,product_title_nl,"
        "merk,fase,status,status_shopify,"
        "hoofdcategorie,sub_subcategorie,"
        "rrp_stuk_eur,rrp_gb_eur"
    )
    if params.get("merk"):
        q = q.ilike("merk", f"%{params['merk']}%")
    if params.get("status_shopify"):
        q = q.eq("status_shopify", params["status_shopify"])
    if params.get("status"):
        q = q.eq("status", params["status"])
    if params.get("fase"):
        q = q.eq("fase", str(params["fase"]))
    if params.get("naam_bevat"):
        term = params["naam_bevat"]
        q = q.or_(f"product_name_raw.ilike.%{term}%,product_title_nl.ilike.%{term}%")
    if params.get("hoofdcategorie_bevat"):
        q = q.ilike("hoofdcategorie", f"%{params['hoofdcategorie_bevat']}%")

    limit = min(int(params.get("limit", 200)), 500)
    return q.limit(limit).execute().data or []


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

        # Voeg assistant-bericht + tool-resultaten toe
        current_messages = current_messages + [
            {"role": "assistant", "content": resp.content},
            {"role": "user",      "content": tool_results_pending},
        ]


# ── Render ────────────────────────────────────────────────────────────────────

def render() -> None:
    st.subheader("💬 Data-assistent")
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

    # Chat-input
    vraag = st.chat_input("Stel een vraag over de productdata...")
    if vraag:
        st.session_state["chat_history"].append({"role": "user", "content": vraag})
        with st.chat_message("user"):
            st.markdown(vraag)

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
            st.rerun()
