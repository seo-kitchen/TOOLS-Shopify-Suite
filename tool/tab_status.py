"""Tab 4 — Status & Analyses.

Traffic-light overzicht gebaseerd op ALLEEN actieve (live) Shopify-producten.
Gearchiveerde producten worden volledig genegeerd.

Drie niveaus:
  🔴 Kritiek  — moet nu opgelost worden
  🟠 Let op   — verbetering nodig, niet urgent
  🟢 OK       — alles goed

Daarna: Hextom wachtrij + export geschiedenis.
"""
from __future__ import annotations

import io
import os
from pathlib import Path

import pandas as pd
import streamlit as st

from ui.supabase_client import get_supabase
from client import get_client_id
from export_log import get_history, get_pending, confirm_applied


def _shopify_read():
    import importlib.util
    p = Path(__file__).parent.parent / "execution" / "shopify_read.py"
    spec = importlib.util.spec_from_file_location("shopify_read_mod", p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@st.cache_resource
def _get_sb_new():
    from supabase import create_client
    from dotenv import load_dotenv
    load_dotenv()
    url = os.getenv("SUPABASE_NEW_URL", "")
    key = os.getenv("SUPABASE_NEW_SERVICE_KEY", "") or os.getenv("SUPABASE_NEW_KEY", "")
    if not url or not key:
        return None
    return create_client(url, key)

TITLE_MAX = 60
DESC_MIN = 120
DESC_MAX = 155


# ── Data ophalen ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=180, show_spinner=False)
def _shopify_active_count() -> dict:
    """Aantal actieve producten in Shopify (via REST API)."""
    try:
        import os, requests
        from dotenv import load_dotenv
        load_dotenv()
        store = os.getenv("SHOPIFY_STORE", "")
        token = os.getenv("SHOPIFY_ACCESS_TOKEN", "")
        if not store or not token:
            return {"error": "SHOPIFY_STORE of SHOPIFY_ACCESS_TOKEN ontbreekt in .env"}
        base = f"https://{store}/admin/api/2026-04"
        headers = {"X-Shopify-Access-Token": token}
        results = {}
        for status in ("active", "draft", "archived"):
            r = requests.get(f"{base}/products/count.json",
                             headers=headers, params={"status": status}, timeout=10)
            results[status] = r.json().get("count", 0) if r.ok else "?"
        return results
    except Exception as e:
        return {"error": str(e)}


@st.cache_data(ttl=600, show_spinner=False)
def _sku_by_handle() -> dict[str, str]:
    """Bouw een handle → SKU mapping. Primair uit shopify_meta_audit, fallback op shopify_sync."""
    try:
        sb = _get_sb_new()
        if not sb:
            return {}
        # Primair: SKU direct in shopify_meta_audit (gevuld door foto-sync)
        res = sb.table("shopify_meta_audit").select("handle,sku").execute()
        mapping = {r["handle"]: r["sku"] for r in (res.data or []) if r.get("handle") and r.get("sku")}
        # Fallback: shopify_sync voor ontbrekende handles
        if len(mapping) < 100:
            res2 = sb.table("shopify_sync").select("handle,sku").execute()
            for r in (res2.data or []):
                if r.get("handle") and r.get("sku") and r["handle"] not in mapping:
                    mapping[r["handle"]] = r["sku"]
        return mapping
    except Exception:
        return {}


@st.cache_data(ttl=180, show_spinner=False)
def _meta_detail(probleem: str) -> list[dict]:
    """
    Haalt de concrete producten op voor een specifiek meta-probleem.
    probleem: 'title_leeg' | 'desc_leeg' | 'templated' | 'title_lang' | 'desc_buiten'
    Voegt SKU toe via shopify_sync lookup.
    """
    try:
        sb = _get_sb_new()
        if not sb:
            return []
        q = sb.table("shopify_meta_audit").select(
            "handle,product_title,vendor,current_meta_title,current_meta_description"
        ).eq("product_status", "active")

        if probleem == "title_leeg":
            q = q.is_("current_meta_title", "null")
        elif probleem == "desc_leeg":
            q = q.is_("current_meta_description", "null")
        elif probleem == "templated":
            q = q.eq("desc_status", "templated")
        elif probleem == "title_lang":
            q = q.eq("title_status", "too_long")
        elif probleem == "desc_buiten":
            q = q.in_("desc_status", ["too_long", "too_short"])

        producten = q.order("vendor").limit(200).execute().data or []

        # SKU toevoegen via handle-lookup
        sku_map = _sku_by_handle()
        for p in producten:
            p["sku"] = sku_map.get(p.get("handle", ""), "—")

        return producten
    except Exception:
        return []


@st.cache_data(ttl=180, show_spinner=False)
def _meta_problemen_actief() -> dict:
    """Meta-problemen voor ACTIEVE producten uit shopify_meta_audit (nieuwe Supabase)."""
    try:
        sb = _get_sb_new()
        if not sb:
            return {"totaal": 0}
        res = (
            sb.table("shopify_meta_audit")
            .select("current_meta_title,current_meta_description,title_status,desc_status")
            .eq("product_status", "active")
            .execute()
        )
        data = res.data or []
        if not data:
            return {"totaal": 0}

        title_leeg  = sum(1 for r in data if not str(r.get("current_meta_title") or "").strip())
        desc_leeg   = sum(1 for r in data if not str(r.get("current_meta_description") or "").strip())
        title_lang  = sum(1 for r in data if len(str(r.get("current_meta_title") or "")) > TITLE_MAX)
        desc_buiten = sum(1 for r in data
                         if str(r.get("current_meta_description") or "").strip()
                         and not (DESC_MIN <= len(str(r.get("current_meta_description") or "")) <= DESC_MAX))
        templated   = sum(1 for r in data if r.get("desc_status") == "templated")
        return {
            "totaal":      len(data),
            "title_leeg":  title_leeg,
            "title_lang":  title_lang,
            "desc_leeg":   desc_leeg,
            "desc_buiten": desc_buiten,
            "templated":   templated,
        }
    except Exception as e:
        return {"error": str(e)}


@st.cache_data(ttl=300, show_spinner=False)
def _foto_status() -> dict:
    """Foto-status voor actieve producten uit shopify_meta_audit."""
    try:
        sb = _get_sb_new()
        if not sb:
            return {"gesynchroniseerd": False}
        res = (
            sb.table("shopify_meta_audit")
            .select("has_image,image_alt_status,image_name_status,has_description")
            .eq("product_status", "active")
            .execute()
        )
        data = res.data or []
        if not data or all(r.get("has_image") is None for r in data):
            return {"gesynchroniseerd": False}
        return {
            "gesynchroniseerd":   True,
            "totaal":             len(data),
            "geen_foto":          sum(1 for r in data if not r.get("has_image")),
            "geen_alt":           sum(1 for r in data if r.get("has_image") and r.get("image_alt_status") == "missing"),
            "supplier_naam":      sum(1 for r in data if r.get("image_name_status") == "supplier"),
            "geen_omschrijving":  sum(1 for r in data if r.get("has_description") is False),
        }
    except Exception as e:
        return {"gesynchroniseerd": False, "error": str(e)}


@st.cache_data(ttl=180, show_spinner=False)
def _foto_detail(probleem: str) -> list[dict]:
    """Concrete producten voor een foto-probleem."""
    try:
        sb = _get_sb_new()
        if not sb:
            return []
        q = sb.table("shopify_meta_audit").select(
            "handle,product_title,vendor,has_image,image_count,"
            "first_image_src,first_image_alt,image_alt_status,image_name_status"
        ).eq("product_status", "active")
        if probleem == "geen_foto":
            q = q.eq("has_image", False)
        elif probleem == "geen_alt":
            q = q.eq("has_image", True).eq("image_alt_status", "missing")
        elif probleem == "supplier_naam":
            q = q.eq("image_name_status", "supplier")
        elif probleem == "geen_omschrijving":
            q = q.eq("has_description", False)
        rows = q.order("vendor").limit(300).execute().data or []
        sku_m = _sku_by_handle()
        for r in rows:
            r["sku"] = sku_m.get(r.get("handle", ""), "—")
        return rows
    except Exception:
        return []


@st.cache_data(ttl=120, show_spinner=False)
def _pipeline_status() -> dict:
    """Actieve pipeline-producten per status (raw/review = aandacht nodig)."""
    try:
        sb = get_supabase()
        res = sb.table("seo_products").select("status,fase").execute()
        counts: dict[str, int] = {}
        for row in (res.data or []):
            s = row.get("status", "?")
            counts[s] = counts.get(s, 0) + 1
        return counts
    except Exception as e:
        return {"error": str(e)}


def _categorie_gaps() -> dict:
    """
    Vergelijkt cat_-tags van ACTIEVE producten (shopify_meta_audit) met
    live Shopify collecties.

    Geeft dict terug:
      cat_tags_used:   set van unieke cat_-waarden in actieve producten
      shopify_handles: set van collection handles in Shopify
      shopify_titels:  set van collection titles (lowercase)
      gaps:            cat_-tags zonder matching collectie (op titel of handle)
      error:           foutmelding of None
    """
    try:
        mod = _shopify_read()
        token = mod.get_token()
        df_cols = mod.haal_collecties(token)
        shopify_titels  = set(df_cols["titel"].str.strip().str.lower()) if "titel" in df_cols.columns else set()
        shopify_handles = set(df_cols["handle"].str.strip().str.lower()) if "handle" in df_cols.columns else set()
    except Exception as e:
        return {"error": f"Shopify API niet bereikbaar: {e}", "gaps": []}

    try:
        sb = _get_sb_new()
        if not sb:
            return {"error": "SUPABASE_NEW_URL ontbreekt", "gaps": []}
        res = (
            sb.table("shopify_meta_audit")
            .select("tags")
            .eq("product_status", "active")
            .execute()
        )
        # Extract cat_-tags uit de tag-strings
        cat_tags: set[str] = set()
        for r in (res.data or []):
            for tag in str(r.get("tags") or "").split(","):
                tag = tag.strip()
                if tag.startswith("cat_"):
                    cat_tags.add(tag[4:])  # strip "cat_" prefix

        gaps = sorted(
            t for t in cat_tags
            if t.lower() not in shopify_titels and t.lower() not in shopify_handles
        )
        return {
            "cat_tags_used":   cat_tags,
            "shopify_titels":  shopify_titels,
            "gaps":            gaps,
            "error":           None,
        }
    except Exception as e:
        return {"error": str(e), "gaps": []}


# ── Oplossen wizard ───────────────────────────────────────────────────────────

TITLE_FORMAT = (
    "Genereer een SEO meta title voor dit product op een Nederlandse webshop.\n"
    "Product: {title}\nVendor: {vendor}\n\n"
    "Regels:\n"
    "- Max 58 tekens\n"
    "- Format bij voorkeur: [Korte productnaam] | [Merk] – Interieur Shop\n"
    "- Als dat te lang is: [Korte productnaam] – Interieur Shop\n"
    "- Bevat product-type als focus-keyword\n"
    "- Gebruik 'je'-vorm\n"
    "Geef ALLEEN de meta title terug, geen uitleg."
)

DESC_FORMAT = (
    "Genereer een SEO meta description voor dit product op een Nederlandse webshop.\n"
    "Product: {title}\nVendor: {vendor}\n"
    "Huidige description: {desc}\n\n"
    "Regels:\n"
    "- Exact 120–155 tekens\n"
    "- Uniek, geen template-zin\n"
    "- Gebruik 'je'-vorm\n"
    "- Eindig met een CTA (bijv. 'Bestel nu', 'Bekijk', 'Ontdek')\n"
    "- Vermeld gratis verzending vanaf €75 als dat past\n"
    "Geef ALLEEN de meta description terug, geen uitleg."
)


def _genereer_claude(prompt: str) -> str:
    from anthropic import Anthropic
    client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=120,
        messages=[{"role": "user", "content": prompt}],
    )
    return msg.content[0].text.strip().strip('"')


def _render_oplossen_wizard(probleem_key: str, producten: list[dict]) -> None:
    """Inline fix-wizard: genereer meta titles/descriptions met Claude."""
    gen_title = probleem_key == "title_leeg"
    gen_desc  = probleem_key in ("desc_leeg", "templated", "desc_buiten")

    veld_label = "meta title" if gen_title else "meta description"
    state_key  = f"oplossen_resultaat_{probleem_key}"
    sb_new     = _get_sb_new()
    shop_store = os.getenv("SHOPIFY_STORE", "")

    st.divider()
    col_dl, col_gen = st.columns([1, 2])

    with col_dl:
        rows_dl = [{
            "SKU":     p.get("sku", "—"),
            "Handle":  p.get("handle", ""),
            "Product": p.get("product_title", ""),
            "Vendor":  p.get("vendor", ""),
            "Huidige meta title": p.get("current_meta_title", ""),
            "Huidige meta desc":  p.get("current_meta_description", ""),
        } for p in producten]
        buf = io.BytesIO()
        pd.DataFrame(rows_dl).to_excel(buf, index=False)
        st.download_button(
            f"📥 Download lijst ({len(producten)} producten)",
            data=buf.getvalue(),
            file_name=f"fix_{probleem_key}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"dl_{probleem_key}",
        )

    with col_gen:
        if st.button(
            f"✨ Genereer {veld_label} met Claude ({len(producten)} producten)",
            key=f"gen_{probleem_key}",
            type="primary",
        ):
            resultaten = []
            bar = st.progress(0, text="Bezig...")
            for i, p in enumerate(producten):
                title  = p.get("product_title", "")
                vendor = p.get("vendor", "")
                desc   = p.get("current_meta_description", "") or ""
                try:
                    if gen_title:
                        gegenereerd = _genereer_claude(TITLE_FORMAT.format(title=title, vendor=vendor))
                    else:
                        gegenereerd = _genereer_claude(DESC_FORMAT.format(title=title, vendor=vendor, desc=desc))
                except Exception as e:
                    gegenereerd = f"[FOUT: {e}]"
                resultaten.append({**p, "gegenereerd": gegenereerd})
                bar.progress((i + 1) / len(producten), text=f"{i+1}/{len(producten)} verwerkt")
            bar.empty()
            st.session_state[state_key] = resultaten
            st.rerun()

    # Toon resultaten als die er zijn
    if state_key in st.session_state:
        resultaten = st.session_state[state_key]
        st.markdown(f"**Preview — {len(resultaten)} gegenereerde {veld_label}s**")

        preview_rows = []
        for r in resultaten:
            gen = r.get("gegenereerd", "")
            tl  = len(gen)
            ok  = ("🟢" if (0 < tl <= TITLE_MAX if gen_title else DESC_MIN <= tl <= DESC_MAX) else "🟠")
            preview_rows.append({
                "": ok,
                "Product":     r.get("product_title", "")[:60],
                veld_label:    gen,
                "Tekens":      tl,
            })
        st.dataframe(pd.DataFrame(preview_rows), hide_index=True, use_container_width=True)

        col_save, col_exp, col_wis = st.columns([2, 2, 1])

        with col_save:
            if st.button("💾 Opslaan als suggestie in Supabase", key=f"save_{probleem_key}"):
                if sb_new:
                    saved = 0
                    for r in resultaten:
                        handle = r.get("handle", "")
                        if not handle:
                            continue
                        upd = (
                            {"suggested_meta_title": r["gegenereerd"]} if gen_title
                            else {"suggested_meta_description": r["gegenereerd"]}
                        )
                        try:
                            sb_new.table("shopify_meta_audit").update(upd).eq("handle", handle).execute()
                            saved += 1
                        except Exception:
                            pass
                    st.success(f"✅ {saved} suggesties opgeslagen in shopify_meta_audit.")
                else:
                    st.error("Supabase niet bereikbaar.")

        with col_exp:
            exp_rows = []
            for r in resultaten:
                h = r.get("handle", "")
                exp_rows.append({
                    "SKU":            r.get("sku", "—"),
                    "Handle":         h,
                    "Product":        r.get("product_title", ""),
                    "Nieuwe meta title" if gen_title else "Nieuwe meta description": r.get("gegenereerd", ""),
                    "URL": f"https://{shop_store}/products/{h}" if shop_store else "",
                })
            buf2 = io.BytesIO()
            pd.DataFrame(exp_rows).to_excel(buf2, index=False)
            st.download_button(
                "📥 Export naar Excel",
                data=buf2.getvalue(),
                file_name=f"gegenereerd_{probleem_key}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"exp_{probleem_key}",
            )

        with col_wis:
            if st.button("🗑 Wis", key=f"wis_{probleem_key}"):
                del st.session_state[state_key]
                st.rerun()


# ── Render helpers ────────────────────────────────────────────────────────────

def _actiepunt(niveau: str, tekst: str, detail: str = "") -> None:
    """Toon één actiepunt als gekleurde info-regel."""
    icon = {"rood": "🔴", "oranje": "🟠", "groen": "🟢"}.get(niveau, "⚪")
    st.markdown(f"{icon} {tekst}")
    if detail:
        st.caption(f"&nbsp;&nbsp;&nbsp;&nbsp;↳ {detail}")


def _sectie_header(titel: str, rood: int, oranje: int) -> None:
    """Sectie-header met badge-tellingen."""
    badges = []
    if rood:
        badges.append(f"🔴 {rood}")
    if oranje:
        badges.append(f"🟠 {oranje}")
    badge_str = "  ·  ".join(badges) if badges else "🟢 alles OK"
    st.markdown(f"#### {titel} &nbsp;&nbsp; {badge_str}")


# ── Render ────────────────────────────────────────────────────────────────────

def render() -> None:
    st.subheader("Status & Analyses")
    st.caption(
        "Gebaseerd op **actieve (live) Shopify-producten**. "
        "Gearchiveerde producten worden genegeerd."
    )

    sb = get_supabase()
    client_id = get_client_id()

    col_refresh, col_resync, col_spacer = st.columns([1, 1.4, 4])
    with col_refresh:
        if st.button("🔄 Ververs", key="st_refresh",
                     help="Wist alleen de cache. Snel — maar haalt geen nieuwe data uit Shopify."):
            st.cache_data.clear()
            st.rerun()
    with col_resync:
        do_resync = st.button(
            "🔁 Resync Shopify",
            key="st_resync_meta",
            help="Haalt alle actieve producten opnieuw op uit Shopify en herberekent "
                 "title/desc-status. Duurt ~30-60s. Gebruik dit na fixes in Shopify.",
        )
    if do_resync:
        import subprocess, sys as _sys
        root   = str(Path(__file__).parent.parent)
        script = str(Path(root) / "execution" / "shopify_meta_sync.py")
        with st.spinner("Shopify meta-sync bezig (~30-60s) ..."):
            result = subprocess.run(
                [_sys.executable, script],
                capture_output=True, text=True, cwd=root, timeout=300, encoding="utf-8",
            )
        if result.returncode == 0:
            st.success("✅ Shopify meta-sync klaar — data ververst.")
            st.cache_data.clear()
            st.rerun()
        else:
            st.error("❌ Sync faalde — zie log hieronder")
            with st.expander("Log", expanded=True):
                st.code(result.stdout + result.stderr or "(geen output)")

    # ── Shopify teller (context) ───────────────────────────────────────────────
    with st.spinner("Shopify ophalen..."):
        sh = _shopify_active_count()

    if "error" not in sh:
        c1, c2, c3 = st.columns(3)
        c1.metric("🟢 Actief in Shopify", sh.get("active", "?"))
        c2.metric("📝 Draft", sh.get("draft", "?"))
        c3.metric("📦 Gearchiveerd", sh.get("archived", "?"),
                  help="Gearchiveerd = wordt niet geanalyseerd")
    else:
        st.warning(f"Shopify API niet bereikbaar: {sh['error']}")

    st.divider()

    # ── Traffic light ─────────────────────────────────────────────────────────
    st.markdown("### 🚦 Overzicht")

    # Verzamel alle issues
    rood_items: list[tuple[str, str]] = []
    oranje_items: list[tuple[str, str]] = []
    groen_items: list[str] = []

    # 1. Hextom wachtrij
    pending = get_pending(sb, client_id)
    if pending:
        rood_items.append((
            f"**{len(pending)} export(s)** wachten op bevestiging in Hextom",
            "Ga naar tab 'Status' → Hextom wachtrij hieronder",
        ))
    else:
        groen_items.append("Alle Hextom exports bevestigd ✓")

    # 2. Meta kwaliteit (alleen actieve producten)
    with st.spinner("Meta analyseren..."):
        meta = _meta_problemen_actief()

    if "error" not in meta and meta.get("totaal", 0) > 0:
        if meta.get("title_leeg", 0) > 0:
            rood_items.append((
                f"**{meta['title_leeg']} actieve producten** zonder meta title",
                "Genereer via Tab 🌐 Collectie SEO",
            ))
        if meta.get("desc_leeg", 0) > 0:
            rood_items.append((
                f"**{meta['desc_leeg']} actieve producten** zonder meta description",
                "Genereer via Tab 🌐 Collectie SEO",
            ))
        if meta.get("templated", 0) > 0:
            rood_items.append((
                f"**{meta['templated']} meta descriptions** zijn standaard-templates",
                "Vervangen door unieke tekst via Tab 🌐 Collectie SEO",
            ))
        if meta.get("title_lang", 0) > 0:
            oranje_items.append((
                f"**{meta['title_lang']} meta titles** langer dan {TITLE_MAX} tekens",
                f"Inkorten tot max {TITLE_MAX} tekens",
            ))
        if meta.get("desc_buiten", 0) > 0:
            oranje_items.append((
                f"**{meta['desc_buiten']} meta descriptions** buiten bereik ({DESC_MIN}–{DESC_MAX} tekens)",
                "Aanpassen via Tab 🌐 Collectie SEO",
            ))
        if not any(meta.get(k, 0) for k in ("title_leeg", "desc_leeg", "title_lang", "desc_buiten", "templated")):
            groen_items.append(f"Meta teksten: alle {meta['totaal']} actieve producten OK ✓")
    elif meta.get("totaal", 0) == 0:
        oranje_items.append((
            "Shopify sync nog niet uitgevoerd",
            "Ga naar Tab 🔎 Inzicht → Shopify sync starten",
        ))

    # 3. Foto-status
    foto = _foto_status()
    if not foto.get("gesynchroniseerd"):
        oranje_items.append((
            "Foto-sync nog niet uitgevoerd",
            "Klik hieronder op 'Foto-sync starten' om foto-status te laden",
        ))
    else:
        if foto.get("geen_foto", 0) > 0:
            rood_items.append((
                f"**{foto['geen_foto']} actieve producten** zonder foto",
                "Upload foto's via Hextom Bulk Image Update",
            ))
        if foto.get("geen_alt", 0) > 0:
            rood_items.append((
                f"**{foto['geen_alt']} producten** zonder alt-tekst op eerste foto",
                "Alt-tekst toevoegen via Hextom Bulk Image Update",
            ))
        if foto.get("geen_omschrijving", 0) > 0:
            rood_items.append((
                f"**{foto['geen_omschrijving']} producten** zonder productomschrijving",
                "Voeg een omschrijving toe in Shopify of via Hextom bulk edit",
            ))
        if foto.get("supplier_naam", 0) > 0:
            oranje_items.append((
                f"**{foto['supplier_naam']} productfotos** hebben nog een leveranciersnaam als bestandsnaam",
                "Hernaam foto's via Hextom Bulk Image Update met SEO-vriendelijke naam",
            ))
        if not any(foto.get(k, 0) for k in ("geen_foto", "geen_alt", "geen_omschrijving", "supplier_naam")):
            groen_items.append(f"Foto's & omschrijvingen: alle {foto.get('totaal', 0)} producten compleet ✓")

    # 4. Pipeline vastgelopen?
    pipe = _pipeline_status()
    if "error" not in pipe:
        review_count = pipe.get("review", 0)
        if review_count > 0:
            oranje_items.append((
                f"**{review_count} producten** staan op status=review (validatiefouten)",
                "Controleer via dashboard_v2 → Validate",
            ))
        else:
            groen_items.append("Pipeline: geen producten vastgelopen in review ✓")

    # ── Toon traffic light ────────────────────────────────────────────────────
    n_rood   = len(rood_items)
    n_oranje = len(oranje_items)
    n_groen  = len(groen_items)

    tl1, tl2, tl3 = st.columns(3)
    with tl1:
        kleur = "🔴" if n_rood > 0 else "⬜"
        st.markdown(
            f"<div style='background:{'#fde8e8' if n_rood else '#f5f5f5'};"
            f"border-radius:8px;padding:16px;text-align:center'>"
            f"<h2 style='margin:0'>{kleur} {n_rood}</h2>"
            f"<p style='margin:4px 0 0 0;color:#666'>Kritiek</p></div>",
            unsafe_allow_html=True,
        )
    with tl2:
        kleur = "🟠" if n_oranje > 0 else "⬜"
        st.markdown(
            f"<div style='background:{'#fff3e0' if n_oranje else '#f5f5f5'};"
            f"border-radius:8px;padding:16px;text-align:center'>"
            f"<h2 style='margin:0'>{kleur} {n_oranje}</h2>"
            f"<p style='margin:4px 0 0 0;color:#666'>Let op</p></div>",
            unsafe_allow_html=True,
        )
    with tl3:
        kleur = "🟢" if n_groen > 0 and n_rood == 0 and n_oranje == 0 else ("🟢" if n_groen > 0 else "⬜")
        st.markdown(
            f"<div style='background:{'#e8f5e9' if n_groen else '#f5f5f5'};"
            f"border-radius:8px;padding:16px;text-align:center'>"
            f"<h2 style='margin:0'>{kleur} {n_groen}</h2>"
            f"<p style='margin:4px 0 0 0;color:#666'>OK</p></div>",
            unsafe_allow_html=True,
        )

    st.markdown("")

    # ── Actiepunten ───────────────────────────────────────────────────────────
    shop_store = os.getenv("SHOPIFY_STORE", "")

    def _toon_producten(probleem_key: str) -> None:
        producten = _meta_detail(probleem_key)
        if not producten:
            st.info("Geen producten gevonden.")
            return
        rows = []
        for p in producten:
            handle = p.get("handle") or ""
            desc = p.get("current_meta_description") or ""
            rows.append({
                "SKU":          p.get("sku") or "—",
                "Vendor":       p.get("vendor") or "—",
                "Product":      p.get("product_title") or handle,
                "Meta title":   p.get("current_meta_title") or "—",
                "Meta desc":    desc[:80] + ("…" if len(desc) > 80 else ""),
                "URL":          f"https://{shop_store}/products/{handle}" if shop_store and handle else "",
            })
        col_cfg = {
            "SKU":        st.column_config.TextColumn("SKU",        width="small"),
            "Vendor":     st.column_config.TextColumn("Vendor",     width="small"),
            "Product":    st.column_config.TextColumn("Product",    width="large"),
            "Meta title": st.column_config.TextColumn("Meta title", width="medium"),
            "Meta desc":  st.column_config.TextColumn("Meta desc",  width="large"),
        }
        if shop_store:
            col_cfg["URL"] = st.column_config.LinkColumn("", width="small", display_text="🔗")
        st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True, column_config=col_cfg)

    META_KEYS = {
        "title_leeg":   "title_leeg",
        "desc_leeg":    "desc_leeg",
        "templated":    "templated",
        "title_lang":   "title_lang",
        "desc_buiten":  "desc_buiten",
    }

    if rood_items or oranje_items:
        FOTO_KEYS = ("geen_foto", "geen_alt", "supplier_naam")

        def _toon_foto_producten(foto_key: str) -> None:
            rows = _foto_detail(foto_key)
            if not rows:
                st.info("Geen producten gevonden.")
                return
            shop_store = os.getenv("SHOPIFY_STORE", "")

            tabel = []
            for r in rows:
                h     = r.get("handle", "")
                fname = (r.get("first_image_src") or "").split("?")[0].split("/")[-1]
                base  = {
                    "SKU":     r.get("sku", "—"),
                    "Vendor":  r.get("vendor", "—"),
                    "Product": r.get("product_title", h),
                    "URL":     f"https://{shop_store}/products/{h}" if shop_store and h else "",
                }

                if foto_key == "geen_foto":
                    tabel.append({**base, "# Foto's": r.get("image_count", 0)})

                elif foto_key == "geen_alt":
                    tabel.append({**base,
                        "# Foto's":     r.get("image_count", 0),
                        "Alt-tekst":    r.get("first_image_alt") or "— ontbreekt",
                        "Bestandsnaam": fname or "—",
                    })

                elif foto_key == "supplier_naam":
                    tabel.append({**base,
                        "Bestandsnaam": fname or "—",
                        "Alt-tekst":    r.get("first_image_alt") or "—",
                    })

                elif foto_key == "geen_omschrijving":
                    tabel.append({**base,
                        "Omschrijving lengte": r.get("description_length", 0),
                    })

            if not tabel:
                st.info("Geen producten gevonden.")
                return

            # Kolom-config per type
            col_cfg = {
                "SKU":    st.column_config.TextColumn("SKU",    width="small"),
                "Vendor": st.column_config.TextColumn("Vendor", width="small"),
                "Product":st.column_config.TextColumn("Product",width="large"),
            }
            if foto_key == "geen_foto":
                col_cfg["# Foto's"] = st.column_config.NumberColumn("# Foto's", width="small")
            elif foto_key == "geen_alt":
                col_cfg["# Foto's"]     = st.column_config.NumberColumn("# Foto's",     width="small")
                col_cfg["Alt-tekst"]    = st.column_config.TextColumn("Alt-tekst",       width="medium")
                col_cfg["Bestandsnaam"] = st.column_config.TextColumn("Bestandsnaam",    width="medium")
            elif foto_key == "supplier_naam":
                col_cfg["Bestandsnaam"] = st.column_config.TextColumn("Bestandsnaam",    width="medium")
                col_cfg["Alt-tekst"]    = st.column_config.TextColumn("Alt-tekst",       width="medium")
            elif foto_key == "geen_omschrijving":
                col_cfg["Omschrijving lengte"] = st.column_config.NumberColumn(
                    "Omschrijving lengte", width="small",
                    help="0 = volledig leeg, >0 = te kort of gedeeltelijk gevuld"
                )
            if shop_store:
                col_cfg["URL"] = st.column_config.LinkColumn("", width="small", display_text="🔗")

            st.dataframe(pd.DataFrame(tabel), hide_index=True, use_container_width=True, column_config=col_cfg)

            # Labels per type
            labels = {
                "geen_foto":         "producten_zonder_foto",
                "geen_alt":          "producten_zonder_alt_tekst",
                "supplier_naam":     "producten_leveranciersnaam",
                "geen_omschrijving": "producten_zonder_omschrijving",
            }
            buf = io.BytesIO()
            pd.DataFrame(tabel).to_excel(buf, index=False)
            st.download_button(
                "📥 Download lijst",
                data=buf.getvalue(),
                file_name=f"{labels.get(foto_key, foto_key)}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"dl_foto_{foto_key}",
            )

        def _render_omschrijving_wizard(producten: list[dict]) -> None:
            """Genereer productomschrijvingen met Claude op basis van beschikbare data."""
            state_key = "oplossen_omschrijving_resultaat"

            st.info(
                "Geen leverancierstekst gevonden in de database. "
                "Claude genereert een Nederlandse productomschrijving op basis van "
                "productnaam, vendor, designer, kleur en materiaal."
            )

            col_gen, col_dl = st.columns([2, 1])
            with col_dl:
                buf = io.BytesIO()
                pd.DataFrame([{
                    "SKU":     p.get("sku", "—"),
                    "Handle":  p.get("handle", ""),
                    "Product": p.get("product_title", ""),
                    "Vendor":  p.get("vendor", ""),
                } for p in producten]).to_excel(buf, index=False)
                st.download_button(
                    f"📥 Download lijst ({len(producten)})",
                    data=buf.getvalue(),
                    file_name="producten_zonder_omschrijving.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="dl_omschr_lijst",
                )

            with col_gen:
                if st.button(
                    f"✨ Genereer omschrijvingen met Claude ({len(producten)} producten)",
                    key="gen_omschrijving", type="primary",
                ):
                    # Verrijk met products_raw data (designer, kleur, materiaal)
                    sb = _get_sb_new()
                    raw_by_handle: dict[str, dict] = {}
                    if sb:
                        try:
                            handles = [p.get("handle", "") for p in producten]
                            # Haal via shopify_sync de SKUs op, dan products_raw matchen
                            sync_res = sb.table("shopify_sync").select("handle,sku").in_("handle", handles[:200]).execute()
                            handle_to_sku = {r["handle"]: r["sku"] for r in (sync_res.data or []) if r.get("sku")}
                            skus = list(handle_to_sku.values())
                            if skus:
                                raw_res = sb.table("products_raw").select(
                                    "sku,designer,kleur_en,materiaal_raw,leverancier_category"
                                ).in_("sku", skus[:200]).execute()
                                sku_to_raw = {r["sku"]: r for r in (raw_res.data or [])}
                                for handle, sku in handle_to_sku.items():
                                    if sku in sku_to_raw:
                                        raw_by_handle[handle] = sku_to_raw[sku]
                        except Exception:
                            pass

                    OMSCHR_PROMPT = (
                        "Schrijf een Nederlandse productomschrijving voor een designwebshop.\n"
                        "Product: {title}\nMerk: {vendor}\n"
                        "{extra}"
                        "\nRegels:\n"
                        "- 2-3 zinnen, max 300 tekens\n"
                        "- Gebruik 'je'-vorm\n"
                        "- Noem het materiaal en gebruik als die info er is\n"
                        "- Eindig met een korte CTA\n"
                        "- Geen prijzen, geen verzendinfo\n"
                        "Geef ALLEEN de omschrijving terug, geen uitleg."
                    )

                    resultaten = []
                    bar = st.progress(0, text="Bezig...")
                    for i, p in enumerate(producten):
                        handle = p.get("handle", "")
                        raw    = raw_by_handle.get(handle, {})
                        extra_parts = []
                        if raw.get("designer"):
                            extra_parts.append(f"Designer: {raw['designer']}")
                        if raw.get("kleur_en"):
                            extra_parts.append(f"Kleur: {raw['kleur_en']}")
                        if raw.get("materiaal_raw"):
                            extra_parts.append(f"Materiaal: {raw['materiaal_raw']}")
                        if raw.get("leverancier_category"):
                            extra_parts.append(f"Categorie: {raw['leverancier_category']}")
                        extra = ("\n".join(extra_parts) + "\n") if extra_parts else ""

                        try:
                            tekst = _genereer_claude(OMSCHR_PROMPT.format(
                                title=p.get("product_title", ""),
                                vendor=p.get("vendor", ""),
                                extra=extra,
                            ))
                        except Exception as e:
                            tekst = f"[FOUT: {e}]"

                        resultaten.append({**p, "gegenereerd": tekst})
                        bar.progress((i + 1) / len(producten), text=f"{i+1}/{len(producten)} verwerkt")
                    bar.empty()
                    st.session_state[state_key] = resultaten
                    st.rerun()

            if state_key in st.session_state:
                resultaten = st.session_state[state_key]
                st.markdown(f"**Preview — {len(resultaten)} omschrijvingen**")

                preview_rows = [{
                    "SKU":          r.get("sku", "—"),
                    "Product":      r.get("product_title", "")[:50],
                    "Omschrijving": r.get("gegenereerd", ""),
                    "Tekens":       len(r.get("gegenereerd", "")),
                } for r in resultaten]
                st.dataframe(pd.DataFrame(preview_rows), hide_index=True, use_container_width=True)

                col_save, col_exp, col_wis = st.columns([2, 2, 1])
                shop_store = os.getenv("SHOPIFY_STORE", "")

                with col_exp:
                    exp_rows = [{
                        "SKU":            r.get("sku", "—"),
                        "Handle":         r.get("handle", ""),
                        "Product":        r.get("product_title", ""),
                        "Omschrijving":   r.get("gegenereerd", ""),
                        "URL": f"https://{shop_store}/products/{r.get('handle','')}" if shop_store else "",
                    } for r in resultaten]
                    buf2 = io.BytesIO()
                    pd.DataFrame(exp_rows).to_excel(buf2, index=False)
                    st.download_button(
                        "📥 Export naar Excel (Hextom)",
                        data=buf2.getvalue(),
                        file_name="gegenereerde_omschrijvingen.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="exp_omschr",
                    )

                with col_wis:
                    if st.button("🗑 Wis", key="wis_omschr"):
                        del st.session_state[state_key]
                        st.rerun()

        def _render_issue(tekst: str, detail: str, issue_key: str | None, issue_type: str = "meta") -> None:
            with st.expander(f"{tekst.replace('**', '')}"):
                st.caption(f"↳ {detail}")
                if issue_key:
                    if issue_type == "foto" and issue_key == "geen_omschrijving":
                        producten = _foto_detail("geen_omschrijving")
                        _toon_foto_producten("geen_omschrijving")
                        st.markdown("")
                        if st.button(
                            f"🔧 Oplossen — genereer {len(producten)} omschrijvingen met Claude",
                            key="oplossen_btn_omschrijving", type="primary",
                        ):
                            st.session_state["oplossen_open_omschrijving"] = True
                        if st.session_state.get("oplossen_open_omschrijving"):
                            _render_omschrijving_wizard(producten)
                    elif issue_type == "foto":
                        _toon_foto_producten(issue_key)
                    else:
                        producten = _meta_detail(issue_key)
                        _toon_producten(issue_key)
                        if producten and issue_key in ("title_leeg", "desc_leeg", "templated", "title_lang", "desc_buiten"):
                            st.markdown("")
                            if st.button(
                                f"🔧 Oplossen — genereer {len(producten)} {('meta title' if issue_key == 'title_leeg' else 'meta description')}s met Claude",
                                key=f"oplossen_btn_{issue_key}", type="primary",
                            ):
                                st.session_state[f"oplossen_open_{issue_key}"] = True
                            if st.session_state.get(f"oplossen_open_{issue_key}"):
                                _render_oplossen_wizard(issue_key, producten)

        if rood_items:
            st.markdown("##### 🔴 Dit moet nu opgelost worden")
            meta_keys_rood  = [k for k in ("title_leeg", "desc_leeg", "templated") if meta.get(k, 0) > 0]
            foto_keys_rood  = [k for k in ("geen_foto", "geen_alt", "geen_omschrijving") if foto.get(k, 0) > 0]
            all_rood_keys   = [(k, "meta") for k in meta_keys_rood] + [(k, "foto") for k in foto_keys_rood]
            for i, (tekst, detail) in enumerate(rood_items):
                key, ktype = all_rood_keys[i] if i < len(all_rood_keys) else (None, "meta")
                _render_issue(f"🔴 {tekst}", detail, key, ktype)

        if oranje_items:
            st.markdown("##### 🟠 Verbetering gewenst")
            meta_keys_oranje = [k for k in ("title_lang", "desc_buiten") if meta.get(k, 0) > 0]
            foto_keys_oranje = [k for k in ("supplier_naam",) if foto.get(k, 0) > 0]
            sync_oranje      = 1 if not foto.get("gesynchroniseerd") else 0
            all_oranje_keys  = [(k, "meta") for k in meta_keys_oranje] + [(k, "foto") for k in foto_keys_oranje]
            for i, (tekst, detail) in enumerate(oranje_items):
                key, ktype = all_oranje_keys[i] if i < len(all_oranje_keys) else (None, "meta")
                _render_issue(f"🟠 {tekst}", detail, key, ktype)
    else:
        st.success("🟢 Alles ziet er goed uit voor de actieve producten!")

    st.divider()

    # ── Foto-sync ─────────────────────────────────────────────────────────────
    with st.expander("📸 Foto-sync (Shopify → Supabase)"):
        st.caption(
            "Haalt voor alle actieve producten de eerste foto op via Shopify REST API "
            "en controleert: heeft het product een foto, is er alt-tekst, en is de "
            "bestandsnaam SEO-vriendelijk of nog een leveranciersnaam?"
        )
        if not foto.get("gesynchroniseerd"):
            st.warning("Foto-data nog niet gesynchroniseerd. Draai eerst de migratie SQL en klik dan Sync.")
            st.code("execution/shopify_photo_migration.sql  →  Supabase SQL Editor", language=None)
        else:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Zonder foto",           foto.get("geen_foto", 0))
            c2.metric("Zonder alt-tekst",      foto.get("geen_alt", 0))
            c3.metric("Leveranciersnaam",       foto.get("supplier_naam", 0))
            c4.metric("Zonder omschrijving",    foto.get("geen_omschrijving", 0))

        if st.button("📸 Foto-sync starten", key="foto_sync_btn"):
            import subprocess, sys as _sys
            root   = str(Path(__file__).parent.parent)
            script = str(Path(root) / "execution" / "shopify_photo_sync.py")
            with st.spinner("Foto-sync bezig (~1 minuut) ..."):
                result = subprocess.run(
                    [_sys.executable, script],
                    capture_output=True, text=True, cwd=root, timeout=300, encoding="utf-8",
                )
            if result.returncode == 0:
                st.success("✅ Foto-sync klaar!")
                st.cache_data.clear()
                st.rerun()
            else:
                st.error("❌ Fout — zie log hieronder")
            with st.expander("Log"):
                st.code(result.stdout + result.stderr or "(geen output)")

    st.divider()

    # ── Hextom wachtrij (detail) ──────────────────────────────────────────────
    st.markdown("### 📋 Hextom wachtrij")
    if not pending:
        st.success("✅ Geen openstaande exports.")
    else:
        for rec in pending:
            ts   = str(rec.get("generated_at", ""))[:16]
            fn   = rec.get("file_name", "?")
            task = rec.get("task_type", "?")
            rows = rec.get("row_count", 0)
            col_info, col_btn = st.columns([5, 1])
            with col_info:
                st.markdown(f"📄 **{fn}**")
                st.caption(f"{task} · {rows} rijen · gegenereerd {ts}")
            with col_btn:
                if st.button("✅ Bevestig", key=f"st_confirm_{rec.get('id')}"):
                    confirm_applied(sb, rec["id"], client_id)
                    st.rerun()

    st.divider()

    # ── Categorie gaps (ingeklapt) ────────────────────────────────────────────
    with st.expander("🏷️ Categorie gaps (actieve producten → Shopify collecties)"):
        st.caption(
            "Haalt cat_-tags op van alle **actieve** producten en vergelijkt die met "
            "live Shopify collecties. Categorieën zonder collectiepagina worden getoond."
        )
        if st.button("🔍 Analyseer categorie gaps", key="st_cat"):
            with st.spinner("Ophalen Shopify collecties + actieve product-tags..."):
                result = _categorie_gaps()
            if result.get("error"):
                st.error(f"Fout: {result['error']}")
            elif not result.get("gaps"):
                n_cats = len(result.get("cat_tags_used", []))
                n_cols = len(result.get("shopify_titels", []))
                st.success(f"✅ Alle {n_cats} categorieën hebben een collectiepagina in Shopify ({n_cols} collecties gevonden).")
            else:
                gaps = result["gaps"]
                st.warning(f"**{len(gaps)} categorieën** hebben nog geen collectiepagina in Shopify:")
                df_gap = pd.DataFrame({"cat_tag": gaps})
                st.dataframe(df_gap, hide_index=True, use_container_width=True)
                st.caption(
                    f"Totaal {len(result.get('cat_tags_used', []))} unieke cat_-tags in actieve producten · "
                    f"{len(result.get('shopify_titels', []))} Shopify collecties gevonden"
                )

    # ── Export geschiedenis (ingeklapt) ───────────────────────────────────────
    with st.expander("📜 Export geschiedenis (laatste 30)"):
        history = get_history(sb, client_id, limit=30)
        if not history:
            st.info("Nog geen exports gelogd.")
        else:
            df_h = pd.DataFrame(history)
            df_h["bevestigd"] = df_h["confirmed_at"].apply(lambda v: "✅ ja" if v else "⏳ nee")
            cols = [c for c in ("task_type", "file_name", "row_count", "generated_at", "bevestigd") if c in df_h.columns]
            st.dataframe(df_h[cols], hide_index=True, use_container_width=True)
