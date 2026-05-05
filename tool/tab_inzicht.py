"""Tab 5 — Inzicht.

Drie secties:
  1. Snel opzoeken  — klant zegt "product X staat verkeerd" → direct opzoeken
  2. Producten online — gefilterde tabel van actieve/draft/gearchiveerde producten
  3. Collecties       — overzicht van collectiepagina's met SEO-status

Data komt uit shopify_meta_audit (old Supabase) + optioneel products_curated /
shopify_sync (new Supabase als SUPABASE_NEW_URL in .env staat).

Kolom-mapping shopify_meta_audit:
  shopify_product_id, handle, product_title, vendor, product_type,
  product_status, price, tags, published_at,
  current_meta_title, current_meta_description,
  current_title_length, current_desc_length,
  title_status, desc_status, review_status,
  approved_title, approved_desc, pushed_at
"""
from __future__ import annotations

import os
import subprocess
import sys
import threading

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from ui.supabase_client import get_supabase
from client import get_client_id

load_dotenv()

TITLE_MAX = 58
DESC_MIN  = 120
DESC_MAX  = 155

STATUS_ICON = {
    "active":   "🟢",
    "draft":    "🟡",
    "archived": "🔴",
}


# ── Extra Supabase (nieuw schema) ─────────────────────────────────────────────

@st.cache_resource
def _get_sb_new():
    from supabase import create_client
    url = os.getenv("SUPABASE_NEW_URL", "")
    key = os.getenv("SUPABASE_NEW_SERVICE_KEY", "") or os.getenv("SUPABASE_NEW_KEY", "")
    if not url or not key:
        return None
    return create_client(url, key)


# ── SEO helpers ───────────────────────────────────────────────────────────────

def _seo_score(title: str | None, desc: str | None) -> str:
    t = len(str(title or "").strip())
    d = len(str(desc or "").strip())
    if t == 0 and d == 0:
        return "🔴 Leeg"
    if 0 < t <= TITLE_MAX and DESC_MIN <= d <= DESC_MAX:
        return "🟢 OK"
    return "🟠 Controleer"


def _seo_issues(title: str | None, desc: str | None) -> list[str]:
    issues = []
    t = len(str(title or "").strip())
    d = len(str(desc or "").strip())
    if t == 0:
        issues.append("Meta title ontbreekt")
    elif t > TITLE_MAX:
        issues.append(f"Meta title te lang ({t} tekens, max {TITLE_MAX})")
    if d == 0:
        issues.append("Meta description ontbreekt")
    elif not (DESC_MIN <= d <= DESC_MAX):
        issues.append(f"Meta description buiten bereik ({d} tekens, moet {DESC_MIN}–{DESC_MAX})")
    return issues


# ── Data-fetchers ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def _load_meta_audit() -> list[dict]:
    try:
        sb = _get_sb_new()
        if not sb:
            st.warning("SUPABASE_NEW_URL ontbreekt in .env")
            return []
        res = (
            sb.table("shopify_meta_audit")
            .select(
                "shopify_product_id,handle,product_title,vendor,product_type,"
                "product_status,price,tags,published_at,"
                "current_meta_title,current_meta_description,"
                "current_title_length,current_desc_length,"
                "title_status,desc_status,has_image,has_description"
            )
            .execute()
        )
        return res.data or []
    except Exception as e:
        st.warning(f"shopify_meta_audit niet bereikbaar: {e}")
        return []


@st.cache_data(ttl=600, show_spinner=False)
def _load_collections() -> list[dict]:
    """Haalt collecties op via Shopify REST API."""
    try:
        import importlib.util
        p = Path(__file__).parent.parent / "execution" / "shopify_read.py"
        spec = importlib.util.spec_from_file_location("shopify_read_mod", p)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        token = mod.get_token()
        df = mod.haal_collecties(token)
        return df.to_dict("records") if not df.empty else []
    except Exception as e:
        st.warning(f"Shopify collecties niet bereikbaar: {e}")
        return []


# ── Zoek-helpers (nieuw schema) ───────────────────────────────────────────────

def _zoek_curated(term: str) -> list[dict]:
    sb = _get_sb_new()
    if not sb:
        return []
    try:
        r = sb.table("products_curated").select("*").eq("sku", term).execute().data or []
        if r:
            return r
        return sb.table("products_curated").select("*").ilike("handle", f"%{term}%").limit(5).execute().data or []
    except Exception:
        return []


def _zoek_raw(term: str) -> list[dict]:
    sb = _get_sb_new()
    if not sb:
        return []
    try:
        r = sb.table("products_raw").select("*").eq("sku", term).execute().data or []
        if r:
            return r
        return sb.table("products_raw").select("*").eq("ean_piece", term).execute().data or []
    except Exception:
        return []


def _zoek_shopify_sync(term: str) -> list[dict]:
    sb = _get_sb_new()
    if not sb:
        return []
    try:
        r = sb.table("shopify_sync").select("*").eq("sku", term).execute().data or []
        if r:
            return r
        return sb.table("shopify_sync").select("*").ilike("handle", f"%{term}%").limit(5).execute().data or []
    except Exception:
        return []


def _zoek_audit(term: str) -> list[dict]:
    """Zoek in shopify_meta_audit op handle of product_title."""
    try:
        sb = _get_sb_new()
        if not sb:
            return []
        r = sb.table("shopify_meta_audit").select("*").ilike("handle", f"%{term}%").limit(5).execute().data or []
        if r:
            return r
        return sb.table("shopify_meta_audit").select("*").ilike("product_title", f"%{term}%").limit(5).execute().data or []
    except Exception:
        return []


# ── Product-kaart ─────────────────────────────────────────────────────────────

def _product_card(curated: dict, raw: dict, sync: dict, audit: dict) -> None:
    title_live    = sync.get("title") or audit.get("product_title") or curated.get("product_title_nl") or "—"
    title_curated = curated.get("product_title_nl") or "—"
    handle        = sync.get("handle") or audit.get("handle") or curated.get("handle") or "—"
    status        = sync.get("shopify_status") or audit.get("product_status") or "onbekend"
    price_live    = sync.get("price") or audit.get("price") or curated.get("verkoopprijs") or "—"
    meta_title    = audit.get("current_meta_title") or ""
    meta_desc     = audit.get("current_meta_description") or curated.get("meta_description") or ""
    sku           = curated.get("sku") or raw.get("sku") or sync.get("sku") or "—"
    vendor        = audit.get("vendor") or raw.get("supplier") or curated.get("supplier") or "—"
    hoofdcat      = curated.get("hoofdcategorie") or "—"
    subcat        = curated.get("subcategorie") or "—"
    sub_sub       = curated.get("sub_subcategorie") or "—"

    status_icon = STATUS_ICON.get(status, "❓")
    seo_score   = _seo_score(meta_title, meta_desc)
    issues      = _seo_issues(meta_title, meta_desc)

    c1, c2, c3 = st.columns([3, 2, 1])
    with c1:
        st.markdown(f"### {title_live}")
        if title_curated != title_live and title_curated != "—":
            st.caption(f"Curated titel: {title_curated}")
    with c2:
        st.markdown(f"**Handle:** `{handle}`")
        st.markdown(f"**SKU:** `{sku}`")
        st.markdown(f"**Vendor:** {vendor}")
    with c3:
        st.metric("Status", f"{status_icon} {status}")
        st.metric("Prijs", f"€ {price_live}" if price_live != "—" else "—")

    st.divider()
    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown("**Categorie**")
        st.write(f"Hoofdcat: {hoofdcat}")
        st.write(f"Subcat: {subcat}")
        st.write(f"Sub-sub: {sub_sub}")
        if raw:
            designer = raw.get("designer") or "—"
            kleur    = raw.get("kleur_en") or "—"
            materia  = raw.get("materiaal_raw") or "—"
            st.caption(f"Designer: {designer}  ·  Kleur: {kleur}  ·  Materiaal: {materia}")

    with col_right:
        st.markdown(f"**SEO kwaliteit:** {seo_score}")
        if issues:
            for iss in issues:
                st.warning(iss)
        else:
            st.success("Meta title & description zijn in orde.")

        t_len = len(str(meta_title or "").strip())
        d_len = len(str(meta_desc or "").strip())
        st.caption(f"Meta title ({t_len}/{TITLE_MAX} tekens)")
        if meta_title:
            st.code(meta_title, language=None)
        else:
            st.caption("_leeg_")
        st.caption(f"Meta description ({d_len} tekens, bereik {DESC_MIN}–{DESC_MAX})")
        if meta_desc:
            st.code(meta_desc, language=None)
        else:
            st.caption("_leeg_")

    shop_store = os.getenv("SHOPIFY_STORE", "")
    if handle != "—" and shop_store:
        st.markdown(f"[🔗 Bekijk in webshop](https://{shop_store}/products/{handle})")


# ── Sync banner ───────────────────────────────────────────────────────────────

def _render_sync_banner() -> None:
    """Sync-knop + instructie voor de migration SQL."""
    with st.expander("🔄 Shopify → Supabase sync", expanded=False):
        st.markdown(
            "**Stap 1 (eenmalig):** Voer `execution/shopify_meta_sync_migration.sql` uit in "
            "de Supabase SQL Editor om de extra kolommen aan te maken."
        )
        st.markdown(
            "**Stap 2:** Klik hieronder om alle actieve Shopify-producten naar "
            "`shopify_meta_audit` te syncen (SEO title, description, prijs, tags, status)."
        )
        st.caption(
            "Gebruikt Shopify GraphQL Bulk Operations — verwerkt ~2000 producten in "
            "één API-call. Duurt ±1–3 minuten."
        )

        col_btn, col_status = st.columns([2, 5])
        with col_btn:
            run_sync = st.button("▶ Sync starten", key="inz_sync_run", type="primary")

        if run_sync:
            root = str(Path(__file__).resolve().parent.parent)
            script = str(Path(root) / "execution" / "shopify_meta_sync.py")

            with st.spinner("Sync bezig (dit duurt ±1–3 min) ..."):
                try:
                    result = subprocess.run(
                        [sys.executable, script],
                        capture_output=True,
                        text=True,
                        cwd=root,
                        timeout=300,
                        encoding="utf-8",
                    )
                    output = result.stdout + result.stderr
                    if result.returncode == 0:
                        st.success("✅ Sync klaar! Ververs de pagina om de nieuwe data te zien.")
                        st.cache_data.clear()
                    else:
                        st.error("❌ Sync mislukt — zie log hieronder.")
                    with st.expander("Log"):
                        st.code(output or "(geen output)")
                except subprocess.TimeoutExpired:
                    st.error("⏱ Timeout — script draaide langer dan 5 minuten.")
                except Exception as exc:
                    st.error(f"❌ {exc}")


# ── Pad-import voor subprocess ────────────────────────────────────────────────
from pathlib import Path


# ── Hoofdrender ───────────────────────────────────────────────────────────────

def render() -> None:
    st.subheader("🔎 Inzicht")
    st.caption("Snel opzoeken, producten online en collectiepagina's in één overzicht.")

    _render_sync_banner()
    st.divider()

    tab_zoek, tab_producten, tab_collecties = st.tabs([
        "🔍 Snel opzoeken",
        "📦 Producten online",
        "🌐 Collecties",
    ])

    # ── TAB 1: Snel opzoeken ──────────────────────────────────────────────────
    with tab_zoek:
        st.markdown(
            "Typ een **SKU**, **EAN**, deel van de productnaam of handle. "
            "Toont alles wat we weten over dat product."
        )

        col_inp, col_btn = st.columns([6, 1])
        with col_inp:
            zoekterm = st.text_input(
                "Zoeken",
                placeholder="bijv.  B0126008-008  ·  5400959163491  ·  vaas-serax",
                key="inz_zoek",
                label_visibility="collapsed",
            ).strip()
        with col_btn:
            st.caption("&nbsp;")
            if st.button("✖", key="inz_wis", help="Wis zoekterm"):
                st.session_state["inz_zoek"] = ""
                st.rerun()

        if not zoekterm:
            st.info("Voer een zoekterm in.")
        else:
            with st.spinner(f"Zoeken naar **{zoekterm}**..."):
                curated_list = _zoek_curated(zoekterm)
                raw_list     = _zoek_raw(zoekterm)
                sync_list    = _zoek_shopify_sync(zoekterm)
                audit_list   = _zoek_audit(zoekterm)

                # Als audit leeg is maar we hebben wel een handle via curated/sync,
                # doe een gerichte handle-lookup in shopify_meta_audit.
                if not audit_list:
                    handle_fallback = (
                        (sync_list[0].get("handle") if sync_list else None)
                        or (curated_list[0].get("handle") if curated_list else None)
                    )
                    if handle_fallback:
                        audit_list = _zoek_audit(handle_fallback)

            curated = curated_list[0] if curated_list else {}
            raw     = raw_list[0]     if raw_list     else {}
            sync    = sync_list[0]    if sync_list    else {}
            audit   = audit_list[0]   if audit_list   else {}

            if not any([curated, raw, sync, audit]):
                st.error(f"Niets gevonden voor **{zoekterm}**.")
            else:
                gevonden_in = [
                    n for n, d in [("curated", curated), ("raw", raw),
                                   ("shopify_sync", sync), ("meta_audit", audit)] if d
                ]
                st.success(f"Gevonden in: {', '.join(gevonden_in)}")
                _product_card(curated, raw, sync, audit)

                all_hits = max(len(curated_list), len(sync_list), len(audit_list))
                if all_hits > 1:
                    with st.expander(f"Meer resultaten ({all_hits} gevonden)"):
                        combined = {}
                        for rec in curated_list + sync_list + audit_list:
                            if rec:
                                key = rec.get("sku") or rec.get("handle") or ""
                                combined[key] = rec
                        for key_val, rec in list(combined.items())[:10]:
                            lbl = (rec.get("product_title_nl") or rec.get("product_title")
                                   or rec.get("title") or rec.get("handle") or key_val or "?")
                            st.caption(f"• {lbl}  —  {key_val}")

    # ── TAB 2: Producten online ───────────────────────────────────────────────
    with tab_producten:
        col_r, _ = st.columns([1, 5])
        with col_r:
            if st.button("🔄 Ververs", key="inz_p_refresh"):
                st.cache_data.clear()
                st.rerun()

        with st.spinner("Producten laden..."):
            audit_data = _load_meta_audit()

        if not audit_data:
            st.warning(
                "Geen data in `shopify_meta_audit`. "
                "Klik hierboven op **Sync starten** om Shopify te syncen."
            )
        else:
            df_raw = pd.DataFrame(audit_data)

            # ── Filters ───────────────────────────────────────────────────────
            f1, f2, f3, f4 = st.columns([2, 2, 2, 2])
            with f1:
                status_opts = ["Alle"] + sorted(
                    df_raw["product_status"].dropna().unique().tolist()
                ) if "product_status" in df_raw.columns else ["Alle"]
                status_filter = st.selectbox("Status", status_opts, key="inz_p_status")
            with f2:
                vendor_opts = ["Alle"] + sorted(
                    df_raw["vendor"].dropna().unique().tolist()
                ) if "vendor" in df_raw.columns else ["Alle"]
                vendor_filter = st.selectbox("Vendor", vendor_opts, key="inz_p_vendor")
            with f3:
                seo_filter = st.selectbox(
                    "SEO score", ["Alle", "🟢 OK", "🟠 Controleer", "🔴 Leeg"],
                    key="inz_p_seo"
                )
            with f4:
                title_filter = st.selectbox(
                    "Title status",
                    ["Alle", "missing", "too_long", "too_short", "ok"],
                    key="inz_p_title"
                )

            # ── Bouw weergave-df ───────────────────────────────────────────────
            shop_store = os.getenv("SHOPIFY_STORE", "")
            rows = []
            for r in audit_data:
                status  = (r.get("product_status") or "onbekend").lower()
                vendor  = r.get("vendor") or "—"
                title   = r.get("product_title") or "—"
                handle  = r.get("handle") or "—"
                mt      = r.get("current_meta_title") or ""
                md      = r.get("current_meta_description") or ""
                price   = r.get("price")
                t_stat  = r.get("title_status") or "—"
                seo     = _seo_score(mt, md)
                t_len   = len(str(mt).strip())
                d_len   = len(str(md).strip())
                url     = f"https://{shop_store}/products/{handle}" if shop_store and handle != "—" else ""

                if status_filter != "Alle" and status != status_filter:
                    continue
                if vendor_filter != "Alle" and vendor != vendor_filter:
                    continue
                if seo_filter != "Alle" and seo != seo_filter:
                    continue
                if title_filter != "Alle" and t_stat != title_filter:
                    continue

                rows.append({
                    "Status":      f"{STATUS_ICON.get(status, '❓')} {status}",
                    "Titel":       title,
                    "Vendor":      vendor,
                    "Prijs":       f"€ {price:.2f}" if isinstance(price, (int, float)) else "—",
                    "SEO":         seo,
                    "Title len":   t_len,
                    "Desc len":    d_len,
                    "Title status": t_stat,
                    "URL":         url,
                })

            view_df = pd.DataFrame(rows)
            st.caption(f"{len(view_df)} producten weergegeven (van {len(audit_data)} totaal)")

            if not view_df.empty:
                col_cfg = {
                    "Status":       st.column_config.TextColumn("Status",        width="small"),
                    "Titel":        st.column_config.TextColumn("Titel",         width="large"),
                    "Vendor":       st.column_config.TextColumn("Vendor",        width="small"),
                    "Prijs":        st.column_config.TextColumn("Prijs",         width="small"),
                    "SEO":          st.column_config.TextColumn("SEO",           width="small"),
                    "Title len":    st.column_config.NumberColumn("Title len",   width="small",
                                        help=f"Tekens meta title (max {TITLE_MAX})"),
                    "Desc len":     st.column_config.NumberColumn("Desc len",    width="small",
                                        help=f"Tekens meta desc ({DESC_MIN}–{DESC_MAX})"),
                    "Title status": st.column_config.TextColumn("Title status",  width="small"),
                }
                if shop_store:
                    col_cfg["URL"] = st.column_config.LinkColumn("Webshop", width="small",
                                         display_text="🔗")
                st.dataframe(view_df, use_container_width=True, hide_index=True,
                             column_config=col_cfg)
            else:
                st.info("Geen producten voldoen aan de filters.")

    # ── TAB 3: Collecties ─────────────────────────────────────────────────────
    with tab_collecties:
        col_r2, _ = st.columns([1, 5])
        with col_r2:
            if st.button("🔄 Ververs", key="inz_c_refresh"):
                st.cache_data.clear()
                st.rerun()

        with st.spinner("Collecties laden..."):
            cols_data = _load_collections()

        if not cols_data:
            st.warning(
                "Geen data in `seo_website_collections`. "
                "Laad via Setup → Website-structuur in dashboard_v2."
            )
        else:
            shop_store = os.getenv("SHOPIFY_STORE", "")
            online_filter = st.selectbox("Filter", ["Alle", "🟢 Online", "🔴 Offline"], key="inz_c_filter")
            if online_filter == "🟢 Online":
                rows_c = [r for r in rows_c if r["Online"] == "🟢"]
            elif online_filter == "🔴 Offline":
                rows_c = [r for r in rows_c if r["Online"] == "🔴"]

            rows_c = []
            for c in cols_data:
                titel      = c.get("titel") or c.get("title") or "—"
                handle     = c.get("handle") or "—"
                gepubl     = c.get("gepubliceerd") or ("ja" if c.get("published_at") else "nee")
                online_icon = "🟢" if gepubl == "ja" else "🔴"
                url        = f"https://{shop_store}/collections/{handle}" if shop_store and handle != "—" else ""

                rows_c.append({
                    "Online":    online_icon,
                    "Collectie": titel,
                    "Handle":    handle,
                    "URL":       url,
                })

            view_c = pd.DataFrame(rows_c)
            st.caption(f"{len(view_c)} collecties opgehaald via Shopify API")

            col_cfg_c = {
                "Online":     st.column_config.TextColumn("",           width="small"),
                "Collectie":  st.column_config.TextColumn("Collectie",  width="large"),
                "Handle":     st.column_config.TextColumn("Handle",     width="medium"),
            }
            if shop_store:
                col_cfg_c["URL"] = st.column_config.LinkColumn("Webshop", width="small",
                                       display_text="🔗")
            st.dataframe(view_c, use_container_width=True, hide_index=True,
                         column_config=col_cfg_c)
            st.caption("SEO meta title/description van collecties is beschikbaar via Tab 🌐 Collectie SEO.")
