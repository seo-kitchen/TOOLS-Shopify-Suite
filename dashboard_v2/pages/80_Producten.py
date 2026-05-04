"""Producten overzicht met krachtige filters + search + sortering.

Leest ``seo_products`` met server-side filtering en paginatie (per 100 rijen)
om ook bij 7k+ producten snel te blijven.
"""
from __future__ import annotations

import io

import pandas as pd
import streamlit as st

from ui.layout import page_header
from ui.supabase_client import get_supabase


page_header(
    "📦 Producten",
    subtitle="Read-only tabel over `seo_products` met filters, search, paginatie en bulk-acties.",
)


DEFAULT_COLS = [
    "id", "sku", "ean_shopify", "product_name_raw", "product_title_nl",
    "merk", "fase", "status", "status_shopify",
    "hoofdcategorie", "sub_subcategorie",
    "rrp_stuk_eur", "rrp_gb_eur",
]
ALL_COLS = DEFAULT_COLS + [
    "ean_piece", "handle", "tags", "meta_description",
    "subcategorie", "kleur_en", "kleur_nl", "materiaal_nl",
    "lengte_cm", "breedte_cm", "hoogte_cm",
    "match_methode", "match_zekerheid", "review_reden",
    "shopify_product_id", "shopify_variant_id",
    "designer", "giftbox", "giftbox_qty",
    "inkoopprijs_stuk_eur", "inkoopprijs_gb_eur",
    "photo_packshot_1", "photo_lifestyle_1",
    "created_at", "updated_at",
]


# ── Sidebar filters ──────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### 🔎 Filters")

    fase = st.selectbox("Fase", ["alle", "1", "2", "3", "4", "5", "6"], key="pr_fase")
    merken = st.multiselect(
        "Merk",
        ["Serax", "Pottery Pots", "Printworks", "S&P/Bonbistro"],
        key="pr_merk",
    )
    status = st.multiselect(
        "Status pipeline",
        ["raw", "matched", "ready", "review", "exported"],
        key="pr_status",
    )
    status_shopify = st.multiselect(
        "Status Shopify",
        ["actief", "archief", "nieuw", "onbekend"],
        key="pr_sx",
    )
    hoofdcat = st.text_input("Hoofdcategorie bevat", key="pr_hc")
    subsub = st.text_input("Sub-subcategorie bevat", key="pr_ssc")

    heeft_foto = st.selectbox("Heeft foto's?", ["alle", "ja", "nee"], key="pr_foto")
    heeft_meta = st.selectbox("Heeft meta-description?", ["alle", "ja", "nee"], key="pr_meta")

    st.markdown("#### Prijs (EUR)")
    prijs_min, prijs_max = st.slider("Prijsrange", 0, 2000, (0, 2000), 5, key="pr_prijs")

    zoek = st.text_input(
        "Zoek (SKU / EAN / naam / titel / handle)",
        key="pr_zoek",
        placeholder="bv. B4020040 of 'deep plate'",
    )

    st.markdown("#### Weergave")
    cols = st.multiselect(
        "Kolommen",
        ALL_COLS,
        default=DEFAULT_COLS,
        key="pr_cols",
    )
    per_page = st.selectbox("Rijen per pagina", [50, 100, 200, 500], index=1, key="pr_per_page")
    sort_col = st.selectbox(
        "Sorteer op",
        ["updated_at", "created_at", "sku", "product_title_nl", "rrp_stuk_eur", "fase"],
        key="pr_sort",
    )
    sort_desc = st.toggle("Aflopend", value=True, key="pr_sort_desc")


# ── Query builder ────────────────────────────────────────────────────────────

@st.cache_data(ttl=30, show_spinner=False)
def _fetch(filter_key: str, offset: int, limit: int, select_cols: list[str],
           sort_col: str, sort_desc: bool, _params: dict) -> tuple[list[dict], int]:
    """filter_key + _params zorgen dat cache invalidateert bij andere filters."""
    sb = get_supabase()

    q = sb.table("seo_products").select(",".join(select_cols), count="exact")

    if _params["fase"] and _params["fase"] != "alle":
        q = q.eq("fase", _params["fase"])
    if _params["merken"]:
        q = q.in_("merk", _params["merken"])
    if _params["status"]:
        q = q.in_("status", _params["status"])
    if _params["status_shopify"]:
        q = q.in_("status_shopify", _params["status_shopify"])
    if _params["hoofdcat"]:
        q = q.ilike("hoofdcategorie", f"%{_params['hoofdcat']}%")
    if _params["subsub"]:
        q = q.ilike("sub_subcategorie", f"%{_params['subsub']}%")
    if _params["zoek"]:
        term = _params["zoek"].strip()
        q = q.or_(
            f"sku.ilike.%{term}%,"
            f"ean_shopify.ilike.%{term}%,"
            f"product_name_raw.ilike.%{term}%,"
            f"product_title_nl.ilike.%{term}%,"
            f"handle.ilike.%{term}%"
        )
    if _params["heeft_foto"] == "ja":
        q = q.not_.is_("photo_packshot_1", "null").neq("photo_packshot_1", "")
    elif _params["heeft_foto"] == "nee":
        q = q.or_("photo_packshot_1.is.null,photo_packshot_1.eq.")
    if _params["heeft_meta"] == "ja":
        q = q.not_.is_("meta_description", "null").neq("meta_description", "")
    elif _params["heeft_meta"] == "nee":
        q = q.or_("meta_description.is.null,meta_description.eq.")
    if _params["prijs_min"] > 0 or _params["prijs_max"] < 2000:
        q = q.gte("rrp_stuk_eur", _params["prijs_min"]).lte("rrp_stuk_eur", _params["prijs_max"])

    q = q.order(sort_col, desc=sort_desc).range(offset, offset + limit - 1)
    res = q.execute()
    return (res.data or [], res.count or 0)


# ── Fetch & render ───────────────────────────────────────────────────────────

page_num = st.session_state.get("pr_page", 0)
select_cols = list(dict.fromkeys(["id"] + cols))

params = dict(
    fase=fase, merken=merken, status=status, status_shopify=status_shopify,
    hoofdcat=hoofdcat, subsub=subsub, zoek=zoek,
    heeft_foto=heeft_foto, heeft_meta=heeft_meta,
    prijs_min=prijs_min, prijs_max=prijs_max,
)
filter_key = str(sorted(params.items())) + str(cols) + sort_col + str(sort_desc)

try:
    rows, total = _fetch(filter_key, page_num * per_page, per_page, select_cols,
                          sort_col, sort_desc, params)
except Exception as e:
    st.error(f"❌ Fout bij ophalen producten: {e}")
    st.caption("Check of `seo_products` bestaat en of de SUPABASE_KEY in `.env` staat.")
    st.stop()


# KPI row
c1, c2, c3, c4 = st.columns(4)
c1.metric("Gevonden", f"{total:,}")
c2.metric("Pagina", f"{page_num + 1} / {max(1, (total + per_page - 1) // per_page)}")
c3.metric("Kolommen", len(cols))
c4.metric("Weergegeven", len(rows))

# Actie-knoppen
act1, act2, act3, act4 = st.columns([1, 1, 1, 3])
if act1.button("⬅️ Vorige", disabled=page_num == 0, width="stretch"):
    st.session_state["pr_page"] = max(0, page_num - 1)
    st.rerun()
if act2.button("Volgende ➡️", disabled=(page_num + 1) * per_page >= total, width="stretch"):
    st.session_state["pr_page"] = page_num + 1
    st.rerun()
if act3.button("🔄 Refresh", width="stretch"):
    _fetch.clear()
    st.rerun()

# CSV export
if rows:
    df_csv = pd.DataFrame(rows)
    buf = io.BytesIO()
    df_csv.to_excel(buf, index=False, engine="openpyxl")
    act4.download_button(
        "📥 Download huidige pagina (xlsx)",
        data=buf.getvalue(),
        file_name=f"producten_pagina_{page_num + 1}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        width="stretch",
    )


# ── Tabel met selectie ───────────────────────────────────────────────────────

if not rows:
    st.info("Geen producten gevonden met deze filters.")
    st.stop()

df = pd.DataFrame(rows)
for col in cols:
    if col not in df.columns:
        df[col] = None
df = df[["id"] + cols]

selected = st.data_editor(
    df.assign(_select=False),
    column_config={
        "_select": st.column_config.CheckboxColumn("✔", default=False, width="small"),
        "id": st.column_config.NumberColumn("ID", disabled=True),
    },
    column_order=["_select"] + ["id"] + cols,
    hide_index=True,
    disabled=["id"] + cols,
    width="stretch",
    key=f"pr_editor_{page_num}",
)

selected_ids = selected.loc[selected["_select"], "id"].tolist()

st.divider()

# ── Row-detail expander ──────────────────────────────────────────────────────

if selected_ids:
    st.markdown(f"### 🔍 Details — {len(selected_ids)} geselecteerd")
    with st.expander("Toon eerste 5 volledige rijen"):
        sb = get_supabase()
        full = (
            sb.table("seo_products")
            .select("*")
            .in_("id", selected_ids[:5])
            .execute()
            .data
            or []
        )
        for record in full:
            st.markdown(f"**#{record['id']} · {record.get('sku') or '(zonder SKU)'}**")
            st.json(record, expanded=False)


# ── Bulk-acties ──────────────────────────────────────────────────────────────

st.markdown("### ⚡ Bulk-acties")
if not selected_ids:
    st.caption("_Selecteer eerst rijen via de checkboxes hierboven._")
else:
    b1, b2, b3 = st.columns(3)

    if b1.button(
        f"➡️ Stuur {len(selected_ids)} rijen naar Transform (cap 25)",
        disabled=len(selected_ids) > 25,
        width="stretch",
    ):
        st.session_state["selected_ids"] = selected_ids
        st.session_state["transform_from_producten"] = True
        st.switch_page("pages/30_Transform.py")

    if b2.button(f"🚩 Markeer {len(selected_ids)} voor review", width="stretch"):
        try:
            sb = get_supabase()
            sb.table("seo_products").update({"status": "review"}).in_("id", selected_ids).execute()
            _fetch.clear()
            st.success(f"{len(selected_ids)} rijen op 'review' gezet.")
            st.rerun()
        except Exception as e:
            st.error(f"Fout: {e}")

    if b3.button(f"📥 Download selectie ({len(selected_ids)}) als xlsx", width="stretch"):
        try:
            sb = get_supabase()
            full = sb.table("seo_products").select("*").in_("id", selected_ids).execute().data or []
            buf = io.BytesIO()
            pd.DataFrame(full).to_excel(buf, index=False, engine="openpyxl")
            st.download_button(
                "💾 Klik om te downloaden",
                data=buf.getvalue(),
                file_name=f"selectie_{len(selected_ids)}_producten.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        except Exception as e:
            st.error(f"Fout: {e}")
