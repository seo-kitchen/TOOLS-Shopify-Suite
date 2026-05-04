"""Shopify index — read-only view van seo_shopify_index."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from ui.layout import page_header
from ui.supabase_client import get_supabase


page_header("🛍️ Shopify index", subtitle="De Shopify snapshot waar match.py tegenaan matcht.")


with st.sidebar:
    st.markdown("### Filters")
    status = st.multiselect("Status", ["actief", "archief"], default=["actief"], key="sx_status")
    vendor = st.multiselect(
        "Vendor / Merk",
        ["Serax", "Pottery Pots", "Printworks", "S&P/Bonbistro"],
        key="sx_merk",
    )
    zoek = st.text_input("Zoek (SKU / EAN / titel / handle)", key="sx_search")
    limit = st.selectbox("Max rijen", [100, 500, 1000, 5000], index=1, key="sx_limit")


@st.cache_data(ttl=60, show_spinner=False)
def _fetch(status_t, vendor_t, zoek, limit):
    sb = get_supabase()
    q = sb.table("seo_shopify_index").select("*").limit(limit)
    if status_t:
        q = q.in_("status", list(status_t))
    if vendor_t:
        q = q.in_("vendor", list(vendor_t))
    if zoek:
        term = zoek.strip()
        q = q.or_(
            f"sku.ilike.%{term}%,ean.ilike.%{term}%,title.ilike.%{term}%,handle.ilike.%{term}%"
        )
    return q.execute().data or []


try:
    rows = _fetch(tuple(status), tuple(vendor), zoek, limit)
except Exception as e:
    st.error(f"❌ {e}")
    st.stop()

if not rows:
    st.info("Geen producten gevonden in de Shopify-index met deze filters.")
    st.stop()

df = pd.DataFrame(rows)
st.caption(f"{len(df)} rijen (max {limit})")
st.dataframe(df, hide_index=True, width="stretch")
