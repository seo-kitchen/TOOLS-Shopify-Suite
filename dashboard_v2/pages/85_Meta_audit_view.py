"""Meta audit overzicht — shopify_meta_audit met length flags."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from ui.layout import page_header
from ui.supabase_client import get_supabase


page_header("🔎 Meta audit — overzicht")


META_TITLE_MAX = 60
META_DESC_MAX = 155


with st.sidebar:
    st.markdown("### Filters")
    vendor = st.multiselect("Vendor", ["Serax", "Pottery Pots", "Printworks", "S&P/Bonbistro"], key="ma_vendor")
    flag_title_long = st.checkbox(f"Meta title > {META_TITLE_MAX}", key="ma_tl")
    flag_desc_long = st.checkbox(f"Meta desc > {META_DESC_MAX}", key="ma_dl")
    flag_title_empty = st.checkbox("Meta title leeg", key="ma_te")
    flag_desc_empty = st.checkbox("Meta desc leeg", key="ma_de")
    zoek = st.text_input("Zoek (handle / title)", key="ma_search")


@st.cache_data(ttl=60, show_spinner=False)
def _fetch():
    sb = get_supabase()
    return sb.table("shopify_meta_audit").select("*").execute().data or []


try:
    rows = _fetch()
except Exception as e:
    st.error(f"❌ Kon shopify_meta_audit niet lezen: {e}")
    st.caption("Laad via Setup → Meta audit loader.")
    st.stop()

if not rows:
    st.info("Geen meta-audit data. Laad via Setup → Meta audit loader.")
    st.stop()

df = pd.DataFrame(rows)

# Zoek + filters
if vendor and "vendor" in df.columns:
    df = df[df["vendor"].isin(vendor)]
if zoek:
    mask = df.apply(lambda r: any(zoek.lower() in str(v).lower() for v in r.values), axis=1)
    df = df[mask]

mt_col = next((c for c in ("meta_title", "metatitle") if c in df.columns), None)
md_col = next((c for c in ("meta_description", "metadescription") if c in df.columns), None)

if mt_col:
    df["_title_len"] = df[mt_col].fillna("").astype(str).str.len()
if md_col:
    df["_desc_len"] = df[md_col].fillna("").astype(str).str.len()

if flag_title_long and mt_col:
    df = df[df["_title_len"] > META_TITLE_MAX]
if flag_desc_long and md_col:
    df = df[df["_desc_len"] > META_DESC_MAX]
if flag_title_empty and mt_col:
    df = df[df[mt_col].fillna("").astype(str).str.strip() == ""]
if flag_desc_empty and md_col:
    df = df[df[md_col].fillna("").astype(str).str.strip() == ""]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Totaal", len(df))
if mt_col:
    c2.metric(f"Title > {META_TITLE_MAX}", int((df["_title_len"] > META_TITLE_MAX).sum()))
if md_col:
    c3.metric(f"Desc > {META_DESC_MAX}", int((df["_desc_len"] > META_DESC_MAX).sum()))
if mt_col:
    c4.metric("Title leeg", int((df[mt_col].fillna("").astype(str).str.strip() == "").sum()))

st.dataframe(df, hide_index=True, width="stretch")
