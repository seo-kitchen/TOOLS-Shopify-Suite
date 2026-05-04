"""Collections & filter values — seo_website_collections + seo_filter_values."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from ui.layout import page_header
from ui.supabase_client import get_supabase


page_header("📚 Collections & filter values")


@st.cache_data(ttl=120, show_spinner=False)
def _collections() -> pd.DataFrame:
    sb = get_supabase()
    return pd.DataFrame(sb.table("seo_website_collections").select("*").execute().data or [])


@st.cache_data(ttl=120, show_spinner=False)
def _filters() -> pd.DataFrame:
    sb = get_supabase()
    return pd.DataFrame(sb.table("seo_filter_values").select("*").execute().data or [])


tab_coll, tab_flt = st.tabs(["📁 Collections", "🎛️ Filter values"])

with tab_coll:
    try:
        df = _collections()
    except Exception as e:
        st.error(f"❌ {e}")
        df = pd.DataFrame()
    if df.empty:
        st.info("Geen collections geladen. Laad via Setup → Website-structuur.")
    else:
        zoek = st.text_input("Zoek", key="coll_search")
        if zoek:
            mask = df.apply(lambda r: any(zoek.lower() in str(v).lower() for v in r.values), axis=1)
            df = df[mask]
        st.caption(f"{len(df)} collections")
        st.dataframe(df, hide_index=True, width="stretch")

with tab_flt:
    try:
        df = _filters()
    except Exception as e:
        st.error(f"❌ {e}")
        df = pd.DataFrame()
    if df.empty:
        st.info("Geen filter values geladen.")
    else:
        if "type" in df.columns:
            typ = st.multiselect("Type", sorted(df["type"].dropna().unique()), key="flt_type")
            if typ:
                df = df[df["type"].isin(typ)]
        zoek = st.text_input("Zoek waarde", key="flt_search")
        if zoek:
            df = df[df.apply(lambda r: any(zoek.lower() in str(v).lower() for v in r.values), axis=1)]
        st.caption(f"{len(df)} filter-values")
        st.dataframe(df, hide_index=True, width="stretch")
