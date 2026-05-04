"""Import runs — log van alle ingest-acties."""
from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

from ui.layout import page_header
from ui.supabase_client import get_supabase


page_header("📜 Import runs", subtitle="Historie van alle ingest-runs uit `seo_import_runs`.")


with st.sidebar:
    st.markdown("### Filters")
    fase = st.selectbox("Fase", ["alle", "1", "2", "3", "4", "5", "6"], key="imp_fase")
    merken = st.multiselect("Leverancier", ["Serax", "Pottery Pots", "Printworks", "S&P/Bonbistro"], key="imp_merk")
    status_opts = st.multiselect("Status", ["ok", "error", "partial"], key="imp_status")
    days_back = st.slider("Periode (dagen terug)", 1, 90, 30, key="imp_days")
    limit = st.selectbox("Max rijen", [50, 100, 500, 1000], index=1, key="imp_limit")


@st.cache_data(ttl=60, show_spinner=False)
def _fetch(fase, merken, status_opts, days_back, limit):
    sb = get_supabase()
    since = (datetime.utcnow() - timedelta(days=days_back)).isoformat()
    q = sb.table("seo_import_runs").select("*").gte("created_at", since).order("created_at", desc=True).limit(limit)
    if fase != "alle":
        q = q.eq("fase", fase)
    if merken:
        q = q.in_("leverancier", merken)
    if status_opts:
        q = q.in_("status", status_opts)
    return q.execute().data or []


try:
    rows = _fetch(fase, tuple(merken), tuple(status_opts), days_back, limit)
except Exception as e:
    st.error(f"❌ Fout: {e}")
    st.stop()

if not rows:
    st.info("Geen import runs gevonden met deze filters.")
    st.stop()

df = pd.DataFrame(rows)

c1, c2, c3 = st.columns(3)
c1.metric("Runs gevonden", len(df))
if "status" in df.columns:
    c2.metric("OK", int((df["status"] == "ok").sum()))
    c3.metric("Errors", int((df["status"].isin(["error", "partial"])).sum()))

st.dataframe(df, hide_index=True, width="stretch")
