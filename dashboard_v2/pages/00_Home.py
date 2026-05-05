"""Home — landing page met KPI's en snelle links naar de pipeline."""
from __future__ import annotations

import streamlit as st

from ui.layout import kpi_card, page_header
from ui.supabase_client import get_supabase


page_header(
    "🍳 SEOkitchen Dashboard",
    subtitle="Unified dashboard voor alle Shopify-pipeline tools. Kies links een pagina.",
)


@st.cache_data(ttl=300, show_spinner="Tellen in Supabase…")
def _counts():
    sb = get_supabase()
    total = sb.table("seo_products").select("id", count="exact").execute().count or 0
    review = (
        sb.table("seo_products")
        .select("id", count="exact")
        .eq("status", "review")
        .execute()
        .count
        or 0
    )
    ready = (
        sb.table("seo_products")
        .select("id", count="exact")
        .eq("status", "ready")
        .execute()
        .count
        or 0
    )
    pending = (
        sb.table("seo_learnings")
        .select("id", count="exact")
        .eq("status", "pending")
        .execute()
        .count
        or 0
    )
    runs = (
        sb.table("seo_import_runs")
        .select("*")
        .order("created_at", desc=True)
        .limit(5)
        .execute()
        .data
        or []
    )
    return dict(total=total, review=review, ready=ready, pending=pending, runs=runs)


try:
    data = _counts()
    c1, c2, c3, c4 = st.columns(4)
    kpi_card(c1, "Producten in DB", f"{data['total']:,}")
    kpi_card(c2, "Review-queue", data["review"], help_text="status='review' — heeft handmatige check nodig")
    kpi_card(c3, "Klaar voor export", data["ready"], help_text="status='ready' — kan naar Shopify")
    kpi_card(c4, "Pending learnings", data["pending"], help_text="Correcties die wachten op 'Apply'")

    st.divider()
    st.subheader("Laatste import runs")
    if data["runs"]:
        st.dataframe(data["runs"], hide_index=True, width="stretch")
    else:
        st.caption("Nog geen import runs geregistreerd.")
except Exception as e:
    st.warning(
        "⚠️ Kon Supabase-cijfers niet ophalen. Check .env en of de SQL-migraties "
        f"zijn uitgevoerd (zie `execution/schema_v2_dashboard.sql`).\n\nFout: {e}"
    )

st.divider()
st.markdown(
    """
### Waar begin ik?

- **Nieuwe batch verwerken** → Pipeline → `1. Ingest`
- **Bestaande producten bekijken / filteren** → Overzichten → `Producten`
- **Correctie leren aan het systeem** → Learning system → `Learnings`
- **Prijsupdate draaien** → Post-export → `Prijzen updaten`
"""
)
