"""SEOkitchen Taakdashboard — taakgerichte overlay op de bestaande pipeline.

Start:
    streamlit run tool/app.py

Vereisten:
  - .env in de SEOKITCHEN root (Supabase, Shopify, Anthropic API keys)
  - dashboard_v2/ en execution/ mappen aanwezig (worden NIET gewijzigd)
  - Supabase: migration.sql eenmalig uitvoeren voor seo_export_files tabel

Wat dit doet:
  Vier tabbladen, elk voor een veelvoorkomende klantvraag:
    1. Nieuwe producten toevoegen  (pipeline wizard)
    2. Prijzen bijwerken           (upload → diff → Hextom Excel)
    3. Collectie SEO teksten       (bekijken / genereren / exporteren)
    4. Status & Analyses           (health checks, Hextom wachtrij, gaps)

Wat dit NIET doet:
  - Bestaande dashboard_v2/ pagina's aanpassen
  - Direct naar Shopify pushen (altijd via Hextom Excel)
"""
from __future__ import annotations

import sys
from pathlib import Path

# ── Importpaden ───────────────────────────────────────────────────────────────
# dashboard_v2/ wordt toegevoegd zodat ui.supabase_client etc importeerbaar zijn.
# Root wordt toegevoegd zodat execution.* importeerbaar is.
_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
_DASHBOARD = _ROOT / "dashboard_v2"

for _p in [str(_DASHBOARD), str(_ROOT), str(_HERE)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

import streamlit as st

# Sessie-defaults overnemen van bestaand dashboard (fase, merk, etc.)
try:
    from ui.session import ensure_defaults
    ensure_defaults()
except Exception:
    pass

# Lokale tab-modules
from tab_nieuwe import render as render_nieuwe
from tab_prijzen import render as render_prijzen
from tab_collectie import render as render_collectie
from tab_status import render as render_status
from tab_notes import render as render_notes
from tab_inzicht import render as render_inzicht
from tab_chat import render as render_chat
from client import client_selector, get_client_label

# ── Pagina-config ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SEOkitchen — Taken",
    page_icon="🍳",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Header ────────────────────────────────────────────────────────────────────
h1, h2, h3 = st.columns([3, 2, 1])
with h1:
    st.markdown("## 🍳 SEOkitchen")
    st.caption("Taakdashboard — Hextom is altijd de uitvoer naar Shopify")
with h2:
    st.caption("&nbsp;")
with h3:
    st.markdown("**Klant**")
    client_selector()

st.divider()

# ── Vier tabbladen ────────────────────────────────────────────────────────────
tabs = st.tabs([
    "📦 Nieuwe producten",
    "💶 Prijzen bijwerken",
    "🌐 Collectie SEO",
    "🔍 Status & Analyses",
    "🔎 Inzicht",
    "💬 Assistent",
    "📝 Notities",
])

with tabs[0]:
    render_nieuwe()

with tabs[1]:
    render_prijzen()

with tabs[2]:
    render_collectie()

with tabs[3]:
    render_status()

with tabs[4]:
    render_inzicht()

with tabs[5]:
    render_chat()

with tabs[6]:
    render_notes()
