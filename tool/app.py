"""SEOkitchen Taakdashboard — entry point.

Gebruikt st.navigation zodat per paginawissel alleen de betreffende module
wordt uitgevoerd (in plaats van alle 7 tabs tegelijk bij elke interactie).

Start:
    streamlit run tool/app.py
"""
from __future__ import annotations

import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
_DASHBOARD = _ROOT / "dashboard_v2"

for _p in [str(_DASHBOARD), str(_ROOT), str(_HERE)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

import streamlit as st

st.set_page_config(
    page_title="SEOkitchen — Taken",
    page_icon="🍳",
    layout="wide",
    initial_sidebar_state="expanded",
)

try:
    from ui.session import ensure_defaults
    ensure_defaults()
except Exception:
    pass

# Client selector en branding in sidebar (zichtbaar op elke pagina)
try:
    from client import client_selector
    with st.sidebar:
        st.markdown("## 🍳 SEOkitchen")
        st.caption("Taakdashboard — Hextom is altijd de uitvoer")
        st.divider()
        st.markdown("**Klant**")
        client_selector()
except Exception:
    pass

PAGES_DIR = _HERE / "pages"

NAV = {
    "Pipeline": [
        st.Page(str(PAGES_DIR / "01_Nieuwe.py"),    title="Nieuwe producten",  icon="📦", default=True, url_path="nieuwe"),
        st.Page(str(PAGES_DIR / "02_Prijzen.py"),   title="Prijzen bijwerken", icon="💶", url_path="prijzen"),
        st.Page(str(PAGES_DIR / "03_Collectie.py"), title="Collectie SEO",     icon="🌐", url_path="collectie"),
        st.Page(str(PAGES_DIR / "08_Herverwerk.py"), title="Archief herverwerken", icon="♻️", url_path="herverwerk"),
    ],
    "Overzicht": [
        st.Page(str(PAGES_DIR / "04_Status.py"),    title="Status & Analyses", icon="🔍", url_path="status"),
        st.Page(str(PAGES_DIR / "05_Inzicht.py"),   title="Inzicht",           icon="🔎", url_path="inzicht"),
    ],
    "Overig": [
        st.Page(str(PAGES_DIR / "06_Chat.py"),      title="Assistent",         icon="💬", url_path="chat"),
        st.Page(str(PAGES_DIR / "07_Notities.py"),  title="Notities",          icon="📝", url_path="notities"),
    ],
}

nav = st.navigation(NAV, position="sidebar", expanded=True)
nav.run()
