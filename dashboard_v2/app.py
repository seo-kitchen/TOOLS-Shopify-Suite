"""SEOkitchen unified dashboard — entry point.

Start met:
    streamlit run dashboard_v2/app.py

Structuur:
    app.py               — dit bestand, bouwt de navigatie met st.navigation
    pages/               — één Python file per pagina
    ui/                  — shared helpers (supabase, layout, learnings, job_lock)
    execution/           — frozen-snapshot kopieën van de execution scripts
                           die door pages worden geïmporteerd.
"""
from __future__ import annotations

import sys
from pathlib import Path

# Zorg dat ``ui`` en ``execution`` importable zijn vanuit pages/ files, ongeacht
# vanuit welke map streamlit gestart wordt.
_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import streamlit as st  # noqa: E402

from ui.session import ensure_defaults  # noqa: E402


st.set_page_config(
    page_title="SEOkitchen Dashboard",
    page_icon="🍳",
    layout="wide",
    initial_sidebar_state="expanded",
)
ensure_defaults()


# ─────────────────────────────────────────────────────────────────────────────
# Pagina-registratie
# Bestaande volgorde / iconen / URL's worden bepaald hier. Voeg nieuwe pagina's
# toe door ze onder in ``pages/`` te zetten en hier een regel toe te voegen.
# ─────────────────────────────────────────────────────────────────────────────

PAGES_DIR = _HERE / "pages"

NAV = {
    "Overview": [
        st.Page(str(PAGES_DIR / "00_Home.py"),                  title="Home",                 icon="🏠", default=True, url_path="home"),
    ],
    "Pipeline": [
        st.Page(str(PAGES_DIR / "10_Ingest.py"),                title="1. Ingest",            icon="📥", url_path="ingest"),
        st.Page(str(PAGES_DIR / "20_Match.py"),                 title="2. Match",             icon="🔗", url_path="match"),
        st.Page(str(PAGES_DIR / "30_Transform.py"),             title="3. Transform",         icon="✨", url_path="transform"),
        st.Page(str(PAGES_DIR / "40_Validate.py"),              title="4. Validate",          icon="✅", url_path="validate"),
        st.Page(str(PAGES_DIR / "50_Export.py"),                title="5. Export",            icon="📤", url_path="export"),
    ],
    "Post-export": [
        st.Page(str(PAGES_DIR / "60_Prijzen.py"),               title="Prijzen updaten",      icon="💶", url_path="prijzen"),
        st.Page(str(PAGES_DIR / "61_Foto_resize.py"),           title="Foto's resizen",       icon="🖼️", url_path="fotos"),
        st.Page(str(PAGES_DIR / "62_Bynder.py"),                title="Bynder matching",      icon="📸", url_path="bynder"),
        st.Page(str(PAGES_DIR / "63_Serax_dimensies.py"),       title="Serax dimensies",      icon="📐", url_path="serax-dim"),
    ],
    "Setup & reference": [
        st.Page(str(PAGES_DIR / "70_Masterdata_mapping.py"),    title="Masterdata-mapping",   icon="🗂️", url_path="masterdata"),
        st.Page(str(PAGES_DIR / "71_Categorieen_seed.py"),      title="Categorie-seed",       icon="🌱", url_path="cat-seed"),
        st.Page(str(PAGES_DIR / "72_Categorieen_extend.py"),    title="Categorie uitbreiden", icon="➕", url_path="cat-extend"),
        st.Page(str(PAGES_DIR / "73_Website_structuur.py"),     title="Website-structuur",    icon="🌐", url_path="website-struct"),
        st.Page(str(PAGES_DIR / "74_Meta_audit.py"),            title="Meta audit loader",    icon="🔍", url_path="meta-audit-load"),
    ],
    "Overzichten": [
        st.Page(str(PAGES_DIR / "80_Producten.py"),             title="Producten",            icon="📦", url_path="producten"),
        st.Page(str(PAGES_DIR / "81_Categorieen.py"),           title="Categorieën",          icon="🏷️", url_path="categorieen"),
        st.Page(str(PAGES_DIR / "82_Import_runs.py"),           title="Import runs",          icon="📜", url_path="import-runs"),
        st.Page(str(PAGES_DIR / "83_Shopify_index.py"),         title="Shopify index",        icon="🛍️", url_path="shopify-index"),
        st.Page(str(PAGES_DIR / "84_Collections.py"),           title="Collections & filters", icon="📚", url_path="collections"),
        st.Page(str(PAGES_DIR / "85_Meta_audit_view.py"),       title="Meta audit overzicht", icon="🔎", url_path="meta-audit-view"),
    ],
    "Learning system": [
        st.Page(str(PAGES_DIR / "90_Learnings.py"),             title="Learnings",            icon="🧠", url_path="learnings"),
    ],
}


nav = st.navigation(NAV, position="sidebar", expanded=True)
nav.run()
