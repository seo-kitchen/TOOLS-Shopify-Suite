"""SEOkitchen Shopify Suite — entry point."""
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
    page_title="SEOkitchen — Shopify Suite",
    page_icon="🔑",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Huisstijl CSS (Mode B — Cool) ─────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Fraunces:ital,opsz,wght@0,9..144,300;0,9..144,400;0,9..144,500;1,9..144,300;1,9..144,400&family=Inter:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');

:root {
    --sk-paper:      #F2ECDD;
    --sk-cream:      #E8E1D1;
    --sk-cream-2:    #EFE8D8;
    --sk-brown:      #372108;
    --sk-ink-2:      #3F3830;
    --sk-ink-3:      #6F6557;
    --sk-gold:       #8B6F3F;
    --sk-sky:        #AECDF6;
    --sk-sky-soft:   #D6E5FA;
    --sk-navy:       #314159;
    --sk-line:       rgba(55,33,8,0.18);
    --sk-line-str:   rgba(55,33,8,0.38);
    --sk-success:    #4F7A4A;
    --sk-warning:    #B8862E;
    --sk-danger:     #B64027;
}

/* ── Canvas ── */
.stApp {
    background-color: var(--sk-paper) !important;
    font-family: 'Inter', system-ui, sans-serif !important;
    color: var(--sk-brown) !important;
}

header[data-testid="stHeader"] {
    background-color: var(--sk-paper) !important;
    border-bottom: 1px solid var(--sk-line) !important;
}

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background-color: var(--sk-navy) !important;
    border-right: 1px solid rgba(174,205,246,0.12) !important;
}
section[data-testid="stSidebar"] .stMarkdown p,
section[data-testid="stSidebar"] .stMarkdown span,
section[data-testid="stSidebar"] p {
    color: var(--sk-sky-soft) !important;
}
section[data-testid="stSidebar"] small,
section[data-testid="stSidebar"] .stCaption {
    color: rgba(214,229,250,0.50) !important;
}
section[data-testid="stSidebar"] label {
    color: var(--sk-sky-soft) !important;
}
section[data-testid="stSidebar"] hr {
    border-top: 1px solid rgba(174,205,246,0.14) !important;
}
section[data-testid="stSidebar"] .stSelectbox > div > div {
    background: rgba(174,205,246,0.08) !important;
    color: var(--sk-sky-soft) !important;
    border: 1px solid rgba(174,205,246,0.20) !important;
    border-radius: 4px !important;
}

/* Nav section labels */
li[data-testid="stNavSectionHeader"] span,
[data-testid="stSidebarNavSeparator"] span {
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 10px !important;
    letter-spacing: 0.18em !important;
    text-transform: uppercase !important;
    color: rgba(174,205,246,0.40) !important;
}

/* Nav links */
a[data-testid="stSidebarNavLink"] {
    color: var(--sk-sky-soft) !important;
    border-radius: 4px !important;
    font-family: 'Inter', sans-serif !important;
    font-size: 14px !important;
    font-weight: 400 !important;
    padding: 6px 12px !important;
}
a[data-testid="stSidebarNavLink"]:hover {
    background: rgba(174,205,246,0.10) !important;
    color: var(--sk-sky) !important;
}
a[data-testid="stSidebarNavLink"][aria-current="page"] {
    background: rgba(174,205,246,0.16) !important;
    color: var(--sk-sky) !important;
    font-weight: 500 !important;
}

/* ── Typografie ── */
h1, h2, h3 {
    font-family: 'Fraunces', Georgia, serif !important;
    color: var(--sk-brown) !important;
    font-weight: 400 !important;
}
h1 { font-size: 32px !important; line-height: 1.10 !important; }
h2 { font-size: 24px !important; line-height: 1.15 !important; }
h3 { font-size: 20px !important; font-style: italic !important; line-height: 1.20 !important; }

p, li, .stMarkdown {
    font-family: 'Inter', sans-serif !important;
    font-size: 15px !important;
    color: var(--sk-brown) !important;
    line-height: 1.55 !important;
}

/* ── Metrics ── */
[data-testid="stMetric"] {
    background: var(--sk-cream-2) !important;
    border: 1px solid var(--sk-line) !important;
    border-radius: 6px !important;
    padding: 16px 20px !important;
}
[data-testid="stMetricValue"] > div {
    font-family: 'Fraunces', Georgia, serif !important;
    font-weight: 300 !important;
    font-size: 30px !important;
    color: var(--sk-brown) !important;
}
[data-testid="stMetricLabel"] > div {
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 10px !important;
    letter-spacing: 0.14em !important;
    text-transform: uppercase !important;
    color: var(--sk-ink-3) !important;
}

/* ── Knoppen ── */
.stButton > button,
.stDownloadButton > button {
    font-family: 'Inter', sans-serif !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    border-radius: 999px !important;
    padding: 8px 22px !important;
    border: 1px solid var(--sk-brown) !important;
    background: transparent !important;
    color: var(--sk-brown) !important;
    transition: background 0.12s, color 0.12s !important;
    letter-spacing: 0.01em !important;
}
.stButton > button:hover,
.stDownloadButton > button:hover {
    background: rgba(55,33,8,0.07) !important;
}
.stButton > button[kind="primary"] {
    background: var(--sk-brown) !important;
    color: var(--sk-paper) !important;
    border-color: var(--sk-brown) !important;
}
.stButton > button[kind="primary"]:hover {
    background: var(--sk-ink-2) !important;
}

/* ── Inputs ── */
.stTextInput input,
.stNumberInput input,
.stTextArea textarea {
    border: none !important;
    border-bottom: 1px solid var(--sk-line-str) !important;
    border-radius: 0 !important;
    background: transparent !important;
    color: var(--sk-brown) !important;
    font-family: 'Inter', sans-serif !important;
    font-size: 15px !important;
    padding-left: 0 !important;
    box-shadow: none !important;
}
.stTextInput input:focus,
.stNumberInput input:focus,
.stTextArea textarea:focus {
    border-bottom: 2px solid var(--sk-brown) !important;
    box-shadow: none !important;
}
.stSelectbox > div > div,
.stMultiSelect > div > div {
    border: none !important;
    border-bottom: 1px solid var(--sk-line-str) !important;
    border-radius: 0 !important;
    background: transparent !important;
    box-shadow: none !important;
}

/* ── Tabellen ── */
[data-testid="stDataFrameResizable"] th,
[data-testid="stDataFrame"] th {
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 10px !important;
    letter-spacing: 0.12em !important;
    text-transform: uppercase !important;
    color: var(--sk-ink-3) !important;
    background: var(--sk-cream) !important;
    border-bottom: 1px solid var(--sk-line-str) !important;
}
[data-testid="stDataFrame"] td {
    font-family: 'Inter', sans-serif !important;
    font-size: 14px !important;
    color: var(--sk-brown) !important;
}

/* ── Expanders ── */
[data-testid="stExpander"] {
    border: 1px solid var(--sk-line) !important;
    border-radius: 6px !important;
    background: var(--sk-cream-2) !important;
}
[data-testid="stExpanderDetails"] {
    background: var(--sk-cream-2) !important;
}

/* ── Divider ── */
[data-testid="stDivider"] hr, hr {
    border: none !important;
    border-top: 1px solid var(--sk-line) !important;
    margin: 20px 0 !important;
}

/* ── Meldingen ── */
[data-testid="stNotification"] {
    border-radius: 4px !important;
    font-family: 'Inter', sans-serif !important;
    font-size: 14px !important;
}

/* ── Chat ── */
.stChatInput textarea {
    border: 1px solid var(--sk-line-str) !important;
    border-radius: 4px !important;
    background: var(--sk-cream-2) !important;
    font-family: 'Inter', sans-serif !important;
    font-size: 15px !important;
}
[data-testid="stChatMessage"] {
    background: var(--sk-cream-2) !important;
    border: 1px solid var(--sk-line) !important;
    border-radius: 6px !important;
}

/* ── Caption ── */
.stCaption, small {
    color: var(--sk-ink-3) !important;
    font-family: 'Inter', sans-serif !important;
    font-size: 13px !important;
}

/* ── Streamlit chrome verbergen ── */
#MainMenu { visibility: hidden !important; }
footer { display: none !important; }
[data-testid="stToolbar"] { display: none !important; }
[data-testid="stDecoration"] { display: none !important; }
</style>
""", unsafe_allow_html=True)


# ── Sessie ────────────────────────────────────────────────────────────────────
try:
    from ui.session import ensure_defaults
    ensure_defaults()
except Exception:
    pass

# ── Sidebar ───────────────────────────────────────────────────────────────────
try:
    from client import client_selector
    with st.sidebar:
        st.markdown("""
        <div style="padding: 8px 0 16px 0;">
            <div style="font-family:'JetBrains Mono',monospace;font-size:10px;
                        letter-spacing:0.18em;text-transform:uppercase;
                        color:rgba(174,205,246,0.45);margin-bottom:6px;">
                — SEO KITCHEN —
            </div>
            <div style="font-family:'Fraunces',Georgia,serif;font-size:22px;
                        font-weight:400;color:#D6E5FA;line-height:1.1;">
                Shopify Suite
            </div>
        </div>
        """, unsafe_allow_html=True)
        st.divider()
        st.markdown(
            "<span style='font-family:JetBrains Mono,monospace;font-size:10px;"
            "letter-spacing:0.14em;text-transform:uppercase;"
            "color:rgba(174,205,246,0.45)'>Klant</span>",
            unsafe_allow_html=True,
        )
        client_selector()
except Exception:
    pass

# ── Navigatie ─────────────────────────────────────────────────────────────────
PAGES_DIR = _HERE / "pages"

NAV = {
    "Werkbank": [
        st.Page(str(PAGES_DIR / "06_Chat.py"),       title="Werkbank (chat)",            default=True, url_path="chat"),
    ],
    "Pipeline (volledig)": [
        st.Page(str(PAGES_DIR / "10_Pipeline.py"),   title="Volledige pipeline",         url_path="pipeline"),
        st.Page(str(PAGES_DIR / "11_Learnings.py"),  title="Learnings (feedback)",        url_path="learnings"),
    ],
    "Snelle acties": [
        st.Page(str(PAGES_DIR / "12_Quick_Update.py"), title="Quick Update (upload + fix)", url_path="quick-update"),
        st.Page(str(PAGES_DIR / "01_Nieuwe.py"),     title="Nieuwe producten (legacy)",  url_path="nieuwe"),
        st.Page(str(PAGES_DIR / "02_Prijzen.py"),    title="Prijzen bijwerken",          url_path="prijzen"),
        st.Page(str(PAGES_DIR / "03_Collectie.py"),  title="Collectie SEO-teksten",      url_path="collectie"),
        st.Page(str(PAGES_DIR / "08_Herverwerk.py"), title="Archief herverwerken",       url_path="herverwerk"),
        st.Page(str(PAGES_DIR / "09_Herverwerk_Review.py"), title="Herverwerk — review", url_path="herverwerk-review"),
    ],
    "Overzicht": [
        st.Page(str(PAGES_DIR / "04_Status.py"),     title="Status & analyses",          url_path="status"),
        st.Page(str(PAGES_DIR / "05_Inzicht.py"),    title="Inzicht in producten",       url_path="inzicht"),
    ],
    "Overig": [
        st.Page(str(PAGES_DIR / "07_Notities.py"),   title="Notities",                   url_path="notities"),
    ],
}

nav = st.navigation(NAV, position="sidebar", expanded=True)
nav.run()
