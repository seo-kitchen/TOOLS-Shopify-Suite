"""Client selector — bovenaan elke pagina.

Slaat de actieve klant op in st.session_state["active_client"].
Alle Supabase queries worden gefilterd op client_id.

Uitbreiden naar meerdere klanten: voeg een entry toe aan CLIENTS
en voeg de Supabase-credentials toe aan .env als SUPABASE_URL_<key>.
"""
from __future__ import annotations

import os
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# Klanten-register: slug → weergavenaam
# Voeg hier nieuwe klanten toe als je schaalt.
CLIENTS: dict[str, str] = {
    "interieurshop": "Interieur Shop NL",
}

DEFAULT_CLIENT = "interieurshop"


def client_selector() -> str:
    """Render de client dropdown en return de actieve client_id."""
    if "active_client" not in st.session_state:
        st.session_state["active_client"] = DEFAULT_CLIENT

    options = list(CLIENTS.keys())
    current = st.session_state["active_client"]
    idx = options.index(current) if current in options else 0

    selected = st.selectbox(
        "Klant",
        options=options,
        format_func=lambda k: CLIENTS.get(k, k),
        index=idx,
        key="_client_sel",
        label_visibility="collapsed",
    )
    if selected != st.session_state["active_client"]:
        # Reset wizard-state bij wisselen van klant
        for key in list(st.session_state.keys()):
            if key.startswith(("np_", "pr_", "col_")):
                del st.session_state[key]
    st.session_state["active_client"] = selected
    return selected


def get_client_id() -> str:
    return st.session_state.get("active_client", DEFAULT_CLIENT)


def get_client_label() -> str:
    cid = get_client_id()
    return CLIENTS.get(cid, cid)
