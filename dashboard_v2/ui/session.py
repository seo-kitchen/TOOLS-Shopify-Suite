"""Typed accessors for the session-state keys used across pages.

Keeping these in one place avoids the session_state sprawl from
streamlit_app.py where typo'd keys silently failed.
"""
from __future__ import annotations

from typing import Any

import streamlit as st


DEFAULTS = {
    # Active fase selector (string so URL-query-params round-trip cleanly)
    "fase": "4",
    # Selected merk voor titel-building
    "merk": "Serax",
    # Uploaded dataframe (after a fresh ingest upload)
    "uploaded_df": None,
    "uploaded_filename": None,
    # Product-ids selected for next pipeline step
    "selected_ids": [],
    # Transform batch cap
    "transform_batch_cap": 25,
    # Match queue (twijfelgevallen pending user decision)
    "match_queue": [],
    # Last pipeline run results per step
    "last_run": {},
}


def ensure_defaults() -> None:
    for k, v in DEFAULTS.items():
        if k not in st.session_state:
            st.session_state[k] = v


def get(key: str, default: Any = None) -> Any:
    return st.session_state.get(key, default if default is not None else DEFAULTS.get(key))


def set(key: str, value: Any) -> None:  # noqa: A001 - shadowing builtin is intentional here
    st.session_state[key] = value
