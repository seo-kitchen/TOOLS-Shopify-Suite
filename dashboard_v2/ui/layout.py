"""Layout primitives so every page has the same look.

Usage:
    from ui.layout import page_header, explainer, side_by_side, action_bar
"""
from __future__ import annotations

from typing import Iterable

import pandas as pd
import streamlit as st


def page_header(title: str, subtitle: str | None = None, fase: str | None = None,
                last_run: str | None = None) -> None:
    """Consistent header at the top of every page."""
    cols = st.columns([3, 1, 1])
    with cols[0]:
        st.title(title)
        if subtitle:
            st.caption(subtitle)
    with cols[1]:
        if fase:
            st.metric("Fase", fase)
    with cols[2]:
        if last_run:
            st.metric("Laatste run", last_run)
    st.divider()


def explainer(text: str) -> None:
    """Block A — short 'wat gaat er gebeuren' panel."""
    st.info(text, icon="ℹ️")


def side_by_side(
    left_title: str,
    left_df: pd.DataFrame | dict | None,
    right_title: str,
    right_df: pd.DataFrame | dict | None,
    diff_title: str | None = None,
    diff_df: pd.DataFrame | dict | None = None,
) -> None:
    """Block B — Excel-bron naast Supabase-state (en optioneel 'na' kolom).

    Accepts either a DataFrame or a dict (which will be rendered as a 2-col table).
    """
    cols = st.columns(3 if diff_df is not None else 2)

    def _render(container, title: str, data):
        with container:
            st.markdown(f"**{title}**")
            if data is None:
                st.caption("_(geen data)_")
                return
            if isinstance(data, dict):
                df = pd.DataFrame([{"veld": k, "waarde": v} for k, v in data.items()])
                st.dataframe(df, hide_index=True, width="stretch")
            else:
                st.dataframe(data, hide_index=True, width="stretch")

    _render(cols[0], left_title, left_df)
    _render(cols[1], right_title, right_df)
    if diff_df is not None and diff_title is not None:
        _render(cols[2], diff_title, diff_df)


def action_bar(
    buttons: Iterable[tuple[str, str, str]],
    batch_cap: int | None = None,
) -> dict[str, bool]:
    """Block C — consistent action row.

    Each button tuple: (key, label, type)  where type is 'primary' or 'secondary'.
    Returns a dict mapping key -> clicked (bool).
    """
    clicks: dict[str, bool] = {}
    with st.container():
        if batch_cap is not None:
            st.caption(f"⚠️ Batch-cap: maximaal **{batch_cap}** items per run (voorkomt massa-Claude calls).")
        cols = st.columns(len(list(buttons)) or 1)

    buttons = list(buttons)
    cols = st.columns(len(buttons) or 1)
    for i, (key, label, btn_type) in enumerate(buttons):
        clicks[key] = cols[i].button(label, type=btn_type, key=f"action_{key}", width="stretch")
    return clicks


def kpi_card(col, label: str, value: str | int, delta: str | None = None,
             help_text: str | None = None) -> None:
    """One KPI tile for the Home page scorecard row."""
    with col:
        st.metric(label=label, value=value, delta=delta, help=help_text)


def result_panel(success: bool, message: str, details: str | None = None) -> None:
    """Block D — uniform result panel after a run."""
    if success:
        st.success(message, icon="✅")
    else:
        st.error(message, icon="❌")
    if details:
        with st.expander("Details"):
            st.code(details)
