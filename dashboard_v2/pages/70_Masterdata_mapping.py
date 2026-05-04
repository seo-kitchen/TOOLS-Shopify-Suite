"""Masterdata kolom-mapping — detect & store kolom-mapping per leverancier."""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st

from ui.layout import explainer, page_header

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))


page_header("🗂️ Masterdata kolom-mapping")

explainer(
    "Upload een supplier-Excel → auto-detect kolom-layout → preview → opslaan "
    "in `config/kolom_mapping_{leverancier}.json` zodat ingest dezelfde mapping "
    "volgende keer meteen kan gebruiken."
)


c1, c2 = st.columns([2, 1])
with c1:
    uploaded = st.file_uploader("Supplier Excel", type=["xlsx", "xls"], key="md_file")
with c2:
    leverancier = st.selectbox(
        "Leverancier",
        ["serax", "potterypots", "printworks", "sp_bonbistro"],
        key="md_lev",
    )


if uploaded is not None:
    try:
        df_preview = pd.read_excel(uploaded, dtype=str, nrows=10)
        uploaded.seek(0)
        st.dataframe(df_preview, hide_index=True, width="stretch")
        st.caption(f"Eerste 10 rijen · {len(df_preview.columns)} kolommen gedetecteerd")
    except Exception as e:
        st.error(f"Kon Excel niet lezen: {e}")


if uploaded is not None and st.button("🔍 Detect & opslaan", type="primary"):
    from execution.setup_masterdata import detect_and_store_mapping

    tmpdir = Path(tempfile.mkdtemp(prefix="md_"))
    path = tmpdir / uploaded.name
    path.write_bytes(uploaded.getvalue())

    log_area = st.empty()
    log_lines: list[str] = []
    def _log(msg): log_lines.append(str(msg)); log_area.code("\n".join(log_lines[-30:]))

    # Simple callbacks: auto-confirm since this page is interactive anyway
    def _on_confirm(mapping: dict) -> bool:
        st.markdown("#### Gedetecteerde mapping:")
        st.json(mapping)
        return True

    def _on_ambiguous(field: str, candidates: list[str]) -> str:
        if not candidates:
            return ""
        # Pick the first one; in a real interactive flow we'd ask the user.
        _log(f"Ambiguous field '{field}' — auto-picked: {candidates[0]}")
        return candidates[0]

    try:
        mapping = detect_and_store_mapping(
            file_path=str(path),
            leverancier=leverancier,
            on_confirm=_on_confirm,
            on_ambiguous=_on_ambiguous,
            logger=_log,
        )
        st.success(f"✅ Mapping opgeslagen in `config/kolom_mapping_{leverancier}.json`")
        st.json(mapping)
    except Exception as e:
        st.error(f"❌ {e}")
        import traceback
        with st.expander("Traceback"):
            st.code(traceback.format_exc())
