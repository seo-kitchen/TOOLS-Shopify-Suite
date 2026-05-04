"""Categorie-seed — eenmalig seeden van seo_category_mapping."""
from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

from ui.layout import explainer, page_header

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))


page_header("🌱 Categorie-mapping seeden")

explainer(
    "Eenmalige actie: populeer `seo_category_mapping` met de baseline SOP-"
    "categorie mapping. **Upsert** voegt toe / update. **Reset** leegt de "
    "tabel eerst en herseedt. Gebruik reset alleen als je zeker weet wat je doet."
)

mode = st.radio("Mode", ["upsert", "reset"], horizontal=True, key="seed_mode",
                help="upsert = toevoegen/updaten · reset = eerst alles verwijderen, dan opnieuw seeden")

if mode == "reset":
    st.warning(
        "⚠️ **Reset modus.** Alle bestaande rijen in `seo_category_mapping` worden verwijderd. "
        "Alle learnings van type `category_mapping` die zijn doorgevoerd gaan verloren.",
        icon="⚠️",
    )
    confirm = st.text_input("Type `RESET` om te bevestigen", key="seed_confirm")
    if confirm != "RESET":
        st.stop()

if st.button(f"🚀 Run seed ({mode})", type="primary"):
    try:
        from execution.seed_categories import seed_categories
    except ImportError as e:
        st.error(f"⏳ seed_categories niet beschikbaar: {e}")
        st.stop()

    log_area = st.empty()
    log_lines: list[str] = []
    def _log(msg): log_lines.append(str(msg)); log_area.code("\n".join(log_lines[-30:]))

    try:
        result = seed_categories(mode=mode, logger=_log)
        c1, c2, c3 = st.columns(3)
        c1.metric("Inserted", getattr(result, "inserted", 0))
        c2.metric("Updated", getattr(result, "updated", 0))
        c3.metric("Deleted", getattr(result, "deleted", 0))
        st.success("✅ Seed klaar!")
    except Exception as e:
        st.error(f"❌ Fout: {e}")
        import traceback
        with st.expander("Traceback"):
            st.code(traceback.format_exc())
