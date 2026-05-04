"""Categorie-mapping uitbreiden met nieuwe leverancier-codes (NEW_MAPPINGS)."""
from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

from ui.layout import explainer, page_header

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))


page_header("➕ Categorie-mapping uitbreiden")

explainer(
    "Voert de hardcoded `NEW_MAPPINGS` uit `execution/extend_category_mapping.py` toe aan "
    "`seo_category_mapping`. Bestaande keys worden geskipt (upsert)."
)

st.info(
    "💡 Wil je nieuwe mappings toevoegen via chef-correcties? Gebruik **Learnings** "
    "(typ natuurlijke taal → Claude structureert → Apply). Deze pagina is voor "
    "geprogrammeerde batch-extensies.",
    icon="ℹ️",
)


if st.button("🚀 Run extend_category_mapping", type="primary"):
    from execution.extend_category_mapping import extend_category_mapping

    log_area = st.empty()
    log_lines: list[str] = []
    def _log(msg): log_lines.append(str(msg)); log_area.code("\n".join(log_lines[-30:]))

    try:
        result = extend_category_mapping(logger=_log)
        if isinstance(result, dict):
            c1, c2, c3 = st.columns(3)
            c1.metric("Inserted", result.get("inserted", 0))
            c2.metric("Skipped", result.get("skipped", 0))
            c3.metric("Totaal mappings", result.get("total_mappings", 0))
            if "coverage" in result:
                st.metric("Coverage", f"{result.get('coverage', 0):.1f}%",
                          help=f"Matched {result.get('matched', 0)} van {result.get('total_products', 0)} producten")
        st.success("✅ Extend klaar!")
    except Exception as e:
        st.error(f"❌ {e}")
        import traceback
        with st.expander("Traceback"):
            st.code(traceback.format_exc())
