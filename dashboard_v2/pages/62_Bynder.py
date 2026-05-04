"""Bynder matching — match Bynder photo exports tegen seo_products + shopify_index."""
from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

from ui.layout import explainer, page_header

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))


page_header("📸 Bynder matching")

explainer(
    "Parset Bynder photo-exports (SKU's uit filenames), matcht tegen `seo_products` "
    "en `seo_shopify_index`, produceert een xlsx met SKU / title / handle / "
    "barcode per foto, of 'NO MATCH'."
)


c1, c2 = st.columns(2)
with c1:
    share = st.number_input("Share nummer", min_value=1, max_value=99, value=1, key="by_share")
with c2:
    output_path = st.text_input("Output pad (optioneel)", value="", key="by_out",
                                  help="Leeg laten = auto-naam in ./exports/")


if st.button("🚀 Run Bynder matching", type="primary"):
    from execution.match_bynder_photos import match_bynder

    log_area = st.empty()
    log_lines: list[str] = []
    def _log(msg): log_lines.append(str(msg)); log_area.code("\n".join(log_lines[-30:]))

    try:
        result = match_bynder(
            share=int(share),
            output_path=output_path or None,
            logger=_log,
        )
        c1, c2 = st.columns(2)
        c1.metric("Matched", getattr(result, "matched_count", 0))
        c2.metric("Unmatched", getattr(result, "unmatched_count", 0))

        xlsx_path = Path(getattr(result, "xlsx_path", "") or "")
        if xlsx_path.exists():
            st.download_button(
                f"📥 Download resultaat ({xlsx_path.name})",
                data=xlsx_path.read_bytes(),
                file_name=xlsx_path.name,
            )
    except Exception as e:
        st.error(f"❌ {e}")
        import traceback
        with st.expander("Traceback"):
            st.code(traceback.format_exc())
