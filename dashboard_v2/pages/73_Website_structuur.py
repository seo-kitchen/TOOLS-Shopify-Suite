"""Website-structuur loader — laad Shopify active + archive exports."""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import streamlit as st

from ui.layout import explainer, page_header

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))


page_header("🌐 Website-structuur laden")

explainer(
    "Upload de Shopify Admin exports (active + archief producten CSV) en laad ze "
    "in `seo_shopify_index`, `seo_website_collections`, `seo_filter_values`. "
    "Dit is de referentie waar match.py tegenaan checkt."
)


c1, c2 = st.columns(2)
with c1:
    active_csv = st.file_uploader("Active producten CSV", type=["csv"], key="ws_active")
with c2:
    archive_csv = st.file_uploader("Archief producten CSV (optioneel)", type=["csv"], key="ws_archive")


if active_csv is not None and st.button("🚀 Run load_website_structure", type="primary"):
    from execution.load_website_structure import load_website_structure

    tmpdir = Path(tempfile.mkdtemp(prefix="ws_"))
    a_path = tmpdir / active_csv.name
    a_path.write_bytes(active_csv.getvalue())
    r_path = None
    if archive_csv is not None:
        r_path = tmpdir / archive_csv.name
        r_path.write_bytes(archive_csv.getvalue())

    prog = st.progress(0.0)
    log_area = st.empty()
    log_lines: list[str] = []
    def _log(msg): log_lines.append(str(msg)); log_area.code("\n".join(log_lines[-30:]))
    def _prog(i, n, msg=""): prog.progress(min(max(i / max(n, 1), 0.0), 1.0)); msg and _log(msg)

    try:
        result = load_website_structure(
            active_csv=str(a_path),
            archive_csv=str(r_path) if r_path else None,
            progress=_prog,
            logger=_log,
        )
        c1, c2, c3 = st.columns(3)
        c1.metric("Producten", getattr(result, "products_loaded", 0))
        c2.metric("Collections", getattr(result, "collections_loaded", 0))
        c3.metric("Filter-values", getattr(result, "filters_loaded", 0))
        st.success("✅ Website-structuur geladen!")
    except Exception as e:
        st.error(f"❌ {e}")
        import traceback
        with st.expander("Traceback"):
            st.code(traceback.format_exc())
