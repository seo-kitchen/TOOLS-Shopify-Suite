"""Serax dimensies parsen — 'L 13,8 W 13,8 H 7' → lengte/breedte/hoogte_cm."""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import streamlit as st

from ui.layout import explainer, page_header

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))


page_header("📐 Serax dimensies parsen")

explainer(
    "Leest de dimensie-string uit de Serax masterdata (bijv. `L 13,8 W 13,8 H 7`) "
    "en vult `lengte_cm`, `breedte_cm`, `hoogte_cm` op `seo_products` waar die "
    "velden nog leeg zijn. Override standaard-bestand met eigen upload."
)


uploaded = st.file_uploader(
    "Optioneel: masterdata Excel override",
    type=["xlsx"],
    key="dim_file",
    help="Leeg = gebruikt standaardpad `Master Files/Masterdata serax new items_2026_Interieur-Shop.xlsx`",
)


if st.button("🚀 Run dimensie-parser", type="primary"):
    from execution.fix_serax_dimensions import fix_serax_dimensions

    tmpdir = None
    path: str | None = None
    if uploaded is not None:
        tmpdir = Path(tempfile.mkdtemp(prefix="dim_"))
        p = tmpdir / uploaded.name
        p.write_bytes(uploaded.getvalue())
        path = str(p)

    prog = st.progress(0.0)
    log_area = st.empty()
    log_lines: list[str] = []
    def _log(msg): log_lines.append(str(msg)); log_area.code("\n".join(log_lines[-30:]))
    def _prog(i, n, msg=""): prog.progress(min(max(i / max(n, 1), 0.0), 1.0)); msg and _log(msg)

    try:
        result = fix_serax_dimensions(file_path=path, progress=_prog, logger=_log)
        c1, c2, c3 = st.columns(3)
        c1.metric("Geparsed", getattr(result, "parsed_count", 0))
        c2.metric("Updated", getattr(result, "updated_count", 0))
        c3.metric("Parse-fouten", len(getattr(result, "failed_parse", []) or []))
        failed = getattr(result, "failed_parse", []) or []
        if failed:
            with st.expander(f"⚠️ {len(failed)} SKUs konden niet geparsed worden"):
                st.json(failed[:100])
    except Exception as e:
        st.error(f"❌ {e}")
        import traceback
        with st.expander("Traceback"):
            st.code(traceback.format_exc())
