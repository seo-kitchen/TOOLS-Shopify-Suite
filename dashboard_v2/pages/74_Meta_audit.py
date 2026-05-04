"""Meta audit loader — laad Shopify active export in shopify_meta_audit."""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import streamlit as st

from ui.layout import explainer, page_header

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))


page_header("🔍 Meta audit — laden")

explainer(
    "Upload het 'Alle Active Producten' Shopify-export bestand. "
    "Whitelist-kolommen (Product ID, Handle, Title, Vendor, Meta Title, Meta Description) "
    "worden geladen in `shopify_meta_audit`. Daarna kun je via "
    "**Overzichten → Meta audit overzicht** zien welke producten meta's missen."
)


uploaded = st.file_uploader(
    "Shopify active export (.xlsx)",
    type=["xlsx"],
    key="ma_file",
    help="Typisch 'Alle Active Producten.xlsx' exportbestand uit Shopify Admin.",
)

dry_run = st.toggle("Dry-run (valideer zonder schrijven)", value=True, key="ma_dry")


if uploaded is not None and st.button("🚀 Run meta audit loader", type="primary"):
    from execution.meta_audit_loader import load_meta_audit

    tmpdir = Path(tempfile.mkdtemp(prefix="ma_"))
    path = tmpdir / uploaded.name
    path.write_bytes(uploaded.getvalue())

    prog = st.progress(0.0)
    log_area = st.empty()
    log_lines: list[str] = []
    def _log(msg): log_lines.append(str(msg)); log_area.code("\n".join(log_lines[-30:]))
    def _prog(i, n, msg=""): prog.progress(min(max(i / max(n, 1), 0.0), 1.0)); msg and _log(msg)

    try:
        result = load_meta_audit(
            file_path=str(path),
            dry_run=dry_run,
            progress=_prog,
            logger=_log,
        )
        c1, c2, c3 = st.columns(3)
        c1.metric("Geladen", getattr(result, "loaded_count", 0))
        c2.metric("Title issues", getattr(result, "title_issues_count", 0))
        c3.metric("Desc issues", getattr(result, "desc_issues_count", 0))
        if dry_run:
            st.info("🧪 Dry-run: geen writes naar shopify_meta_audit.")
        else:
            st.success("✅ Meta audit data geladen!")
    except Exception as e:
        st.error(f"❌ {e}")
        import traceback
        with st.expander("Traceback"):
            st.code(traceback.format_exc())
