"""Foto's resizen — download van Serax CDN → max 4999×4999 → Supabase Storage."""
from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

from ui.layout import explainer, page_header

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))


page_header("🖼️ Foto's resizen")

explainer(
    "Download foto's van de Serax CDN, resized ze onder Shopify's 25 MP limiet "
    "(max 4999×4999, 90% JPEG), upload naar Supabase Storage en zet de URL "
    "terug in `seo_products.photo_packshot_*` / `photo_lifestyle_*`."
)


c1, c2, c3 = st.columns(3)
with c1:
    dry = st.toggle("Dry-run", value=True, key="foto_dry",
                    help="Aan: alleen rapporteren. Uit: daadwerkelijk downloaden + uploaden.")
with c2:
    skip_existing = st.toggle("Skip bestaande", value=True, key="foto_skip")
with c3:
    limit = st.number_input("Limit (0 = alles)", min_value=0, max_value=5000, value=50, key="foto_lim")


if st.button("🚀 Run photo resize", type="primary"):
    from execution.resize_photos import resize_photos

    prog = st.progress(0.0)
    log_area = st.empty()
    log_lines: list[str] = []
    def _log(msg): log_lines.append(str(msg)); log_area.code("\n".join(log_lines[-30:]))
    def _prog(i, n, msg=""): prog.progress(min(max(i / max(n, 1), 0.0), 1.0)); msg and _log(msg)

    try:
        result = resize_photos(
            dry_run=dry,
            limit=int(limit) if limit else None,
            skip_existing=skip_existing,
            progress=_prog,
            logger=_log,
        )
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Verwerkt", getattr(result, "processed_count", 0))
        m2.metric("Uploaded", getattr(result, "uploaded_count", 0))
        m3.metric("Skipped", getattr(result, "skipped_count", 0))
        m4.metric("Errors", len(getattr(result, "errors", []) or []))
        errs = getattr(result, "errors", []) or []
        if errs:
            with st.expander(f"⚠️ {len(errs)} errors"):
                st.json(errs[:50])
    except Exception as e:
        st.error(f"❌ {e}")
        import traceback
        with st.expander("Traceback"):
            st.code(traceback.format_exc())
