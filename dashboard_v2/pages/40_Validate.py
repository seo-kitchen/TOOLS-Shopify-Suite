"""4. Validate — quality checks + auto-fixes (decimals, meta-lengte)."""
from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

from ui.job_lock import acquire, current_holder, release
from ui.layout import explainer, page_header
from ui.supabase_client import current_user_email, get_supabase

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))


page_header("✅ 4. Validate")

explainer(
    "Checkt verplichte velden (titel, EAN, prijs, hoofdcat), dubbele EANs, "
    "decimals, meta title ≤60 / desc ≤155, categorieën/filter-values die op de "
    "website bestaan, foto-URLs aanwezig. Auto-fix voor decimals & meta truncation."
)


c1, c2, c3 = st.columns([1, 1, 2])
with c1:
    fase = st.selectbox("Fase", ["1", "2", "3", "4", "5", "6"], index=3, key="val_fase")
with c2:
    autofix = st.toggle("Auto-fix aan", value=True, key="val_autofix",
                        help="Decimalen opschonen en meta truncation. Aanbevolen: aan.")
with c3:
    st.caption(
        "🧪 Alleen rijen met status='ready' of 'review' worden gecheckt. "
        "Rijen die falen worden op status='review' gezet met `review_reden`."
    )


@st.cache_data(ttl=30, show_spinner=False)
def _preview_count(fase: str):
    sb = get_supabase()
    return (
        sb.table("seo_products")
        .select("id", count="exact")
        .eq("fase", fase)
        .in_("status", ["ready", "review"])
        .execute()
        .count
        or 0
    )


try:
    cnt = _preview_count(fase)
    st.metric(f"Rijen in fase {fase} met status ready/review", f"{cnt:,}")
except Exception as e:
    st.error(f"Kon preview niet ophalen: {e}")
    cnt = 0


st.divider()

if st.button(f"🚀 Run validate fase {fase}", type="primary", disabled=cnt == 0):
    try:
        from execution.validate import validate_fase
    except ImportError as e:
        st.error(f"⏳ `validate_fase` niet beschikbaar: {e}")
        st.stop()

    lock = acquire(fase=fase, step="validate",
                   details={"user": current_user_email(), "autofix": autofix})
    if lock is None:
        h = current_holder(fase, "validate")
        st.error(f"🔒 Vergrendeld door {h.get('started_by') if h else '?'}.")
        st.stop()

    prog = st.progress(0.0)
    log_area = st.empty()
    log_lines: list[str] = []

    def _log(msg): log_lines.append(str(msg)); log_area.code("\n".join(log_lines[-30:]))
    def _prog(i, n, msg=""): prog.progress(min(max(i / max(n, 1), 0.0), 1.0)); msg and _log(msg)

    try:
        result = validate_fase(fase=fase, autofix=autofix, progress=_prog, logger=_log)
        release(lock["id"], success=True, details={
            "total": getattr(result, "total", 0),
            "ok": getattr(result, "ok", 0),
            "review": getattr(result, "review", 0),
        })
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Totaal gecheckt", getattr(result, "total", 0))
        m2.metric("OK", getattr(result, "ok", 0))
        m3.metric("Naar review", getattr(result, "review", 0))
        m4.metric("Auto-fixed", getattr(result, "autofixed", 0))
        issues = getattr(result, "issues", []) or []
        if issues:
            with st.expander(f"🔍 {len(issues)} issues"):
                st.json(issues[:200])
    except Exception as e:
        release(lock["id"], success=False, details=str(e))
        st.error(f"❌ Fout: {e}")
        import traceback
        with st.expander("Traceback"):
            st.code(traceback.format_exc())
