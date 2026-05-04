"""2. Match — match raw producten tegen seo_shopify_index (SKU/EAN).

Twijfelgevallen (EAN-match maar andere SKU) worden in de UI als radio-cards
getoond en door chef beslist. Geen CLI-input() meer nodig.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

from ui.job_lock import acquire, current_holder, release
from ui.layout import explainer, page_header
from ui.supabase_client import current_user_email, get_supabase

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))


page_header("🔗 2. Match tegen Shopify-index")

explainer(
    "Matcht raw producten op SKU (100% → auto) en EAN (twijfel → jij beslist). "
    "Markeert status_shopify (actief / archief / nieuw). Twee passes: eerst "
    "eenvoudige matches + defer twijfels, dan kies je per twijfelgeval."
)


c1, c2, c3 = st.columns([1, 1, 2])
with c1:
    fase = st.selectbox("Fase", ["1", "2", "3", "4", "5", "6"], index=3, key="m_fase")
with c2:
    limit = st.number_input("Max rijen", min_value=1, max_value=5000, value=500, key="m_limit",
                             help="Hoeveel raw producten scannen. Hoger = langer.")


@st.cache_data(ttl=30, show_spinner=False)
def _raw_count(fase: str):
    sb = get_supabase()
    return (
        sb.table("seo_products")
        .select("id", count="exact")
        .eq("fase", fase)
        .eq("status", "raw")
        .execute()
        .count
        or 0
    )


try:
    cnt = _raw_count(fase)
    st.metric(f"Raw rijen in fase {fase}", f"{cnt:,}")
except Exception as e:
    st.error(str(e))


# ── Pass 1: first match run (defers conflicts) ───────────────────────────────

st.divider()
st.markdown("### Stap 1 — First pass (auto-matches + defer twijfels)")

if st.button(f"🚀 Run first-pass match (max {limit})", type="primary"):
    try:
        from execution.match import match_fase
    except ImportError as e:
        st.error(f"⏳ match_fase niet beschikbaar: {e}")
        st.stop()

    lock = acquire(fase=fase, step="match", details={"user": current_user_email()})
    if lock is None:
        h = current_holder(fase, "match")
        st.error(f"🔒 Vergrendeld door {h.get('started_by') if h else '?'}.")
        st.stop()

    prog = st.progress(0.0)
    log_area = st.empty()
    log_lines: list[str] = []
    def _log(msg): log_lines.append(str(msg)); log_area.code("\n".join(log_lines[-30:]))
    def _prog(i, n, msg=""): prog.progress(min(max(i / max(n, 1), 0.0), 1.0)); msg and _log(msg)

    try:
        # First pass: defer all conflicts
        result = match_fase(
            fase=fase,
            on_conflict=lambda excel, hit: "__defer__",
            progress=_prog,
            logger=_log,
        )
        release(lock["id"], success=True, details={
            "matched": getattr(result, "matched_count", 0),
            "deferred": len(getattr(result, "deferred", []) or []),
        })
        st.session_state["m_deferred"] = getattr(result, "deferred", []) or []
        st.session_state["m_decisions"] = {}

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Matched auto", getattr(result, "matched_count", 0))
        m2.metric("Nieuw", getattr(result, "new_count", 0))
        m3.metric("Archief", getattr(result, "archief_count", 0))
        m4.metric("Twijfels", len(st.session_state["m_deferred"]))
        st.success("✅ First pass klaar. Ga hieronder door met de twijfelgevallen.")
    except Exception as e:
        release(lock["id"], success=False, details=str(e))
        st.error(f"❌ Fout: {e}")
        import traceback
        with st.expander("Traceback"):
            st.code(traceback.format_exc())


# ── Pass 2: resolve deferred conflicts ───────────────────────────────────────

deferred = st.session_state.get("m_deferred", [])
decisions: dict = st.session_state.get("m_decisions", {})

if deferred:
    st.divider()
    st.markdown(f"### Stap 2 — Beoordeel twijfelgevallen ({len(deferred)})")

    for idx, conflict in enumerate(deferred):
        key = str(conflict.get("product_id", idx))
        if key in decisions:
            continue

        with st.container(border=True):
            reason = conflict.get("reason", "EAN match maar SKU verschilt")
            st.markdown(f"**Reden:** {reason}")

            c_left, c_right = st.columns(2)
            excel = conflict.get("excel_row", {}) or {}
            hit = conflict.get("shopify_hit", {}) or {}

            def _render(container, title: str, src: dict):
                with container:
                    st.markdown(f"**{title}**")
                    rows = [
                        {"Veld": "SKU", "Waarde": src.get("sku") or "-"},
                        {"Veld": "EAN", "Waarde": src.get("ean") or src.get("ean_shopify") or "-"},
                        {"Veld": "Naam / Title", "Waarde": (src.get("product_name_raw") or src.get("title") or "-")[:60]},
                        {"Veld": "Merk / Vendor", "Waarde": src.get("merk") or src.get("vendor") or "-"},
                        {"Veld": "Status", "Waarde": src.get("status") or "-"},
                    ]
                    st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")

            _render(c_left, "📄 Excel (raw)", excel)
            _render(c_right, "🛍️ Shopify-hit", hit)

            choice = st.radio(
                f"Keuze voor product {key}",
                ["actief", "archief", "nieuw", "skip"],
                index=0,
                key=f"m_choice_{key}",
                horizontal=True,
            )
            if st.button("💾 Opslaan", key=f"m_save_{key}"):
                decisions[key] = choice
                st.session_state["m_decisions"] = decisions
                st.rerun()

    unresolved = [d for d in deferred if str(d.get("product_id")) not in decisions]
    st.caption(f"**{len(decisions)}/{len(deferred)}** beoordeeld.")

    if decisions and not unresolved:
        st.success("✅ Alle twijfelgevallen beoordeeld.")
        if st.button("🚀 Commit alle beslissingen", type="primary"):
            try:
                from execution.match import match_fase
                pending_ids = [int(k) for k in decisions.keys() if str(k).isdigit()]

                def _on_conflict(excel, hit):
                    pid = str(excel.get("id") or excel.get("product_id") or hit.get("product_id"))
                    return decisions.get(pid, "skip")

                res2 = match_fase(fase=fase, ids=pending_ids, on_conflict=_on_conflict)
                st.success(f"✅ {getattr(res2, 'matched_count', 0)} beslissingen doorgevoerd.")
                st.session_state["m_deferred"] = []
                st.session_state["m_decisions"] = {}
            except Exception as e:
                st.error(f"❌ {e}")
