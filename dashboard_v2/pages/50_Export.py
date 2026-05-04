"""5. Export — genereer Hextom bulk Excel (nieuw + archief)."""
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


page_header("📤 5. Export")

explainer(
    "Bouwt de Hextom bulk-edit Excel voor de huidige fase. Splitst automatisch "
    "in `Shopify_Nieuw_fase{N}.xlsx` (nieuwe producten) en `Shopify_Archief_fase{N}.xlsx` "
    "(reactivaties). Optioneel: 1-file versie met Nieuw + Archief + Analyse tabs."
)


c1, c2, c3 = st.columns([1, 2, 2])
with c1:
    fase = st.selectbox("Fase", ["1", "2", "3", "4", "5", "6"], index=3, key="exp_fase")
with c2:
    output_dir = st.text_input("Output folder", value="./exports", key="exp_dir")
with c3:
    export_type = st.radio(
        "Formaat",
        ["Hextom strict (2 files)", "Standaard 3-tab"],
        key="exp_type",
        horizontal=True,
    )


@st.cache_data(ttl=30, show_spinner=False)
def _counts(fase: str) -> dict:
    sb = get_supabase()
    ready = (
        sb.table("seo_products").select("id,status_shopify", count="exact")
        .eq("fase", fase).eq("status", "ready").execute()
    )
    nieuw = sum(1 for r in (ready.data or []) if r.get("status_shopify") == "nieuw")
    archief = sum(1 for r in (ready.data or []) if r.get("status_shopify") == "archief")
    actief = sum(1 for r in (ready.data or []) if r.get("status_shopify") == "actief")
    return {"total": ready.count or 0, "nieuw": nieuw, "archief": archief, "actief": actief}


try:
    c = _counts(fase)
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Ready totaal", c["total"])
    m2.metric("Nieuw", c["nieuw"])
    m3.metric("Archief → heractivatie", c["archief"])
    m4.metric("Actief (update)", c["actief"])
except Exception as e:
    st.error(f"Kon counts niet ophalen: {e}")


st.divider()

if st.button(f"🚀 Run export fase {fase}", type="primary"):
    try:
        if export_type.startswith("Hextom"):
            from execution.export import export_fase as _run_export
        else:
            from execution.export_standaard import export_standaard as _run_export
    except ImportError as e:
        st.error(f"⏳ Export functie niet beschikbaar: {e}")
        st.stop()

    lock = acquire(fase=fase, step="export", details={"type": export_type, "user": current_user_email()})
    if lock is None:
        h = current_holder(fase, "export")
        st.error(f"🔒 Vergrendeld door {h.get('started_by') if h else '?'}.")
        st.stop()

    prog = st.progress(0.0)
    log_area = st.empty()
    log_lines: list[str] = []
    def _log(msg): log_lines.append(str(msg)); log_area.code("\n".join(log_lines[-30:]))
    def _prog(i, n, msg=""): prog.progress(min(max(i / max(n, 1), 0.0), 1.0)); msg and _log(msg)

    try:
        # Ensure output dir exists
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        result = _run_export(fase=fase, output_dir=output_dir, progress=_prog, logger=_log)
        release(lock["id"], success=True, details={"nieuw": getattr(result, "nieuw_count", 0)})
        st.success("✅ Export klaar!")

        if export_type.startswith("Hextom"):
            n_path = Path(getattr(result, "nieuw_xlsx_path", "") or "")
            a_path = Path(getattr(result, "archief_xlsx_path", "") or "")
            col1, col2 = st.columns(2)
            if n_path.exists():
                with col1:
                    st.metric("Nieuw", getattr(result, "nieuw_count", 0))
                    st.download_button(f"📥 {n_path.name}", data=n_path.read_bytes(),
                                       file_name=n_path.name,
                                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            if a_path.exists():
                with col2:
                    st.metric("Archief", getattr(result, "archief_count", 0))
                    st.download_button(f"📥 {a_path.name}", data=a_path.read_bytes(),
                                       file_name=a_path.name,
                                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else:
            xlsx = Path(getattr(result, "xlsx_path", "") or "")
            if xlsx.exists():
                st.metric("Nieuw / Archief", f"{getattr(result, 'nieuw_count', 0)} / {getattr(result, 'archief_count', 0)}")
                st.download_button(f"📥 {xlsx.name}", data=xlsx.read_bytes(),
                                   file_name=xlsx.name,
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    except Exception as e:
        release(lock["id"], success=False, details=str(e))
        st.error(f"❌ Fout: {e}")
        import traceback
        with st.expander("Traceback"):
            st.code(traceback.format_exc())
