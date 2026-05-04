"""1. Ingest — upload masterdata Excel → seo_products (status=raw)."""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st

from ui.column_detect import EAN_ALIASES, NAAM_ALIASES, SKU_ALIASES, detect_column
from ui.job_lock import acquire, current_holder, release
from ui.layout import explainer, page_header
from ui.supabase_client import current_user_email

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))


page_header("📥 1. Ingest masterdata")

explainer(
    "Upload een leverancier-Excel (Serax / Pottery Pots / Printworks / S&P). "
    "Het script detecteert kolom-mapping, normaliseert EANs, en schrijft naar "
    "`seo_products` met status='raw'. Elke run wordt gelogd in `seo_import_runs`."
)


# ── Inputs ──────────────────────────────────────────────────────────────────

c1, c2 = st.columns([2, 1])

with c1:
    uploaded = st.file_uploader(
        "Kies een Excel (.xlsx)",
        type=["xlsx", "xls"],
        key="ing_file",
    )

with c2:
    fase = st.selectbox("Fase", ["1", "2", "3", "4", "5", "6"], index=3, key="ing_fase")
    merk = st.selectbox("Leverancier / merk", ["Serax", "Pottery Pots", "Printworks", "S&P/Bonbistro"], key="ing_merk")


fotos_file = st.file_uploader(
    "Optioneel: foto-export Excel",
    type=["xlsx"],
    key="ing_fotos",
    help="Losse xlsx met SKU + photo_packshot_N / photo_lifestyle_N kolommen.",
)


# ── Preview kolom-mapping ────────────────────────────────────────────────────

if uploaded is not None:
    try:
        df_preview = pd.read_excel(uploaded, dtype=str)
        uploaded.seek(0)

        col_sku = detect_column(df_preview.columns, SKU_ALIASES)
        col_ean = detect_column(df_preview.columns, EAN_ALIASES)
        col_naam = detect_column(df_preview.columns, NAAM_ALIASES)

        l, r = st.columns(2)
        with l:
            st.markdown("**📄 Eerste 10 rijen uit Excel**")
            st.dataframe(df_preview.head(10), hide_index=True, width="stretch")
            st.caption(f"Totaal: **{len(df_preview)}** rijen · **{len(df_preview.columns)}** kolommen")
        with r:
            st.markdown("**🔍 Gedetecteerde kolom-mapping**")
            mapping_df = pd.DataFrame([
                {"Veld": "SKU", "Gedetecteerde kolom": col_sku or "⚠️ niet gevonden"},
                {"Veld": "EAN", "Gedetecteerde kolom": col_ean or "⚠️ niet gevonden"},
                {"Veld": "Naam", "Gedetecteerde kolom": col_naam or "⚠️ niet gevonden"},
            ])
            st.dataframe(mapping_df, hide_index=True, width="stretch")

            if not col_sku:
                st.warning("⚠️ SKU-kolom niet gedetecteerd — voeg hem handmatig toe via Setup → Masterdata-mapping.")
    except Exception as e:
        st.error(f"Kon Excel niet lezen: {e}")


# ── Action bar ───────────────────────────────────────────────────────────────

st.divider()

a1, a2 = st.columns(2)

with a1:
    dry_run = st.button("🧪 Dry-run (alleen valideren)", width="stretch", disabled=uploaded is None)

with a2:
    run_real = st.button(
        "🚀 Run ingest (schrijft naar Supabase)",
        type="primary",
        width="stretch",
        disabled=uploaded is None,
    )


def _run(dry: bool):
    """Gedeelde logica voor zowel dry-run als echte run."""
    try:
        from execution.ingest import ingest_masterdata  # lazy — afhankelijk van subagent
    except ImportError as e:
        st.error(f"⏳ `ingest_masterdata` nog niet beschikbaar: {e}. "
                 "Subagent 1 is nog bezig met de refactor — probeer over een paar minuten opnieuw.")
        return

    tmpdir = Path(tempfile.mkdtemp(prefix="ing_"))
    excel_path = tmpdir / uploaded.name
    excel_path.write_bytes(uploaded.getvalue())
    fotos_path = None
    if fotos_file is not None:
        fotos_path = tmpdir / fotos_file.name
        fotos_path.write_bytes(fotos_file.getvalue())

    lock = acquire(fase=fase, step="ingest",
                   details={"file": uploaded.name, "user": current_user_email(), "dry_run": dry})
    if lock is None:
        h = current_holder(fase, "ingest")
        st.error(f"🔒 Vergrendeld door {h.get('started_by') if h else '?'} sinds {(h.get('started_at', '') if h else '')[:16]}.")
        return

    prog = st.progress(0.0)
    log_area = st.empty()
    log_lines: list[str] = []

    def _log(msg):
        log_lines.append(str(msg))
        log_area.code("\n".join(log_lines[-30:]))

    def _prog(i, n, msg=""):
        prog.progress(min(max(i / max(n, 1), 0.0), 1.0))
        if msg:
            _log(msg)

    try:
        result = ingest_masterdata(
            file_path=str(excel_path),
            fase=fase,
            fotos_path=str(fotos_path) if fotos_path else None,
            progress=_prog,
            logger=_log,
        )
        release(lock["id"], success=True, details={"inserted": getattr(result, "inserted_count", 0)})
        if dry:
            st.info(f"🧪 Dry-run klaar. Zou **{getattr(result, 'inserted_count', 0)}** rijen invoegen.")
        else:
            st.success(f"✅ Klaar! **{getattr(result, 'inserted_count', 0)}** rijen ingevoegd.")
        warnings = getattr(result, "warnings", []) or []
        if warnings:
            with st.expander(f"⚠️ {len(warnings)} waarschuwing(en)"):
                for w in warnings[:100]:
                    st.caption(f"- {w}")
    except Exception as e:
        release(lock["id"], success=False, details=str(e))
        st.error(f"❌ Fout: {e}")
        import traceback
        with st.expander("Traceback"):
            st.code(traceback.format_exc())


if dry_run:
    _run(dry=True)
if run_real:
    _run(dry=False)
