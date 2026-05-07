"""3. Transform — categoriseer, vertaal, bouw titel + meta. BATCH CAP 200."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

from ui.layout import explainer, page_header
from ui.session import get
from ui.supabase_client import current_user_email, get_supabase
from ui.job_lock import acquire, release, current_holder

# Zorg dat execution/ importable is
_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))
from execution.transform import transform_batch  # noqa: E402


BATCH_CAP = 200


page_header("✨ 3. Transform", fase=get("fase"))

explainer(
    f"Categorie-mapping + Claude-vertaling + titel + meta-description. "
    f"**Batch-cap: max {BATCH_CAP} rijen per klik** — voorkomt massa-Claude calls. "
    f"Leest actieve rules uit `seo_learnings` en past ze toe bovenop de basisregels."
)


# ── Selectie-bron ────────────────────────────────────────────────────────────

from_producten = st.session_state.pop("transform_from_producten", False)
preselected = st.session_state.get("selected_ids", [])

tabs = st.tabs(["🎯 Via selectie", "🔍 Per fase (status=raw)"])

selected_ids: list[int] = []
source_mode = ""

with tabs[0]:
    st.markdown("Gebruikt de rijen die je hebt geselecteerd in Overzichten → Producten.")
    if from_producten or preselected:
        st.success(f"✅ {len(preselected)} rijen geselecteerd uit Producten-overview.")
        selected_ids = preselected
        source_mode = "selection"
        if st.button("🗑️ Selectie leegmaken"):
            st.session_state["selected_ids"] = []
            st.rerun()
    else:
        st.caption("Nog geen selectie. Ga naar Overzichten → Producten, selecteer rijen (max 25), klik 'Stuur naar Transform'.")

with tabs[1]:
    fase = st.selectbox("Fase", ["1", "2", "3", "4", "5", "6"], index=3, key="tr_fase")
    max_to_fetch = st.slider(
        "Hoeveel 'raw' producten laden?",
        1, BATCH_CAP, value=min(BATCH_CAP, 5), step=1,
        help=f"Hard cap: {BATCH_CAP}",
    )
    if st.button("🔍 Preview raw rijen"):
        sb = get_supabase()
        rows = (
            sb.table("seo_products")
            .select("id,sku,product_name_raw,leverancier_category,leverancier_item_cat,kleur_en,materiaal_nl,rrp_stuk_eur,rrp_gb_eur")
            .eq("status", "raw")
            .eq("fase", fase)
            .limit(max_to_fetch)
            .execute()
            .data
            or []
        )
        if not rows:
            st.info(f"Geen raw producten in fase {fase}.")
        else:
            st.session_state["tr_fase_preview"] = rows
            st.session_state["tr_fase_selected"] = fase

    preview = st.session_state.get("tr_fase_preview") or []
    if preview and st.session_state.get("tr_fase_selected") == fase:
        df = pd.DataFrame(preview)
        st.dataframe(df, hide_index=True, width="stretch")
        if st.button(f"✅ Gebruik deze {len(preview)} rijen voor transform", type="primary"):
            selected_ids = [r["id"] for r in preview]
            source_mode = "fase"
            st.session_state["selected_ids"] = selected_ids


# ── Side-by-side preview ─────────────────────────────────────────────────────

if not selected_ids:
    st.info("Kies eerst een bron hierboven.")
    st.stop()

if len(selected_ids) > BATCH_CAP:
    st.error(f"❌ {len(selected_ids)} rijen geselecteerd — max is {BATCH_CAP}. Verklein de selectie.")
    st.stop()


st.divider()
st.markdown(f"### 🔍 Preview — {len(selected_ids)} rijen")

sb = get_supabase()
full_rows = (
    sb.table("seo_products")
    .select("*")
    .in_("id", selected_ids)
    .execute()
    .data
    or []
)

preview_rows = []
for r in full_rows:
    preview_rows.append({
        "SKU": r.get("sku"),
        "Bron-naam": (r.get("product_name_raw") or "")[:50],
        "Bron-cat": f"{r.get('leverancier_category') or ''} / {r.get('leverancier_item_cat') or ''}",
        "Huidig titel": (r.get("product_title_nl") or "")[:40] or "(leeg)",
        "Huidig sub_sub": r.get("sub_subcategorie") or "(leeg)",
        "Status nu": r.get("status") or "(leeg)",
    })

st.dataframe(pd.DataFrame(preview_rows), hide_index=True, width="stretch")

st.caption("⚠️ Elke rij = 1 Claude call voor materiaal/kleur + 1 voor meta description. Denk na voor je draait.")


# ── Action bar ───────────────────────────────────────────────────────────────

c1, c2 = st.columns(2)

with c1:
    if st.button(f"🚀 Transform {len(selected_ids)} rijen", type="primary", width="stretch"):
        fase_for_lock = full_rows[0].get("fase") if full_rows else "?"
        lock = acquire(fase=str(fase_for_lock), step="transform",
                       details={"ids_count": len(selected_ids), "user": current_user_email()})
        if lock is None:
            h = current_holder(str(fase_for_lock), "transform")
            st.error(f"🔒 Vergrendeld door **{h.get('started_by') if h else '?'}** "
                     f"sinds {h.get('started_at', '?')[:16] if h else '?'}. Wacht tot die run klaar is.")
            st.stop()

        prog = st.progress(0.0)
        log_area = st.empty()
        log_lines: list[str] = []

        def _log(msg: str):
            log_lines.append(msg)
            log_area.code("\n".join(log_lines[-25:]))

        def _prog(i: int, n: int, msg: str = ""):
            prog.progress(min(max(i / max(n, 1), 0.0), 1.0))
            if msg:
                _log(msg)

        try:
            result = transform_batch(
                ids=selected_ids,
                progress=_prog,
                logger=_log,
            )
            release(lock["id"], success=True, details={
                "ready": result.ready, "review": result.review, "errors": result.errors,
            })
            st.success(
                f"✅ Klaar! Ready: **{result.ready}** · Review: **{result.review}** · "
                f"Errors: **{result.errors}** · Learnings toegepast: **{result.learnings_applied}**"
            )
            if result.new_filter_values:
                st.warning(f"⚠️ {len(result.new_filter_values)} nieuwe filter-values die nog niet op de website staan:")
                for f in result.new_filter_values[:20]:
                    st.caption(f"- {f}")
                if len(result.new_filter_values) > 20:
                    st.caption(f"... en {len(result.new_filter_values) - 20} meer")
            if result.twijfelgevallen:
                st.info(f"💬 {len(result.twijfelgevallen)} twijfelgevallen — zie Learnings-pagina om mapping toe te voegen.")
                with st.expander("Toon twijfelgevallen"):
                    st.dataframe(pd.DataFrame(result.twijfelgevallen), hide_index=True, width="stretch")
            st.session_state["selected_ids"] = []
        except Exception as e:
            release(lock["id"], success=False, details=str(e))
            st.error(f"❌ Fout tijdens transform: {e}")
            import traceback
            with st.expander("Traceback"):
                st.code(traceback.format_exc())

with c2:
    if st.button("🧪 Dry-run (preview zonder schrijven)", width="stretch"):
        st.info("🚧 Dry-run modus: toont wat er ZOU gebeuren per rij, zonder Supabase writes. In aanbouw.")
