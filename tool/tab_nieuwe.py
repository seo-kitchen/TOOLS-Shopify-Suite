"""Tab 1 — Nieuwe producten toevoegen.

Vijf stappen als uitklapbare secties. Elke stap toont live-status uit Supabase
en roept de bestaande execution-scripts aan. Hextom-download is altijd de
laatste stap; daarna bevestigen.

Bestaande dashboard_v2/ pagina's worden NIET gewijzigd — dit is een aparte
wrapper die dezelfde execution-scripts aanroept.
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st

# Importpaden worden gezet door app.py (dashboard_v2/ en root zijn in sys.path)
from ui.supabase_client import current_user_email, get_supabase
from ui.job_lock import acquire, release, current_holder

from client import get_client_id
from export_log import log_export, render_confirm_widget


# ── Helpers ───────────────────────────────────────────────────────────────────

LEVERANCIERS = ["Serax", "Pottery Pots", "Printworks", "S&P/Bonbistro"]
FASES = ["1", "2", "3", "4", "5", "6"]

BATCH_CAP = 25


def _status_counts(fase: str) -> dict:
    """Tel producten per status voor deze fase."""
    try:
        sb = get_supabase()
        res = sb.table("seo_products").select("status", count="exact").eq("fase", fase).execute()
        counts: dict[str, int] = {}
        for row in (res.data or []):
            s = row.get("status", "?")
            counts[s] = counts.get(s, 0) + 1
        return counts
    except Exception:
        return {}


def _status_badge(counts: dict) -> str:
    if not counts:
        return "geen data"
    parts = [f"{v} {k}" for k, v in sorted(counts.items())]
    return " · ".join(parts)


def _log_lines_widget():
    """Geeft een progress-bar, log-container en progress/log callbacks terug."""
    prog = st.progress(0.0)
    log_area = st.empty()
    lines: list[str] = []

    def _log(msg: str) -> None:
        lines.append(str(msg))
        log_area.code("\n".join(lines[-30:]))

    def _prog(i: int, n: int, msg: str = "") -> None:
        prog.progress(min(max(i / max(n, 1), 0.0), 1.0))
        if msg:
            _log(msg)

    return prog, log_area, _log, _prog


# ── Render ────────────────────────────────────────────────────────────────────

def render() -> None:
    st.subheader("📦 Nieuwe producten toevoegen")
    st.caption(
        "Volg de vijf stappen van boven naar beneden. "
        "Elke stap toont de live status vanuit Supabase. "
        "Hextom Excel is altijd de laatste stap."
    )

    # Fase + leverancier bovenaan — persistent in session_state
    c1, c2 = st.columns(2)
    with c1:
        fase = st.selectbox("Fase", FASES, index=3, key="np_fase")
    with c2:
        leverancier = st.selectbox("Leverancier", LEVERANCIERS, key="np_lev")

    counts = _status_counts(fase)

    # Voortgangsbalk
    statussen = ["raw", "ready", "review", "exported"]
    klaar = sum(counts.get(s, 0) for s in ("ready", "exported"))
    totaal = sum(counts.values()) or 1
    st.progress(klaar / totaal, text=f"Voortgang fase {fase}: {_status_badge(counts)}")

    st.divider()

    # ── Stap 1: Ingest ───────────────────────────────────────────────────────
    raw_count = counts.get("raw", 0)
    stap1_icon = "✅" if raw_count > 0 else "1️⃣"
    with st.expander(f"{stap1_icon} Stap 1 — Upload & ingest leverancier Excel", expanded=(raw_count == 0)):
        st.caption(
            "Upload de masterdata Excel van de leverancier. "
            "Het script detecteert de kolomindeling automatisch en schrijft naar "
            "`seo_products` met status=raw."
        )

        col_up, col_fo = st.columns(2)
        with col_up:
            uploaded = st.file_uploader("Masterdata Excel (.xlsx)", type=["xlsx", "xls"], key="np_ing_file")
        with col_fo:
            fotos_file = st.file_uploader(
                "Optioneel: foto-export Excel",
                type=["xlsx"],
                key="np_ing_fotos",
                help="Excel met SKU + photo_packshot_N kolommen.",
            )

        if uploaded:
            try:
                df_prev = pd.read_excel(uploaded, dtype=str, nrows=10)
                uploaded.seek(0)
                left, right = st.columns(2)
                with left:
                    st.markdown("**Eerste 10 rijen**")
                    st.dataframe(df_prev, hide_index=True, use_container_width=True)
                with right:
                    st.markdown("**Gedetecteerde kolommen**")
                    st.caption(f"{len(df_prev.columns)} kolommen gevonden")
                    st.write(list(df_prev.columns))
            except Exception as e:
                st.warning(f"Kon preview niet laden: {e}")

        a1, a2 = st.columns(2)
        with a1:
            dry = st.button("🧪 Dry-run", key="np_ing_dry", disabled=uploaded is None)
        with a2:
            run = st.button("🚀 Run ingest", key="np_ing_run", type="primary", disabled=uploaded is None)

        if dry or run:
            try:
                from execution.ingest import ingest_masterdata
            except ImportError as e:
                st.error(f"Ingest script niet beschikbaar: {e}")
                st.stop()

            tmpdir = Path(tempfile.mkdtemp(prefix="np_ing_"))
            excel_path = tmpdir / uploaded.name
            excel_path.write_bytes(uploaded.getvalue())
            fotos_path = None
            if fotos_file:
                fotos_path = tmpdir / fotos_file.name
                fotos_path.write_bytes(fotos_file.getvalue())

            lock = acquire(fase=fase, step="ingest",
                           details={"user": current_user_email(), "dry": dry})
            if lock is None:
                h = current_holder(fase, "ingest")
                st.error(f"🔒 Vergrendeld door {h.get('started_by') if h else '?'}")
                st.stop()

            _, __, _log, _prog = _log_lines_widget()
            try:
                result = ingest_masterdata(
                    file_path=str(excel_path),
                    fase=fase,
                    fotos_path=str(fotos_path) if fotos_path else None,
                    progress=_prog,
                    logger=_log,
                )
                release(lock["id"], success=True)
                cnt = getattr(result, "inserted_count", 0)
                if dry:
                    st.info(f"🧪 Dry-run: zou {cnt} rijen invoegen.")
                else:
                    st.success(f"✅ {cnt} rijen ingevoegd in fase {fase}.")
                warns = getattr(result, "warnings", []) or []
                if warns:
                    with st.expander(f"⚠️ {len(warns)} waarschuwingen"):
                        for w in warns[:50]:
                            st.caption(f"- {w}")
                st.rerun()
            except Exception as e:
                release(lock["id"], success=False, details=str(e))
                st.error(f"❌ {e}")

    # ── Stap 2: Match ────────────────────────────────────────────────────────
    matched_count = sum(counts.get(s, 0) for s in ("ready", "review", "exported"))
    stap2_icon = "✅" if matched_count > 0 else ("2️⃣" if raw_count > 0 else "⏳")
    with st.expander(f"{stap2_icon} Stap 2 — Match aan Shopify index", expanded=(raw_count > 0 and matched_count == 0)):
        st.caption(
            "Matcht SKU/EAN aan de Shopify index (`seo_shopify_index`). "
            "Toont side-by-side welke producten gevonden zijn en welke niet."
        )

        if raw_count == 0:
            st.info("Voer eerst Stap 1 uit om producten in te laden.")
        else:
            # Side-by-side preview van ongematchte producten
            try:
                sb = get_supabase()
                raws = (
                    sb.table("seo_products")
                    .select("id,sku,ean,naam,status,status_shopify,shopify_product_id")
                    .eq("fase", fase)
                    .eq("status", "raw")
                    .limit(100)
                    .execute()
                    .data or []
                )
                if raws:
                    df_raw = pd.DataFrame(raws)
                    st.markdown(f"**{len(raws)} producten met status=raw**")
                    cols_show = [c for c in ("sku", "ean", "naam", "status_shopify", "shopify_product_id") if c in df_raw.columns]
                    st.dataframe(df_raw[cols_show], hide_index=True, use_container_width=True)
            except Exception as e:
                st.warning(f"Kon producten niet ophalen: {e}")

            if st.button("🔗 Run match", key="np_match_run", type="primary"):
                try:
                    from execution.match import match_fase
                except ImportError as e:
                    st.error(f"Match script niet beschikbaar: {e}")
                    st.stop()

                lock = acquire(fase=fase, step="match", details={"user": current_user_email()})
                if lock is None:
                    h = current_holder(fase, "match")
                    st.error(f"🔒 Vergrendeld door {h.get('started_by') if h else '?'}")
                    st.stop()

                _, __, _log, _prog = _log_lines_widget()
                try:
                    result = match_fase(fase=fase, progress=_prog, logger=_log)
                    release(lock["id"], success=True)
                    st.success(
                        f"✅ Match klaar — "
                        f"{getattr(result, 'matched_count', '?')} gematcht, "
                        f"{getattr(result, 'new_count', '?')} nieuw, "
                        f"{getattr(result, 'unmatched_count', '?')} niet gevonden."
                    )
                    st.rerun()
                except Exception as e:
                    release(lock["id"], success=False, details=str(e))
                    st.error(f"❌ {e}")

    # ── Stap 3: Verrijken (Transform) ────────────────────────────────────────
    ready_count = counts.get("ready", 0) + counts.get("review", 0)
    stap3_icon = "✅" if ready_count > 0 else ("3️⃣" if matched_count > 0 else "⏳")
    with st.expander(f"{stap3_icon} Stap 3 — Verrijken (categorie · vertaling · titel · meta)", expanded=False):
        st.caption(
            f"Verrijkt producten in batches van max {BATCH_CAP}. "
            "Leest actieve learnings uit `seo_learnings`. "
            "Selecteer hoeveel je nu wilt verwerken."
        )

        remaining = raw_count
        if remaining == 0:
            st.info("Geen producten met status=raw. Stap 2 al uitgevoerd of nog te doen.")
        else:
            st.metric("Te verrijken", remaining)
            batch_size = st.slider("Batch grootte", 1, BATCH_CAP, value=min(BATCH_CAP, remaining), key="np_tr_batch")

            if st.button(f"✨ Verrijk {batch_size} producten", key="np_tr_run", type="primary"):
                try:
                    from execution.transform import transform_batch
                except ImportError as e:
                    st.error(f"Transform script niet beschikbaar: {e}")
                    st.stop()

                lock = acquire(fase=fase, step="transform", details={"user": current_user_email(), "batch": batch_size})
                if lock is None:
                    h = current_holder(fase, "transform")
                    st.error(f"🔒 Vergrendeld door {h.get('started_by') if h else '?'}")
                    st.stop()

                _, __, _log, _prog = _log_lines_widget()
                try:
                    result = transform_batch(fase=fase, limit=batch_size, progress=_prog, logger=_log)
                    release(lock["id"], success=True)
                    done = getattr(result, "transformed_count", batch_size)
                    st.success(f"✅ {done} producten verrijkt.")
                    st.rerun()
                except Exception as e:
                    release(lock["id"], success=False, details=str(e))
                    st.error(f"❌ {e}")

    # ── Stap 4: Valideren ────────────────────────────────────────────────────
    stap4_icon = "✅" if ready_count > 0 else ("4️⃣" if matched_count > 0 else "⏳")
    with st.expander(f"{stap4_icon} Stap 4 — Valideren", expanded=False):
        st.caption(
            "Checkt verplichte velden, meta-lengtes, dubbele EANs. "
            "Auto-fix voor decimals en te lange meta-teksten."
        )

        if ready_count == 0 and raw_count == 0:
            st.info("Voer eerst Stap 3 uit.")
        else:
            st.metric("Klaar voor validatie (ready + review)", ready_count)

            if st.button("✅ Run validatie", key="np_val_run", type="primary", disabled=(ready_count == 0)):
                try:
                    from execution.validate import validate_fase
                except ImportError as e:
                    st.error(f"Validate script niet beschikbaar: {e}")
                    st.stop()

                lock = acquire(fase=fase, step="validate", details={"user": current_user_email()})
                if lock is None:
                    h = current_holder(fase, "validate")
                    st.error(f"🔒 Vergrendeld door {h.get('started_by') if h else '?'}")
                    st.stop()

                _, __, _log, _prog = _log_lines_widget()
                try:
                    result = validate_fase(fase=fase, autofix=True, progress=_prog, logger=_log)
                    release(lock["id"], success=True)
                    errors = getattr(result, "error_count", 0)
                    fixed = getattr(result, "fixed_count", 0)
                    st.success(f"✅ Validatie klaar — {errors} fouten, {fixed} auto-fixes.")
                    if errors > 0:
                        st.warning("Ga naar de bestaande Validate pagina (dashboard_v2) voor detail per product.")
                    st.rerun()
                except Exception as e:
                    release(lock["id"], success=False, details=str(e))
                    st.error(f"❌ {e}")

    # ── Stap 5: Hextom export ────────────────────────────────────────────────
    export_ready = counts.get("ready", 0)
    stap5_icon = "📥" if export_ready > 0 else ("5️⃣" if ready_count > 0 else "⏳")
    with st.expander(f"{stap5_icon} Stap 5 — Download Hextom Excel", expanded=(export_ready > 0)):
        st.caption(
            "Genereert de Hextom bulk-edit Excel. "
            "Download, importeer in Hextom, pas toe in Shopify, en bevestig hieronder."
        )

        if export_ready == 0:
            st.info(f"Geen producten met status=ready in fase {fase}. Voer Stap 4 eerst uit.")
        else:
            m1, m2 = st.columns(2)
            m1.metric("Klaar voor export", export_ready)
            m2.metric("Fase", fase)

            output_dir = Path("./exports")
            output_dir.mkdir(parents=True, exist_ok=True)

            if st.button(f"🚀 Genereer Hextom Excel voor fase {fase}", key="np_exp_run", type="primary"):
                try:
                    from execution.export import export_fase
                except ImportError as e:
                    st.error(f"Export script niet beschikbaar: {e}")
                    st.stop()

                lock = acquire(fase=fase, step="export", details={"user": current_user_email()})
                if lock is None:
                    h = current_holder(fase, "export")
                    st.error(f"🔒 Vergrendeld door {h.get('started_by') if h else '?'}")
                    st.stop()

                _, __, _log, _prog = _log_lines_widget()
                try:
                    result = export_fase(fase=fase, output_dir=str(output_dir), progress=_prog, logger=_log)
                    release(lock["id"], success=True)

                    nieuw_path = Path(getattr(result, "nieuw_xlsx_path", "") or "")
                    arch_path = Path(getattr(result, "archief_xlsx_path", "") or "")
                    nieuw_count = getattr(result, "nieuw_count", 0)
                    arch_count = getattr(result, "archief_count", 0)

                    st.success(f"✅ Export klaar: {nieuw_count} nieuw · {arch_count} archief")

                    sb = get_supabase()
                    client_id = get_client_id()
                    user = current_user_email()

                    col1, col2 = st.columns(2)
                    rec_n = rec_a = None

                    if nieuw_path.exists():
                        with col1:
                            st.metric("Nieuw", nieuw_count)
                            data_n = nieuw_path.read_bytes()
                            st.download_button(
                                f"📥 {nieuw_path.name}",
                                data=data_n,
                                file_name=nieuw_path.name,
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="np_dl_n",
                            )
                            rec_n = log_export(sb, client_id=client_id, task_type="nieuwe_producten",
                                               fase=fase, file_name=nieuw_path.name,
                                               row_count=nieuw_count, generated_by=user)

                    if arch_path.exists():
                        with col2:
                            st.metric("Archief/heractivatie", arch_count)
                            data_a = arch_path.read_bytes()
                            st.download_button(
                                f"📥 {arch_path.name}",
                                data=data_a,
                                file_name=arch_path.name,
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="np_dl_a",
                            )
                            rec_a = log_export(sb, client_id=client_id, task_type="nieuwe_producten_archief",
                                               fase=fase, file_name=arch_path.name,
                                               row_count=arch_count, generated_by=user)

                    # Sla record-ids op voor bevestiging
                    if rec_n:
                        st.session_state["np_export_rec_n"] = rec_n
                    if rec_a:
                        st.session_state["np_export_rec_a"] = rec_a

                except Exception as e:
                    release(lock["id"], success=False, details=str(e))
                    st.error(f"❌ {e}")
                    import traceback
                    with st.expander("Traceback"):
                        st.code(traceback.format_exc())

            # Bevestiging (toont ook na rerun als record in session_state staat)
            rec_n = st.session_state.get("np_export_rec_n")
            rec_a = st.session_state.get("np_export_rec_a")
            if rec_n or rec_a:
                st.divider()
                st.markdown("### ✅ Bevestig importatie in Hextom")
                st.caption("Klik pas op Bevestig als je de bestanden daadwerkelijk hebt geïmporteerd in Shopify via Hextom.")
                sb = get_supabase()
                user = current_user_email()
                if rec_n:
                    render_confirm_widget(sb, rec_n, user)
                if rec_a:
                    render_confirm_widget(sb, rec_a, user)
