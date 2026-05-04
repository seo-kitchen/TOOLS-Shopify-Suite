"""Tab 2 — Prijzen bijwerken.

Upload nieuwe leverancier prijslijst → diff → Hextom Excel downloaden.
Simpele flow, max twee minuten werk.

Past de giftbox-regel toe: qty>1 → altijd giftbox-prijs, NOOIT stuksprijs.
"""
from __future__ import annotations

import tempfile
from pathlib import Path

import streamlit as st

from ui.supabase_client import current_user_email, get_supabase
from client import get_client_id
from export_log import log_export, render_confirm_widget, render_pending_banner


def render() -> None:
    st.subheader("💶 Prijzen bijwerken")
    st.caption(
        "Upload de nieuwe leverancier prijslijst. "
        "Systeem toont een diff (oud → nieuw) en genereert een Hextom bulk-price Excel. "
        "Hextom is de enige manier om prijzen in Shopify bij te werken."
    )

    sb = get_supabase()
    client_id = get_client_id()
    render_pending_banner(sb, client_id)

    st.divider()

    # ── Upload ────────────────────────────────────────────────────────────────
    uploaded = st.file_uploader("Nieuwe prijslijst (.xlsx)", type=["xlsx", "xls"], key="pr_file")

    if uploaded is None:
        st.info("Upload een prijslijst om te beginnen.")
        return

    output_dir = Path("./exports")
    output_dir.mkdir(parents=True, exist_ok=True)

    col1, col2 = st.columns(2)
    with col1:
        dry = st.button("🧪 Dry-run (alleen rapporteren, geen DB-writes)", key="pr_dry",
                        use_container_width=True)
    with col2:
        run = st.button("🚀 Run — schrijft naar DB + genereert Hextom Excel", key="pr_run",
                        type="primary", use_container_width=True)

    if not dry and not run:
        return

    try:
        from execution.update_prices import run_price_update
    except ImportError as e:
        st.error(f"update_prices script niet beschikbaar: {e}")
        return

    tmpdir = Path(tempfile.mkdtemp(prefix="pr_"))
    path = tmpdir / uploaded.name
    path.write_bytes(uploaded.getvalue())

    prog = st.progress(0.0)
    log_area = st.empty()
    log_lines: list[str] = []

    def _log(msg: str) -> None:
        log_lines.append(str(msg))
        log_area.code("\n".join(log_lines[-30:]))

    def _prog(i: int, n: int, msg: str = "") -> None:
        prog.progress(min(max(i / max(n, 1), 0.0), 1.0))
        if msg:
            _log(msg)

    try:
        result = run_price_update(
            file_path=str(path),
            dry_run=dry,
            output_dir=str(output_dir),
            progress=_prog,
            logger=_log,
        )

        matched = getattr(result, "matched_count", 0)
        updated = getattr(result, "updated_count", 0)
        not_found = len(getattr(result, "not_found_rows", []) or [])

        m1, m2, m3 = st.columns(3)
        m1.metric("Matched SKUs", matched)
        m2.metric("Bijgewerkt", updated)
        m3.metric("Niet gevonden", not_found)

        if dry:
            st.info("🧪 Dry-run klaar — geen DB-writes, geen Excel gegenereerd.")
            return

        # ── Downloads + bevestiging ───────────────────────────────────────
        hex_path = Path(getattr(result, "hextom_xlsx_path", "") or "")
        crx_path = Path(getattr(result, "crx_xlsx_path", "") or "")

        user = current_user_email()
        rec = None

        if hex_path.exists():
            st.markdown("### 📥 Hextom bulk-price Excel")
            st.caption(
                "Download dit bestand, importeer in Hextom → Bulk Edit → Import. "
                "Hextom toont een preview — controleer de prijzen vóór je op Apply klikt."
            )
            data = hex_path.read_bytes()
            st.download_button(
                f"📥 Download: {hex_path.name}",
                data=data,
                file_name=hex_path.name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="pr_dl_hex",
                use_container_width=True,
                type="primary",
            )
            rec = log_export(sb, client_id=client_id, task_type="prijsupdate",
                             fase=None, file_name=hex_path.name,
                             row_count=updated, generated_by=user)

        if crx_path.exists():
            with st.expander("📊 CRX overview (intern)"):
                st.download_button(
                    f"📥 {crx_path.name}",
                    data=crx_path.read_bytes(),
                    file_name=crx_path.name,
                    key="pr_dl_crx",
                )

        nf = getattr(result, "not_found_rows", []) or []
        if nf:
            with st.expander(f"⚠️ {len(nf)} SKUs niet gevonden in DB"):
                st.caption("Deze SKUs staan in de prijslijst maar niet in Supabase.")
                import pandas as pd
                st.dataframe(pd.DataFrame(nf[:100]), hide_index=True, use_container_width=True)

        # Bevestiging
        if rec:
            st.divider()
            st.markdown("### ✅ Bevestig importatie")
            st.caption("Klik hieronder nadat je het Hextom bestand hebt geïmporteerd en de prijzen live zijn in Shopify.")
            render_confirm_widget(sb, rec, user)

    except Exception as e:
        st.error(f"❌ Fout: {e}")
        import traceback
        with st.expander("Traceback"):
            st.code(traceback.format_exc())
