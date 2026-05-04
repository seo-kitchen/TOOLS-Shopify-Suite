"""Audit trail voor Hextom-downloads.

Elke download wordt gelogd in seo_export_files.
Medewerker bevestigt daarna: "Ik heb dit geïmporteerd in Hextom."
Zonder bevestiging blijft de export in de wachtrij zichtbaar.
"""
from __future__ import annotations

from datetime import datetime

import streamlit as st

TABLE = "seo_export_files"


def log_export(
    sb,
    *,
    client_id: str,
    task_type: str,
    fase: str | None,
    file_name: str,
    row_count: int,
    generated_by: str,
) -> dict | None:
    """Schrijf een export-record. Geeft het record terug (met id), of None bij fout."""
    try:
        row = {
            "client_id": client_id,
            "task_type": task_type,
            "fase": fase,
            "file_name": file_name,
            "row_count": row_count,
            "generated_at": datetime.utcnow().isoformat(),
            "generated_by": generated_by,
        }
        res = sb.table(TABLE).insert(row).execute()
        return (res.data or [{}])[0] or None
    except Exception:
        return None


def confirm_applied(sb, export_id: str, confirmed_by: str) -> bool:
    """Markeer een export als bevestigd geïmporteerd in Hextom."""
    try:
        sb.table(TABLE).update({
            "confirmed_at": datetime.utcnow().isoformat(),
            "confirmed_by": confirmed_by,
        }).eq("id", export_id).execute()
        return True
    except Exception:
        return False


def get_pending(sb, client_id: str) -> list[dict]:
    """Haal exports op die nog NIET bevestigd zijn (wachtrij)."""
    try:
        res = (
            sb.table(TABLE)
            .select("*")
            .eq("client_id", client_id)
            .is_("confirmed_at", "null")
            .order("generated_at", desc=True)
            .limit(20)
            .execute()
        )
        return res.data or []
    except Exception:
        return []


def get_history(sb, client_id: str, limit: int = 50) -> list[dict]:
    """Haal recente exports op (bevestigd + onbevestigd)."""
    try:
        res = (
            sb.table(TABLE)
            .select("*")
            .eq("client_id", client_id)
            .order("generated_at", desc=True)
            .limit(limit)
            .execute()
        )
        return res.data or []
    except Exception:
        return []


def render_confirm_widget(sb, export_record: dict, confirmed_by: str) -> bool:
    """Toon de bevestigingswidget onder een download-knop.

    Geeft True terug als de gebruiker bevestigt (en de DB is bijgewerkt).
    """
    export_id = export_record.get("id")
    if not export_id:
        return False

    confirmed = export_record.get("confirmed_at")
    if confirmed:
        st.success(f"✅ Bevestigd geïmporteerd op {str(confirmed)[:16]}")
        return True

    key = f"confirm_{export_id}"
    if st.button("✅ Bevestig: geïmporteerd in Hextom & live in Shopify", key=key, type="primary"):
        if confirm_applied(sb, export_id, confirmed_by):
            st.success("✅ Geregistreerd — export is live in Shopify!")
            st.balloons()
            return True
        else:
            st.error("Kon bevestiging niet opslaan. Probeer opnieuw.")
    return False


def render_pending_banner(sb, client_id: str) -> None:
    """Toon een waarschuwingsbanner als er niet-bevestigde exports zijn.

    Bedoeld voor de home/tab header.
    """
    pending = get_pending(sb, client_id)
    if not pending:
        return

    with st.warning(f"⚠️ **{len(pending)} Hextom-export(s) wachten op bevestiging**"):
        for rec in pending:
            ts = str(rec.get("generated_at", ""))[:16]
            fn = rec.get("file_name", "?")
            task = rec.get("task_type", "?")
            col1, col2 = st.columns([4, 1])
            with col1:
                st.caption(f"📄 `{fn}` · {task} · gegenereerd {ts}")
            with col2:
                key = f"quick_confirm_{rec.get('id')}"
                if st.button("Bevestig ✓", key=key):
                    confirm_applied(sb, rec["id"], client_id)
                    st.rerun()
