"""Tab 5 — Notities & Updates.

Simpel notitieboekje voor het team. Geen taakbeheer, geen tijdregistratie.
Gewoon: schrijf op wat opvalt, wat mist, wat anders moet.

Elke notitie heeft:
  - Tekst (vrij formaat)
  - Label (Foto / Meta / Categorie / Prijs / Overig)
  - Aangemaakt door + datum
  - Open / Opgelost toggle

Opgeslagen in Supabase tabel `seo_notes`.
Fallback: lokaal JSON-bestand als Supabase niet beschikbaar is.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import streamlit as st

from client import get_client_id

import os
from dotenv import load_dotenv
load_dotenv()


def _current_user_email() -> str:
    return os.getenv("USER_EMAIL") or "chef@seokitchen.nl"


@st.cache_resource
def _get_sb():
    from supabase import create_client
    url = os.getenv("SUPABASE_NEW_URL", "")
    key = os.getenv("SUPABASE_NEW_SERVICE_KEY", "") or os.getenv("SUPABASE_NEW_KEY", "")
    if not url or not key:
        return None
    return create_client(url, key)

TABLE = "seo_notes"
FALLBACK_FILE = Path(__file__).parent / ".notes_fallback.json"

LABELS = {
    "foto":       ("📷", "Foto"),
    "meta":       ("🏷️", "Meta"),
    "categorie":  ("📂", "Categorie"),
    "prijs":      ("💶", "Prijs"),
    "overig":     ("📝", "Overig"),
}
LABEL_OPTIES = {v[1]: k for k, v in LABELS.items()}


# ── Supabase helpers ──────────────────────────────────────────────────────────

def _load_notes(client_id: str, alleen_open: bool) -> list[dict]:
    sb = _get_sb()
    if sb:
        try:
            q = sb.table(TABLE).select("*").eq("client_id", client_id).order("aangemaakt_op", desc=True)
            if alleen_open:
                q = q.eq("opgelost", False)
            return q.limit(100).execute().data or []
        except Exception:
            pass
    return _load_fallback(client_id, alleen_open)


def _add_note(client_id: str, tekst: str, label: str, door: str) -> bool:
    sb = _get_sb()
    if sb:
        try:
            sb.table(TABLE).insert({
                "client_id":       client_id,
                "tekst":           tekst.strip(),
                "label":           label,
                "aangemaakt_op":   datetime.utcnow().isoformat(),
                "aangemaakt_door": door,
                "opgelost":        False,
            }).execute()
            return True
        except Exception:
            pass
    return _add_fallback(client_id, tekst, label, door)


def _toggle_opgelost(note_id: str | int, opgelost: bool) -> bool:
    sb = _get_sb()
    if sb:
        try:
            update: dict = {"opgelost": opgelost}
            if opgelost:
                update["opgelost_op"] = datetime.utcnow().isoformat()
            sb.table(TABLE).update(update).eq("id", note_id).execute()
            return True
        except Exception:
            pass
    # fallback: update JSON
    try:
        data = json.loads(FALLBACK_FILE.read_text(encoding="utf-8")) if FALLBACK_FILE.exists() else []
        for n in data:
            if str(n.get("id")) == str(note_id):
                n["opgelost"] = opgelost
        FALLBACK_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        return True
    except Exception:
        return False


def _delete_note(note_id: str | int) -> bool:
    sb = _get_sb()
    if sb:
        try:
            sb.table(TABLE).delete().eq("id", note_id).execute()
            return True
        except Exception:
            pass
    # fallback: verwijder uit JSON
    try:
        data = json.loads(FALLBACK_FILE.read_text(encoding="utf-8")) if FALLBACK_FILE.exists() else []
        data = [n for n in data if str(n.get("id")) != str(note_id)]
        FALLBACK_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        return True
    except Exception:
        return False


# ── Fallback JSON ─────────────────────────────────────────────────────────────

def _load_fallback(client_id: str, alleen_open: bool) -> list[dict]:
    if not FALLBACK_FILE.exists():
        return []
    try:
        data = json.loads(FALLBACK_FILE.read_text(encoding="utf-8"))
        notes = [n for n in data if n.get("client_id") == client_id]
        if alleen_open:
            notes = [n for n in notes if not n.get("opgelost")]
        return sorted(notes, key=lambda n: n.get("aangemaakt_op", ""), reverse=True)
    except Exception:
        return []


def _add_fallback(client_id: str, tekst: str, label: str, door: str) -> bool:
    try:
        data = []
        if FALLBACK_FILE.exists():
            data = json.loads(FALLBACK_FILE.read_text(encoding="utf-8"))
        data.append({
            "id":              f"local_{datetime.utcnow().timestamp()}",
            "client_id":       client_id,
            "tekst":           tekst.strip(),
            "label":           label,
            "aangemaakt_op":   datetime.utcnow().isoformat(),
            "aangemaakt_door": door,
            "opgelost":        False,
        })
        FALLBACK_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        return True
    except Exception:
        return False


# ── Render ────────────────────────────────────────────────────────────────────

def render() -> None:
    st.subheader("Notities")
    st.caption(
        "Schrijf op wat opvalt, wat mist of wat nog moet. "
        "Geen taakbeheer — gewoon een notitieboekje voor het team."
    )

    client_id = get_client_id()
    user = _current_user_email()

    # ── Nieuwe notitie ────────────────────────────────────────────────────────
    with st.form("note_form", clear_on_submit=True):
        st.markdown("**+ Nieuwe notitie**")
        tekst = st.text_area(
            "Notitie",
            placeholder="bijv. 'Serax serie Pias — 20 producten hebben geen packshot foto'",
            height=80,
            label_visibility="collapsed",
        )
        col_label, col_btn = st.columns([3, 1])
        with col_label:
            label_display = st.selectbox(
                "Label",
                options=list(LABEL_OPTIES.keys()),
                index=4,  # Overig als default
                label_visibility="collapsed",
            )
        with col_btn:
            opslaan = st.form_submit_button("💾 Opslaan", use_container_width=True, type="primary")

        if opslaan:
            if not tekst.strip():
                st.warning("Notitie mag niet leeg zijn.")
            else:
                label_key = LABEL_OPTIES[label_display]
                if _add_note(client_id, tekst, label_key, user):
                    st.rerun()
                else:
                    st.error("Kon notitie niet opslaan.")

    st.divider()

    # ── Filter ────────────────────────────────────────────────────────────────
    f1, f2 = st.columns([3, 2])
    with f1:
        alleen_open = st.toggle("Toon alleen openstaande notities", value=True, key="nt_filter")
    with f2:
        label_filter = st.multiselect(
            "Filter op label",
            options=list(LABEL_OPTIES.keys()),
            key="nt_label_filter",
            placeholder="Alle labels",
            label_visibility="collapsed",
        )

    # ── Notities laden ────────────────────────────────────────────────────────
    notes = _load_notes(client_id, alleen_open)

    # Label filter toepassen
    if label_filter:
        actieve_keys = {LABEL_OPTIES[l] for l in label_filter}
        notes = [n for n in notes if n.get("label") in actieve_keys]

    if not notes:
        if alleen_open:
            st.success("✅ Geen openstaande notities!")
        else:
            st.info("Nog geen notities toegevoegd.")
        return

    # ── Notities weergeven ────────────────────────────────────────────────────
    open_count   = sum(1 for n in notes if not n.get("opgelost"))
    opgelost_cnt = sum(1 for n in notes if n.get("opgelost"))

    st.caption(f"{open_count} open · {opgelost_cnt} opgelost  (van {len(notes)} zichtbaar)")

    for note in notes:
        note_id  = note.get("id", "")
        tekst_n  = note.get("tekst", "")
        label    = note.get("label", "overig")
        opgelost = note.get("opgelost", False)
        datum    = str(note.get("aangemaakt_op", ""))[:16]
        door     = note.get("aangemaakt_door", "?").replace("@seokitchen.nl", "")

        icon, label_naam = LABELS.get(label, ("📝", "Overig"))

        # Kaart-stijl
        bg = "#f0f0f0" if opgelost else "#ffffff"
        tekst_kleur = "#888" if opgelost else "#222"
        border = "1px solid #ddd" if opgelost else "1px solid #ccc"

        with st.container():
            col_badge, col_tekst, col_acties = st.columns([1, 6, 2])

            with col_badge:
                st.markdown(
                    f"<div style='background:{'#e0e0e0' if opgelost else '#e8f4fd'};"
                    f"border-radius:6px;padding:6px 8px;text-align:center;"
                    f"font-size:0.8em;color:{'#999' if opgelost else '#1a73e8'}'>"
                    f"{icon}<br>{label_naam}</div>",
                    unsafe_allow_html=True,
                )

            with col_tekst:
                stijl = "text-decoration:line-through;color:#999" if opgelost else f"color:{tekst_kleur}"
                st.markdown(
                    f"<p style='{stijl};margin:4px 0 2px 0'>{tekst_n}</p>"
                    f"<span style='font-size:0.75em;color:#aaa'>{door} · {datum}</span>",
                    unsafe_allow_html=True,
                )

            with col_acties:
                if not opgelost:
                    if st.button("✅ Opgelost", key=f"nt_done_{note_id}", use_container_width=True):
                        _toggle_opgelost(note_id, True)
                        st.rerun()
                else:
                    if st.button("↩️ Heropenen", key=f"nt_reopen_{note_id}", use_container_width=True):
                        _toggle_opgelost(note_id, False)
                        st.rerun()
                if st.button("🗑️", key=f"nt_del_{note_id}", help="Verwijder notitie"):
                    _delete_note(note_id)
                    st.rerun()

        st.markdown("<hr style='margin:4px 0;border:none;border-top:1px solid #eee'>",
                    unsafe_allow_html=True)
