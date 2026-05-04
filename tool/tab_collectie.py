"""Tab 3 — Collectie SEO teksten.

Laadt collecties uit Shopify (via API) of Supabase, toont huidige meta
title/description, genereert suggesties via Claude, laat side-by-side bewerken,
en exporteert een Excel die klaar is voor handmatige import of Hextom.

SEO regels (uit memory):
  - Meta title:       ≤ 58 tekens  (focus-keyword + USP + "je"-vorm)
  - Meta description: 120–155 tekens
"""
from __future__ import annotations

import io
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from ui.supabase_client import current_user_email, get_supabase
from client import get_client_id
from export_log import log_export, render_confirm_widget, render_pending_banner

TITLE_MAX = 58
DESC_MIN = 120
DESC_MAX = 155


# ── Helpers ───────────────────────────────────────────────────────────────────

def _kleur(lengte: int, min_: int, max_: int) -> str:
    if lengte == 0:
        return "🔴"
    if min_ <= lengte <= max_:
        return "🟢"
    return "🟠"


def _shopify_read():
    import importlib.util
    p = Path(__file__).parent.parent / "execution" / "shopify_read.py"
    spec = importlib.util.spec_from_file_location("shopify_read_mod", p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _load_from_shopify() -> pd.DataFrame:
    """Haalt collecties op via Shopify REST API (shopify_read.py)."""
    try:
        mod = _shopify_read()
        token = mod.get_token()
        df = mod.haal_collecties(token)
        return df
    except Exception as e:
        st.warning(f"Shopify API niet bereikbaar: {e}")
        return pd.DataFrame()


def _load_from_supabase() -> pd.DataFrame:
    """Haalt collecties op uit seo_website_collections."""
    try:
        sb = get_supabase()
        res = sb.table("seo_website_collections").select("*").execute()
        data = res.data or []
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as e:
        st.warning(f"Supabase collecties niet beschikbaar: {e}")
        return pd.DataFrame()


def _load_meta_audit() -> dict[str, dict]:
    """Laadt bestaande meta-data uit shopify_meta_audit (product handle → meta)."""
    try:
        sb = get_supabase()
        res = sb.table("shopify_meta_audit").select("handle,meta_title,meta_description").execute()
        return {r["handle"]: r for r in (res.data or []) if r.get("handle")}
    except Exception:
        return {}


def _seo_score(title: str, desc: str) -> str:
    t_len = len(str(title or ""))
    d_len = len(str(desc or ""))
    t_ok = 0 < t_len <= TITLE_MAX
    d_ok = DESC_MIN <= d_len <= DESC_MAX
    if t_ok and d_ok:
        return "🟢 OK"
    if t_len == 0 or d_len == 0:
        return "🔴 Ontbreekt"
    return "🟠 Verbetering"


def _generate_meta_for_collection(collection_title: str, handle: str) -> tuple[str, str]:
    """Genereert meta title + description via Claude voor één collectie."""
    try:
        from anthropic import Anthropic
        import os
        client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

        prompt = f"""Je schrijft SEO meta-teksten voor een Nederlandse interieur-webshop (interieur-shop.nl).

Collectie: {collection_title}
URL-handle: {handle}

Schrijf een meta title en meta description voor deze collectiepagina.

Regels:
- Meta title: maximaal {TITLE_MAX} tekens. Gebruik "je" (niet "u"). Verwerk het hoofdzoekwoord.
  Format: "[Categorie] | Interieur Shop" of "[Zoekwoord] – Gratis verzending v.a. €75"
- Meta description: {DESC_MIN}–{DESC_MAX} tekens. Noem 1-2 USPs (gratis verzending v.a. €75,
  voor 16u = morgen in huis, 9.0 klantbeoordeling). Eindig met een call-to-action.

Geef ALLEEN de twee teksten terug, in dit formaat:
TITLE: [meta title hier]
DESC: [meta description hier]"""

        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()
        title = ""
        desc = ""
        for line in text.splitlines():
            if line.startswith("TITLE:"):
                title = line.replace("TITLE:", "").strip()
            elif line.startswith("DESC:"):
                desc = line.replace("DESC:", "").strip()
        return title, desc
    except Exception as e:
        return "", f"Fout: {e}"


def _to_hextom_excel(rows: list[dict]) -> bytes:
    """Zet collectie-meta rijen om naar een Excel-bestand."""
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Collectie SEO")
    return buf.getvalue()


# ── Render ────────────────────────────────────────────────────────────────────

def render() -> None:
    st.subheader("🌐 Collectie SEO teksten")
    st.caption(
        "Bekijk en verbeter de meta title + description van collectiepagina's. "
        f"Regels: title ≤{TITLE_MAX} tekens · description {DESC_MIN}–{DESC_MAX} tekens. "
        "Exporteer naar Excel voor handmatige import in Shopify."
    )

    sb = get_supabase()
    client_id = get_client_id()
    render_pending_banner(sb, client_id)

    st.divider()

    # ── Bronkeuze ─────────────────────────────────────────────────────────────
    bron = st.radio(
        "Laad collecties vanuit",
        ["Shopify API (live)", "Supabase (gecached)"],
        horizontal=True,
        key="col_bron",
    )

    if st.button("🔄 Laad collecties", key="col_load"):
        if bron.startswith("Shopify"):
            df = _load_from_shopify()
        else:
            df = _load_from_supabase()

        if df.empty:
            st.error("Geen collecties gevonden.")
            return

        st.session_state["col_df"] = df
        st.session_state["col_edits"] = {}

    df: pd.DataFrame | None = st.session_state.get("col_df")
    if df is None or df.empty:
        st.info("Klik op 'Laad collecties' om te beginnen.")
        return

    # ── Filters ───────────────────────────────────────────────────────────────
    f1, f2 = st.columns(2)
    with f1:
        alleen_problemen = st.checkbox("Toon alleen 🔴/🟠 collecties", value=True, key="col_filter_probs")
    with f2:
        zoek = st.text_input("Zoek op naam", key="col_zoek", placeholder="bijv. vazen")

    # Haal meta audit data op als beschikbaar
    meta_audit = _load_meta_audit()

    # Bepaal welke kolommen beschikbaar zijn
    titel_col = next((c for c in ("titel", "title", "name") if c in df.columns), None)
    handle_col = next((c for c in ("handle",) if c in df.columns), None)

    if not titel_col:
        st.error("Geen titel-kolom gevonden in collecties-data. Controleer de bron.")
        return

    # Verrijkt de dataframe met huidige meta (uit audit of leeg)
    rows_display = []
    for _, row in df.iterrows():
        title_val = str(row.get(titel_col, "") or "")
        handle_val = str(row.get(handle_col, "") or "") if handle_col else title_val.lower().replace(" ", "-")
        gepubl = str(row.get("gepubliceerd", "ja") or "ja")

        audit = meta_audit.get(handle_val, {})
        meta_t = str(audit.get("meta_title", "") or "")
        meta_d = str(audit.get("meta_description", "") or "")

        # Check voor lokale bewerkingen
        edits = st.session_state.get("col_edits", {})
        edit = edits.get(handle_val, {})
        meta_t = edit.get("title", meta_t)
        meta_d = edit.get("desc", meta_d)

        score = _seo_score(meta_t, meta_d)

        rows_display.append({
            "_handle": handle_val,
            "_titel": title_val,
            "_gepubl": gepubl,
            "_meta_title": meta_t,
            "_meta_desc": meta_d,
            "_score": score,
            "_t_len": len(meta_t),
            "_d_len": len(meta_d),
        })

    df_display = pd.DataFrame(rows_display)

    if zoek:
        mask = df_display["_titel"].str.lower().str.contains(zoek.lower(), na=False)
        df_display = df_display[mask]

    if alleen_problemen:
        df_display = df_display[df_display["_score"] != "🟢 OK"]

    # ── Samenvatting ──────────────────────────────────────────────────────────
    totaal = len(df_display)
    rood = (df_display["_score"] == "🔴 Ontbreekt").sum()
    oranje = (df_display["_score"] == "🟠 Verbetering").sum()
    groen = (df_display["_score"] == "🟢 OK").sum()

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Getoond", totaal)
    m2.metric("🔴 Ontbreekt", int(rood))
    m3.metric("🟠 Verbetering", int(oranje))
    m4.metric("🟢 OK", int(groen))

    st.divider()

    # ── Bulk genereer ─────────────────────────────────────────────────────────
    n_te_genereren = int(rood + oranje)
    if n_te_genereren > 0:
        with st.expander(f"⚡ Bulk genereer voor {n_te_genereren} collectie(s) met Claude"):
            st.caption(
                "Genereert meta title + description per collectie via Claude Haiku. "
                "Je kunt daarna per collectie nog aanpassen."
            )
            batch = st.slider("Max per klik", 1, 20, value=min(10, n_te_genereren), key="col_gen_batch")
            if st.button(f"✨ Genereer {batch} suggesties", key="col_gen_run"):
                kandidaten = df_display[df_display["_score"] != "🟢 OK"].head(batch)
                edits = st.session_state.get("col_edits", {})
                progress = st.progress(0.0)
                for i, (_, r) in enumerate(kandidaten.iterrows()):
                    progress.progress((i + 1) / len(kandidaten))
                    h = r["_handle"]
                    if h not in edits or not edits[h].get("title"):
                        t, d = _generate_meta_for_collection(r["_titel"], h)
                        edits[h] = {"title": t, "desc": d}
                st.session_state["col_edits"] = edits
                st.success(f"✅ {len(kandidaten)} suggesties gegenereerd.")
                st.rerun()

    st.divider()

    # ── Per-collectie bewerking ───────────────────────────────────────────────
    st.markdown(f"### Collecties ({totaal} getoond)")

    if df_display.empty:
        st.success("Alle collecties hebben goede SEO teksten! 🎉")
        return

    edits = st.session_state.get("col_edits", {})
    changed = False

    for _, row in df_display.iterrows():
        handle = row["_handle"]
        titel = row["_titel"]
        score = row["_score"]
        gepubl = row["_gepubl"]

        edit = edits.get(handle, {})
        cur_title = edit.get("title", row["_meta_title"])
        cur_desc = edit.get("desc", row["_meta_desc"])

        with st.expander(f"{score} **{titel}** (`{handle}`) {'🌐' if gepubl == 'ja' else '📴'}"):
            c_left, c_right = st.columns(2)

            with c_left:
                st.markdown("**Huidige teksten**")
                t_len = len(row["_meta_title"])
                d_len = len(row["_meta_desc"])
                st.caption(f"Title: {t_len} tekens {_kleur(t_len, 0, TITLE_MAX)}")
                st.text(row["_meta_title"] or "(leeg)")
                st.caption(f"Desc: {d_len} tekens {_kleur(d_len, DESC_MIN, DESC_MAX)}")
                st.text(row["_meta_desc"] or "(leeg)")

            with c_right:
                st.markdown("**Bewerken / gegenereerde suggestie**")
                new_title = st.text_input(
                    f"Meta title ({len(cur_title)}/{TITLE_MAX})",
                    value=cur_title,
                    key=f"col_t_{handle}",
                    max_chars=TITLE_MAX,
                )
                t_color = "🟢" if 0 < len(new_title) <= TITLE_MAX else ("🔴" if len(new_title) == 0 else "🟠")
                st.caption(f"{t_color} {len(new_title)} tekens")

                new_desc = st.text_area(
                    f"Meta description ({len(cur_desc)}/{DESC_MAX})",
                    value=cur_desc,
                    key=f"col_d_{handle}",
                    height=80,
                )
                d_color = "🟢" if DESC_MIN <= len(new_desc) <= DESC_MAX else ("🔴" if len(new_desc) == 0 else "🟠")
                st.caption(f"{d_color} {len(new_desc)} tekens (min {DESC_MIN}, max {DESC_MAX})")

                g1, g2 = st.columns(2)
                with g1:
                    if st.button("✨ Genereer", key=f"col_gen_{handle}"):
                        t, d = _generate_meta_for_collection(titel, handle)
                        edits[handle] = {"title": t, "desc": d}
                        st.session_state["col_edits"] = edits
                        st.rerun()
                with g2:
                    if st.button("💾 Opslaan", key=f"col_save_{handle}"):
                        edits[handle] = {"title": new_title, "desc": new_desc}
                        st.session_state["col_edits"] = edits
                        changed = True

    if changed:
        st.rerun()

    # ── Export ────────────────────────────────────────────────────────────────
    st.divider()
    st.markdown("### 📥 Exporteer naar Excel")

    goedgekeurd = [h for h, e in edits.items() if e.get("title") and e.get("desc")]
    st.caption(f"{len(goedgekeurd)} collecties klaar voor export (title + desc ingevuld).")

    if len(goedgekeurd) == 0:
        st.info("Bewerk en sla minstens één collectie op om te exporteren.")
        return

    if st.button(f"📥 Genereer export-Excel ({len(goedgekeurd)} collecties)", key="col_export", type="primary"):
        export_rows = []
        for handle, edit in edits.items():
            if edit.get("title") and edit.get("desc"):
                export_rows.append({
                    "handle": handle,
                    "meta_title": edit["title"],
                    "meta_description": edit["desc"],
                    "title_lengte": len(edit["title"]),
                    "desc_lengte": len(edit["desc"]),
                    "gegenereerd_op": datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
                })

        xlsx_bytes = _to_hextom_excel(export_rows)
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M")
        file_name = f"collectie_seo_{client_id}_{ts}.xlsx"

        st.download_button(
            f"📥 Download: {file_name}",
            data=xlsx_bytes,
            file_name=file_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="col_dl",
            use_container_width=True,
            type="primary",
        )

        user = current_user_email()
        rec = log_export(sb, client_id=client_id, task_type="collectie_seo",
                         fase=None, file_name=file_name,
                         row_count=len(export_rows), generated_by=user)
        if rec:
            st.session_state["col_export_rec"] = rec

    rec = st.session_state.get("col_export_rec")
    if rec:
        st.divider()
        st.markdown("### ✅ Bevestig importatie")
        st.caption(
            "Importeer de Excel in Shopify (handmatig of via Hextom / Matrixify) "
            "en bevestig hieronder zodra de teksten live zijn."
        )
        render_confirm_widget(sb, rec, current_user_email())
