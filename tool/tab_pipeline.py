"""Tab — Volledige pipeline (10 stappen) op nieuwe Supabase.

Stappen:
  1. Ingest      — Excel upload naar products_raw
  2. Categorie-mapping check — gaps in seo_category_mapping
  3. Transform   — categorisatie + vertaling + titel + meta
  4. Validate    — kwaliteit + auto-fix
  5. Review      — handmatige correcties op review-status
  6. Export      — Hextom Excel
  7. Learnings   — feedback regels (pending/applied)
"""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# Maak execution-modules importeerbaar
_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
_DASHBOARD = _ROOT / "dashboard_v2"
for _p in [str(_DASHBOARD), str(_ROOT)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)


@st.cache_resource
def _get_sb():
    from supabase import create_client
    url = os.getenv("SUPABASE_NEW_URL", "")
    key = os.getenv("SUPABASE_NEW_SERVICE_KEY", "") or os.getenv("SUPABASE_NEW_KEY", "")
    if not url or not key:
        raise RuntimeError("SUPABASE_NEW_URL/KEY ontbreekt.")
    return create_client(url, key)


LEVERANCIERS = ["Pottery Pots", "Serax", "Salt & Pepper", "Printworks",
                "BONBISTRO", "ONA", "Urban Nature Culture"]
FASES = ["1", "2", "3", "4", "5", "6"]


# ── Status counts ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=30, show_spinner=False)
def _status_counts(fase: str | None = None, supplier: str | None = None) -> dict:
    sb = _get_sb()
    counts = {"raw_total": 0, "curated_total": 0,
              "raw": 0, "matched": 0, "ready": 0, "review": 0, "exported": 0}
    try:
        # raw producten
        q = sb.table("products_raw").select("id", count="exact")
        if fase and fase != "Alle":
            q = q.eq("fase", fase)
        if supplier and supplier != "Alle":
            q = q.eq("supplier", supplier)
        counts["raw_total"] = q.execute().count or 0

        # curated per status
        q = sb.table("products_curated").select("pipeline_status,sku,supplier,fase")
        if fase and fase != "Alle":
            q = q.eq("fase", fase)
        if supplier and supplier != "Alle":
            q = q.eq("supplier", supplier)
        rows = q.execute().data or []
        counts["curated_total"] = len(rows)
        for r in rows:
            s = r.get("pipeline_status") or "raw"
            counts[s] = counts.get(s, 0) + 1
    except Exception:
        pass
    return counts


def _progress_callback(bar, log_area):
    lines: list[str] = []

    def _cb(i: int, n: int, msg: str = "") -> None:
        try:
            bar.progress(min(max(i / max(n, 1), 0.0), 1.0))
        except Exception:
            pass
        if msg:
            lines.append(str(msg))
            log_area.code("\n".join(lines[-30:]))
    return _cb


# ── Render ────────────────────────────────────────────────────────────────────

def render() -> None:
    st.subheader("Volledige pipeline — nieuwe schema")
    st.caption(
        "Alle stappen voor productverwerking: ingest → transform → validate → export. "
        "Werkt op `products_raw` + `products_curated` met `seo_category_mapping`."
    )

    # ── Globale filters ──
    f1, f2 = st.columns(2)
    with f1:
        fase = st.selectbox("Fase", ["Alle"] + FASES, index=4, key="pl_fase")
    with f2:
        supplier = st.selectbox("Leverancier", ["Alle"] + LEVERANCIERS, key="pl_sup")

    counts = _status_counts(fase, supplier)

    # ── Voortgangsbalk ──
    klaar = counts.get("ready", 0) + counts.get("exported", 0)
    totaal = max(counts.get("raw_total", 0), 1)
    st.progress(klaar / totaal,
                text=f"Voortgang: {klaar} / {totaal} klaar  ·  "
                     f"raw {counts['raw_total']} · curated {counts['curated_total']} · "
                     f"ready {counts.get('ready', 0)} · review {counts.get('review', 0)}")

    if st.button("🔄 Refresh tellers", key="pl_refresh"):
        _status_counts.clear()
        st.rerun()

    st.divider()

    # ════════════════════════════════════════════════════════════════════════
    # STAP 1 — INGEST
    # ════════════════════════════════════════════════════════════════════════
    with st.expander("1. Ingest — Excel upload naar products_raw", expanded=False):
        st.caption(
            "Upload de leverancier-Excel. Auto-detect van kolommen (SKU, EAN, naam, prijzen, "
            "afmetingen, foto's, leverancier-categorieën). Schrijft naar `products_raw`."
        )

        col_up, col_fo = st.columns(2)
        with col_up:
            uploaded = st.file_uploader("Masterdata Excel", type=["xlsx", "xls"], key="pl_ing_file")
        with col_fo:
            fotos_file = st.file_uploader("Optioneel: foto-Excel", type=["xlsx"], key="pl_ing_fotos")

        if uploaded:
            try:
                df_prev = pd.read_excel(uploaded, dtype=str, nrows=5)
                uploaded.seek(0)
                st.markdown("**Eerste 5 rijen:**")
                st.dataframe(df_prev, hide_index=True, use_container_width=True)
                st.caption(f"{len(df_prev.columns)} kolommen: {', '.join(list(df_prev.columns)[:10])}...")
            except Exception as e:
                st.warning(f"Preview mislukt: {e}")

        col_lev, col_run = st.columns([2, 1])
        with col_lev:
            ingest_supplier = st.selectbox("Leverancier voor deze import",
                                            LEVERANCIERS, key="pl_ing_sup")
        with col_run:
            st.caption("&nbsp;")
            run = st.button("Run ingest", type="primary",
                            disabled=uploaded is None or fase == "Alle", key="pl_ing_run")

        if run and uploaded:
            try:
                from execution.ingest_v2 import ingest_masterdata
            except ImportError as e:
                st.error(f"ingest_v2 niet beschikbaar: {e}")
                st.stop()

            tmp = Path(tempfile.mkdtemp(prefix="pl_ing_"))
            excel_path = tmp / uploaded.name
            excel_path.write_bytes(uploaded.getvalue())
            fotos_path = None
            if fotos_file:
                fotos_path = tmp / fotos_file.name
                fotos_path.write_bytes(fotos_file.getvalue())

            bar = st.progress(0.0)
            log_area = st.empty()
            cb = _progress_callback(bar, log_area)
            try:
                result = ingest_masterdata(
                    file_path=str(excel_path),
                    fase=fase,
                    supplier=ingest_supplier,
                    fotos_path=str(fotos_path) if fotos_path else None,
                    progress=cb,
                    logger=lambda m: cb(0, 1, m),
                )
                st.success(f"✅ {result.inserted_count} nieuw, {result.updated_count} updated, "
                           f"{result.skipped_count} overgeslagen.")
                if result.warnings:
                    with st.expander(f"⚠️ {len(result.warnings)} waarschuwingen"):
                        for w in result.warnings[:50]:
                            st.caption(f"- {w}")
                _status_counts.clear()
            except Exception as e:
                st.error(f"❌ {e}")

    # ════════════════════════════════════════════════════════════════════════
    # STAP 2 — CATEGORIE GAPS
    # ════════════════════════════════════════════════════════════════════════
    with st.expander("2. Categorie-mapping check — gaps detecteren", expanded=False):
        st.caption(
            "Vergelijkt unieke leverancier_category + leverancier_item_cat in `products_raw` "
            "tegen `seo_category_mapping`. Toont wat nog niet gemapt is."
        )
        if st.button("Analyseer gaps", key="pl_gaps_run"):
            try:
                sb = _get_sb()
                # Unieke combinaties uit raw
                q = sb.table("products_raw").select("leverancier_category,leverancier_item_cat,supplier")
                if fase and fase != "Alle":
                    q = q.eq("fase", fase)
                if supplier and supplier != "Alle":
                    q = q.eq("supplier", supplier)
                raw_rows = q.execute().data or []

                # Mapping ophalen
                mapping = sb.table("seo_category_mapping").select(
                    "leverancier_category,leverancier_item_cat").execute().data or []
                gemapt = {(m.get("leverancier_category") or "", m.get("leverancier_item_cat") or "")
                          for m in mapping}

                # Aantal per combinatie
                from collections import Counter
                combos = Counter(
                    (r.get("leverancier_category") or "—", r.get("leverancier_item_cat") or "—")
                    for r in raw_rows
                )

                gaps = []
                for (lc, lic), n in combos.most_common():
                    if (lc, lic) not in gemapt:
                        gaps.append({
                            "leverancier_category": lc,
                            "leverancier_item_cat": lic,
                            "aantal_producten": n,
                        })

                if gaps:
                    st.warning(f"⚠️ {len(gaps)} categorie-combinaties nog NIET gemapt "
                               f"(totaal {sum(g['aantal_producten'] for g in gaps)} producten):")
                    st.dataframe(pd.DataFrame(gaps), hide_index=True, use_container_width=True)
                    st.caption("Voeg deze toe aan `seo_category_mapping` voordat je transform draait.")
                else:
                    st.success(f"✅ Alle {len(combos)} categorie-combinaties zijn gemapt.")
            except Exception as e:
                st.error(f"Fout: {e}")

    # ════════════════════════════════════════════════════════════════════════
    # STAP 3 — TRANSFORM
    # ════════════════════════════════════════════════════════════════════════
    with st.expander("3. Transform — categorisatie + vertaling + titel + meta", expanded=False):
        st.caption(
            "Verwerkt producten uit `products_raw` naar `products_curated`. "
            "Categoriseert via mapping, vertaalt materiaal/kleur, bouwt NL-titel, "
            "schrijft meta description (Sonnet). Past learnings toe."
        )

        col_n, col_run = st.columns([2, 1])
        with col_n:
            batch_size = st.number_input("Batch grootte", 1, 500, 50, 10, key="pl_tr_size")
        with col_run:
            st.caption("&nbsp;")
            run_tr = st.button(f"Verrijk {batch_size} producten", type="primary",
                                disabled=(fase == "Alle"), key="pl_tr_run")

        if run_tr:
            try:
                from execution.transform_v2 import transform_batch
            except ImportError as e:
                st.error(f"transform_v2 niet beschikbaar: {e}")
                st.stop()

            bar = st.progress(0.0)
            log_area = st.empty()
            cb = _progress_callback(bar, log_area)
            try:
                with st.spinner("Bezig..."):
                    result = transform_batch(
                        fase=fase if fase != "Alle" else None,
                        limit=int(batch_size),
                        pipeline_status="raw",
                        progress=cb,
                        logger=lambda m: cb(0, 1, m),
                    )
                st.success(f"✅ Klaar — {result.ready} ready, {result.review} review, "
                           f"{result.errors} errors. {result.learnings_applied} learnings toegepast.")
                if result.new_filter_values:
                    with st.expander(f"⚠️ {len(result.new_filter_values)} nieuwe filterwaarden"):
                        for w in result.new_filter_values[:30]:
                            st.caption(f"- {w}")
                if result.twijfelgevallen:
                    with st.expander(f"🤔 {len(result.twijfelgevallen)} twijfelgevallen"):
                        for t in result.twijfelgevallen[:30]:
                            st.caption(f"- {t['sku']}: {t['info']}")
                _status_counts.clear()
            except Exception as e:
                st.error(f"❌ {e}")
                import traceback
                with st.expander("Traceback"):
                    st.code(traceback.format_exc())

    # ════════════════════════════════════════════════════════════════════════
    # STAP 4 — VALIDATE
    # ════════════════════════════════════════════════════════════════════════
    with st.expander("4. Validate — kwaliteit + auto-fix", expanded=False):
        st.caption(
            "Checkt verplichte velden, meta-lengtes, dubbele handles, decimalen. "
            "Auto-fix actief: te lange meta wordt afgekapt, decimalen opgeschoond."
        )

        col_a, col_v = st.columns([2, 1])
        with col_a:
            autofix = st.toggle("Auto-fix aan", value=True, key="pl_val_fix")
        with col_v:
            st.caption("&nbsp;")
            run_v = st.button("Run validatie", type="primary",
                              disabled=counts.get("ready", 0) == 0 and counts.get("review", 0) == 0,
                              key="pl_val_run")

        if run_v:
            try:
                from execution.validate_v2 import validate_batch
            except ImportError as e:
                st.error(f"validate_v2 niet beschikbaar: {e}")
                st.stop()

            bar = st.progress(0.0)
            log_area = st.empty()
            cb = _progress_callback(bar, log_area)
            try:
                with st.spinner("Bezig..."):
                    result = validate_batch(
                        fase=fase if fase != "Alle" else None,
                        autofix=autofix,
                        progress=cb,
                        logger=lambda m: cb(0, 1, m),
                    )
                st.success(f"✅ Klaar — {result.ok_count} ok, {result.error_count} review, "
                           f"{result.fixed_count} auto-fixes.")
                if result.errors:
                    with st.expander(f"❌ {len(result.errors)} producten met errors"):
                        for e in result.errors[:30]:
                            st.caption(f"- {e['sku']}: {'; '.join(e['errors'])}")
                _status_counts.clear()
            except Exception as e:
                st.error(f"❌ {e}")

    # ════════════════════════════════════════════════════════════════════════
    # STAP 5 — REVIEW (handmatige correctie)
    # ════════════════════════════════════════════════════════════════════════
    with st.expander(f"5. Review — handmatige correctie ({counts.get('review', 0)} producten)",
                     expanded=False):
        st.caption(
            "Producten met `pipeline_status=review` hebben handmatige aandacht nodig. "
            "Bewerk hieronder en zet status terug op `ready` om door te gaan naar export."
        )

        if counts.get("review", 0) == 0:
            st.success("✅ Geen producten in review.")
        else:
            try:
                sb = _get_sb()
                q = sb.table("products_curated").select(
                    "id,sku,supplier,fase,product_title_nl,hoofdcategorie,sub_subcategorie,"
                    "materiaal_nl,kleur_nl,verkoopprijs,meta_description,review_reden,pipeline_status"
                ).eq("pipeline_status", "review")
                if fase and fase != "Alle":
                    q = q.eq("fase", fase)
                if supplier and supplier != "Alle":
                    q = q.eq("supplier", supplier)
                review_rows = q.limit(100).execute().data or []

                if review_rows:
                    df_rev = pd.DataFrame(review_rows)
                    st.dataframe(df_rev[["sku", "product_title_nl", "supplier",
                                          "hoofdcategorie", "review_reden"]],
                                 hide_index=True, use_container_width=True)
                    st.caption(f"Eerste {len(review_rows)} review-producten getoond. "
                               "Bewerk handmatig in tabblad Producten of via SQL.")
            except Exception as e:
                st.error(f"Fout: {e}")

    # ════════════════════════════════════════════════════════════════════════
    # STAP 6 — EXPORT
    # ════════════════════════════════════════════════════════════════════════
    with st.expander(f"6. Export — Hextom Excel ({counts.get('ready', 0)} klaar)", expanded=False):
        st.caption(
            "Gebruik de Archief-herverwerken pagina om producten te selecteren en te exporteren, "
            "of de standaard Export-knop hieronder voor alle producten met `pipeline_status=ready`."
        )

        if st.button(f"Genereer Hextom Excel voor {counts.get('ready', 0)} producten",
                     disabled=counts.get("ready", 0) == 0, key="pl_exp_run"):
            try:
                from tab_herverwerk import _build_hextom_excel
                sb = _get_sb()
                q = sb.table("products_curated").select("*").eq("pipeline_status", "ready")
                if fase and fase != "Alle":
                    q = q.eq("fase", fase)
                if supplier and supplier != "Alle":
                    q = q.eq("supplier", supplier)
                rows = q.execute().data or []

                # Verrijk met products_raw (foto's, EAN, afmetingen)
                skus = [r["sku"] for r in rows if r.get("sku")]
                raw_by_sku = {}
                if skus:
                    for i in range(0, len(skus), 200):
                        chunk = skus[i:i + 200]
                        raw_res = sb.table("products_raw").select("*").in_("sku", chunk).execute().data or []
                        for r in raw_res:
                            raw_by_sku[r["sku"]] = r

                # Merge raw data in voor de export
                for r in rows:
                    raw = raw_by_sku.get(r.get("sku", ""), {})
                    # Voeg foto's, EAN, designer, afmetingen toe vanuit raw
                    for veld in ("ean_shopify", "ean_piece", "designer",
                                 "hoogte_cm", "lengte_cm", "breedte_cm",
                                 "photo_packshot_1", "photo_packshot_2", "photo_packshot_3",
                                 "photo_packshot_4", "photo_packshot_5",
                                 "photo_lifestyle_1", "photo_lifestyle_2", "photo_lifestyle_3",
                                 "photo_lifestyle_4", "photo_lifestyle_5"):
                        if not r.get(veld) and raw.get(veld):
                            r[veld] = raw[veld]
                    # Vendor wordt gemapt vanuit supplier
                    r["vendor"] = r.get("supplier", "")
                    r["product_title"] = r.get("product_title_nl", "")
                    r["product_status"] = r.get("pipeline_status", "")

                xlsx = _build_hextom_excel(rows)
                fase_label = fase if fase != "Alle" else "alle"
                sup_label = supplier.replace(" ", "_").replace("/", "-") if supplier != "Alle" else "alle"
                st.download_button(
                    f"Download {len(rows)} producten",
                    data=xlsx,
                    file_name=f"hextom_fase{fase_label}_{sup_label}_{len(rows)}st.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="pl_exp_dl",
                )
            except Exception as e:
                st.error(f"❌ {e}")
                import traceback
                with st.expander("Traceback"):
                    st.code(traceback.format_exc())
