"""Tab — Archief herverwerken.

Haalt producten op uit products_curated (+ products_raw voor foto's/EAN),
gecombineerd met shopify_meta_audit voor de Shopify-status (active/archived/draft).
Selecteer een subset en herstart de pipeline of exporteer direct naar Hextom.
"""
from __future__ import annotations

import io
import os

import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()


@st.cache_resource
def _get_sb():
    from supabase import create_client
    url = os.getenv("SUPABASE_NEW_URL", "")
    key = os.getenv("SUPABASE_NEW_SERVICE_KEY", "") or os.getenv("SUPABASE_NEW_KEY", "")
    if not url or not key:
        raise RuntimeError("SUPABASE_NEW_URL of SUPABASE_NEW_SERVICE_KEY ontbreekt.")
    return create_client(url, key)


LEVERANCIERS = ["Pottery Pots", "Serax", "Printworks", "Salt & Pepper"]
SHOPIFY_STATUSSEN = ["archived", "active", "draft"]
PIPELINE_STATUSSEN = ["raw", "matched", "ready", "review", "exported"]

# Hextom-kolomstructuur
HEXTOM_COLUMNS = [
    "Variant SKU", "", "", "Product Handle", "Product Title",
    "Product Vendor", "Product Type", "Variant Barcode", "Variant Price",
    "Variant Cost", "Product Description", "", "", "", "Product Tags",
    "Variant Metafield custom.collectie", "Product Metafield custom.designer",
    "Product Metafield custom.materiaal", "Product Metafield custom.kleur",
    "Product Metafield custom.hoogte_filter", "Product Metafield custom.lengte_filter",
    "Product Metafield custom.breedte_filter",
    "photo_packshot_1", "photo_packshot_2", "photo_packshot_3",
    "photo_packshot_4", "photo_packshot_5",
    "photo_lifestyle_1", "photo_lifestyle_2", "photo_lifestyle_3",
    "photo_lifestyle_4", "photo_lifestyle_5",
    "Product Metafield custom.ean",
    "Product Metafield custom.artikelnummer",
    "Product Metafield custom.meta_description",
]
TEXT_FORMAT_COLUMNS = {"Variant Barcode", "Product Metafield custom.ean"}
STATUS_FILL = {
    "active":   PatternFill("solid", fgColor="CCFFCC"),
    "archived": PatternFill("solid", fgColor="FFE4B5"),
    "draft":    PatternFill("solid", fgColor="E0E0E0"),
}


def _clean_decimal(value) -> str:
    if value is None:
        return ""
    s = str(value).replace(",", ".")
    try:
        f = float(s)
        return f"{f:.10f}".rstrip("0").rstrip(".")
    except ValueError:
        return s


def _build_hextom_excel(merged: list[dict]) -> bytes:
    """Bouw Hextom Excel uit gemergde curated+raw records."""
    wb = openpyxl.Workbook()
    ws = wb.active

    header_fill = PatternFill("solid", fgColor="1F4E79")
    header_font = Font(bold=True, color="FFFFFF", size=10)
    for col_idx, col_name in enumerate(HEXTOM_COLUMNS, start=1):
        cell = ws.cell(row=1, column=col_idx, value=col_name if col_name else "")
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center")

    for row_idx, p in enumerate(merged, start=2):
        row_data = {
            "Variant SKU":                               p.get("sku", ""),
            "":                                          "",
            "Product Handle":                            p.get("handle", ""),
            "Product Title":                             p.get("product_title_nl", ""),
            "Product Vendor":                            p.get("supplier", ""),
            "Product Type":                              p.get("hoofdcategorie", ""),
            "Variant Barcode":                           str(p.get("ean_shopify", "") or ""),
            "Variant Price":                             _clean_decimal(p.get("verkoopprijs")),
            "Variant Cost":                              _clean_decimal(p.get("inkoopprijs")),
            "Product Description":                       p.get("meta_description", "") or "",
            "Product Tags":                              p.get("tags", "") or "",
            "Variant Metafield custom.collectie":        p.get("collectie", "") or "",
            "Product Metafield custom.designer":         p.get("designer", "") or "",
            "Product Metafield custom.materiaal":        p.get("materiaal_nl", "") or "",
            "Product Metafield custom.kleur":            p.get("kleur_nl", "") or "",
            "Product Metafield custom.hoogte_filter":    _clean_decimal(p.get("hoogte_cm")),
            "Product Metafield custom.lengte_filter":    _clean_decimal(p.get("lengte_cm")),
            "Product Metafield custom.breedte_filter":   _clean_decimal(p.get("breedte_cm")),
            "photo_packshot_1":                          p.get("photo_packshot_1", "") or "",
            "photo_packshot_2":                          p.get("photo_packshot_2", "") or "",
            "photo_packshot_3":                          p.get("photo_packshot_3", "") or "",
            "photo_packshot_4":                          p.get("photo_packshot_4", "") or "",
            "photo_packshot_5":                          p.get("photo_packshot_5", "") or "",
            "photo_lifestyle_1":                         p.get("photo_lifestyle_1", "") or "",
            "photo_lifestyle_2":                         p.get("photo_lifestyle_2", "") or "",
            "photo_lifestyle_3":                         p.get("photo_lifestyle_3", "") or "",
            "photo_lifestyle_4":                         p.get("photo_lifestyle_4", "") or "",
            "photo_lifestyle_5":                         p.get("photo_lifestyle_5", "") or "",
            "Product Metafield custom.ean":              str(p.get("ean_piece", "") or ""),
            "Product Metafield custom.artikelnummer":    p.get("sku", "") or "",
            "Product Metafield custom.meta_description": p.get("meta_description", "") or "",
        }
        shopify_status = p.get("shopify_status", "")
        row_fill = STATUS_FILL.get(shopify_status)
        for col_idx, col_name in enumerate(HEXTOM_COLUMNS, start=1):
            value = row_data.get(col_name, "") if col_name else ""
            cell = ws.cell(row=row_idx, column=col_idx, value=value)
            if col_name in TEXT_FORMAT_COLUMNS and value:
                cell.value = str(value)
                cell.number_format = "@"
            if row_fill:
                cell.fill = row_fill

    col_widths = {1: 18, 4: 40, 5: 50, 8: 16, 11: 60, 15: 50}
    for col_idx in range(1, len(HEXTOM_COLUMNS) + 1):
        ws.column_dimensions[get_column_letter(col_idx)].width = col_widths.get(col_idx, 20)

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ── Data ophalen ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=60, show_spinner=False)
def _load(supplier: str, shopify_status: str, pipeline_status: str,
          fase: str, zoek: str, limit: int) -> list[dict]:
    sb = _get_sb()

    # Stap 1: als shopify_status filter → haal handles op uit shopify_meta_audit
    handle_whitelist: list[str] | None = None
    if shopify_status != "Alle":
        q_audit = sb.table("shopify_meta_audit").select("handle,product_status,vendor")
        q_audit = q_audit.eq("product_status", shopify_status)
        if supplier != "Alle":
            q_audit = q_audit.ilike("vendor", f"%{supplier}%")
        audit_rows = q_audit.limit(2000).execute().data or []
        handle_whitelist = [r["handle"] for r in audit_rows if r.get("handle")]
        if not handle_whitelist:
            return []

    # Stap 2: query products_curated
    q = sb.table("products_curated").select(
        "id,sku,raw_id,supplier,fase,product_title_nl,handle,"
        "hoofdcategorie,subcategorie,sub_subcategorie,"
        "collectie,tags,materiaal_nl,kleur_nl,"
        "meta_title,meta_description,verkoopprijs,inkoopprijs,pipeline_status"
    )
    if supplier != "Alle" and shopify_status == "Alle":
        q = q.ilike("supplier", f"%{supplier}%")
    if pipeline_status != "Alle":
        q = q.eq("pipeline_status", pipeline_status)
    if fase != "Alle":
        q = q.eq("fase", fase)
    if zoek:
        q = q.or_(f"sku.ilike.%{zoek}%,product_title_nl.ilike.%{zoek}%")
    if handle_whitelist is not None:
        # filter in batches van 200 (Supabase IN-limiet)
        q = q.in_("handle", handle_whitelist[:500])

    curated_rows = q.order("supplier").limit(limit).execute().data or []
    if not curated_rows:
        return []

    # Stap 3: verrijk met products_raw (foto's, EAN, afmetingen) via SKU
    skus = [r["sku"] for r in curated_rows if r.get("sku")]
    raw_by_sku: dict[str, dict] = {}
    if skus:
        raw_res = sb.table("products_raw").select(
            "sku,ean_shopify,ean_piece,designer,kleur_en,"
            "hoogte_cm,lengte_cm,breedte_cm,"
            "photo_packshot_1,photo_packshot_2,photo_packshot_3,"
            "photo_packshot_4,photo_packshot_5,"
            "photo_lifestyle_1,photo_lifestyle_2,photo_lifestyle_3,"
            "photo_lifestyle_4,photo_lifestyle_5"
        ).in_("sku", skus[:500]).execute().data or []
        raw_by_sku = {r["sku"]: r for r in raw_res}

    # Stap 4: haal shopify_status op via handle (voor kleurcodering export)
    handles = [r["handle"] for r in curated_rows if r.get("handle")]
    shopify_status_by_handle: dict[str, str] = {}
    if handles and shopify_status == "Alle":
        audit_res = sb.table("shopify_meta_audit").select("handle,product_status") \
            .in_("handle", handles[:500]).execute().data or []
        shopify_status_by_handle = {r["handle"]: r.get("product_status", "") for r in audit_res}

    # Stap 5: samenvoegen
    merged = []
    for c in curated_rows:
        raw = raw_by_sku.get(c.get("sku", ""), {})
        sh_status = (
            shopify_status if shopify_status != "Alle"
            else shopify_status_by_handle.get(c.get("handle", ""), "onbekend")
        )
        merged.append({**c, **raw, "shopify_status": sh_status})

    return merged


# ── Render ────────────────────────────────────────────────────────────────────

def render() -> None:
    st.subheader("♻️ Archief herverwerken")
    st.caption(
        "Selecteer producten rechtstreeks uit de database — geen Hextom-export nodig. "
        "Filter op merk en Shopify-status, selecteer een subset, en exporteer of herstart."
    )

    # ── Filters ───────────────────────────────────────────────────────────────
    f1, f2, f3, f4 = st.columns([2, 2, 1, 2])
    with f1:
        supplier = st.selectbox("Merk / Leverancier", ["Alle"] + LEVERANCIERS, key="hv_sup")
    with f2:
        shopify_status = st.selectbox(
            "Status in Shopify", ["Alle"] + SHOPIFY_STATUSSEN, index=2, key="hv_sh"
        )  # default: archived
    with f3:
        fase = st.selectbox("Fase", ["Alle", "1", "2", "3", "4", "5", "6"], key="hv_fase")
    with f4:
        zoek = st.text_input("Zoek (SKU / titel)", placeholder="bijv. B4020040", key="hv_zoek")

    f5, f6 = st.columns([2, 2])
    with f5:
        pipeline_status = st.selectbox(
            "Pipeline-status", ["Alle"] + PIPELINE_STATUSSEN, key="hv_ps"
        )
    with f6:
        limit = st.number_input("Max te laden", 50, 2000, 500, 50, key="hv_limit")

    col_btn, col_ref = st.columns([1, 1])
    with col_btn:
        laden = st.button("🔍 Laad producten", type="primary", key="hv_laden")
    with col_ref:
        if st.button("🔄 Ververs cache", key="hv_refresh"):
            _load.clear()
            st.rerun()

    if laden:
        st.session_state["hv_geladen"] = True
    if not st.session_state.get("hv_geladen"):
        st.info("Stel filters in en klik **Laad producten** om te beginnen.")
        return

    # ── Laden ─────────────────────────────────────────────────────────────────
    with st.spinner("Ophalen uit Supabase..."):
        try:
            rows = _load(supplier, shopify_status, pipeline_status,
                         fase, zoek.strip(), int(limit))
        except Exception as e:
            st.error(f"❌ Fout bij ophalen: {e}")
            return

    if not rows:
        st.warning("Geen producten gevonden met deze filters.")
        return

    # ── Metrics ───────────────────────────────────────────────────────────────
    df = pd.DataFrame(rows)
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Gevonden", len(rows))
    m2.metric("Met NL-titel", int(df["product_title_nl"].notna().sum()))
    m3.metric("Met meta-desc", int(df["meta_description"].notna().sum()))
    m4.metric("Met foto", int(df["photo_packshot_1"].notna().sum()) if "photo_packshot_1" in df else 0)

    # ── Tabel ─────────────────────────────────────────────────────────────────
    TOON = ["sku", "product_title_nl", "supplier", "fase",
            "pipeline_status", "shopify_status", "hoofdcategorie", "verkoopprijs"]
    for col in TOON:
        if col not in df.columns:
            df[col] = None

    edited = st.data_editor(
        df[["id"] + TOON].assign(_select=False),
        column_config={
            "_select":         st.column_config.CheckboxColumn("✔", default=False, width="small"),
            "id":              st.column_config.NumberColumn("ID", disabled=True, width="small"),
            "sku":             st.column_config.TextColumn("SKU", disabled=True, width="small"),
            "product_title_nl": st.column_config.TextColumn("Titel NL", disabled=True, width="large"),
            "supplier":        st.column_config.TextColumn("Merk", disabled=True, width="small"),
            "fase":            st.column_config.TextColumn("Fase", disabled=True, width="small"),
            "pipeline_status": st.column_config.TextColumn("Pipeline", disabled=True, width="small"),
            "shopify_status":  st.column_config.TextColumn("Shopify", disabled=True, width="small"),
            "hoofdcategorie":  st.column_config.TextColumn("Categorie", disabled=True, width="medium"),
            "verkoopprijs":    st.column_config.NumberColumn("Prijs", disabled=True, width="small", format="€ %.2f"),
        },
        column_order=["_select", "id", "sku", "product_title_nl", "supplier",
                      "fase", "pipeline_status", "shopify_status", "hoofdcategorie", "verkoopprijs"],
        hide_index=True,
        disabled=["id"] + TOON,
        width="stretch",
        key="hv_editor",
    )

    selected_ids = edited.loc[edited["_select"], "id"].tolist()
    selected_rows = [r for r in rows if r["id"] in selected_ids]
    st.caption(f"**{len(selected_ids)} van {len(rows)} geselecteerd**")

    if not selected_ids:
        st.info("Selecteer producten via de checkboxes hierboven.")
        return

    st.divider()
    st.markdown(f"### ⚡ Acties voor {len(selected_ids)} geselecteerde producten")

    a1, a2, a3 = st.columns(3)

    # ── A: Reset pipeline-status ───────────────────────────────────────────────
    with a1:
        st.markdown("**A — Klaar voor Transform**")
        st.caption("Reset pipeline_status naar `matched` zodat ze in Transform verschijnen.")
        nieuwe_ps = st.selectbox(
            "Nieuwe pipeline-status",
            ["matched", "raw", "ready"],
            key="hv_new_ps",
        )
        if st.button(f"🔄 Reset {len(selected_ids)} → {nieuwe_ps}", type="primary", key="hv_reset"):
            try:
                sb = _get_sb()
                sb.table("products_curated").update({"pipeline_status": nieuwe_ps}) \
                  .in_("id", selected_ids).execute()
                _load.clear()
                st.success(
                    f"✅ {len(selected_ids)} producten op `{nieuwe_ps}` gezet. "
                    "Ga naar **Transform** voor AI-titels en descriptions."
                )
            except Exception as e:
                st.error(f"Fout: {e}")

    # ── B: Hextom export ──────────────────────────────────────────────────────
    with a2:
        st.markdown("**B — Exporteer naar Hextom**")
        st.caption("Exporteer direct als Hextom Excel (voor complete producten).")
        compleet = sum(
            1 for r in selected_rows
            if r.get("product_title_nl") and r.get("meta_description") and r.get("hoofdcategorie")
        )
        onvolledig = len(selected_ids) - compleet
        if onvolledig:
            st.warning(f"⚠️ {onvolledig} producten missen titel / meta / categorie.")

        if st.button(f"📥 Download {len(selected_ids)} als Hextom Excel", key="hv_export"):
            with st.spinner("Excel bouwen..."):
                xlsx = _build_hextom_excel(selected_rows)
            sup_label = supplier.replace(" ", "_").replace("/", "-")
            sh_label = shopify_status if shopify_status != "Alle" else "mix"
            st.download_button(
                label=f"💾 Download ({len(selected_ids)} producten)",
                data=xlsx,
                file_name=f"hextom_{sup_label}_{sh_label}_{len(selected_ids)}st.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="hv_dl",
            )

    # ── C: Naar Transform ─────────────────────────────────────────────────────
    with a3:
        st.markdown("**C — Stuur naar Transform**")
        st.caption(f"Zet max 25 IDs klaar in de sessie voor het Transform-scherm.")
        n = min(len(selected_ids), 25)
        if len(selected_ids) > 25:
            st.warning(f"Transform heeft een cap van 25 — de eerste {n} worden gestuurd.")
        if st.button(f"✨ Stuur {n} naar Transform", key="hv_to_transform"):
            st.session_state["selected_ids"] = selected_ids[:25]
            st.session_state["transform_from_producten"] = True
            st.success(f"✅ {n} IDs klaargezet. Ga naar **Transform** in het menu.")

    st.divider()

    # ── Volledigheidscheck ────────────────────────────────────────────────────
    with st.expander(f"🔍 Volledigheidscheck ({len(selected_ids)} producten)"):
        check = []
        for r in selected_rows:
            check.append({
                "SKU":           r.get("sku", "—"),
                "Titel NL":      "✅" if r.get("product_title_nl") else "❌",
                "Meta desc":     "✅" if r.get("meta_description") else "❌",
                "Categorie":     "✅" if r.get("hoofdcategorie") else "❌",
                "Prijs":         "✅" if r.get("verkoopprijs") else "❌",
                "Foto":          "✅" if r.get("photo_packshot_1") else "❌",
                "Handle":        "✅" if r.get("handle") else "❌",
                "Shopify-status": r.get("shopify_status", "—"),
            })
        st.dataframe(pd.DataFrame(check), hide_index=True, use_container_width=True)
