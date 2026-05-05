"""Tab — Archief herverwerken.

Haal gearchiveerde (of andere) producten rechtstreeks op uit seo_products,
selecteer een subset en start de pipeline opnieuw:
  1. Bekijken & filteren
  2. Status resetten naar 'matched' (klaar voor Transform)
  3. Direct exporteren als Hextom Excel (als data al compleet is)
"""
from __future__ import annotations

import io
import os
import sys
from pathlib import Path

import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter
import pandas as pd
import streamlit as st

from ui.supabase_client import get_supabase

MERKEN = ["Pottery Pots", "Serax", "Printworks", "S&P/Bonbistro"]
STATUS_SHOPIFY_OPTS = ["archief", "nieuw", "actief", "onbekend"]
STATUS_PIPELINE_OPTS = ["raw", "matched", "ready", "review", "exported"]
FASES = ["1", "2", "3", "4", "5", "6"]

# Exacte Hextom-kolomstructuur (zelfde als execution/export.py)
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
    "actief":  PatternFill("solid", fgColor="FFCCCC"),
    "archief": PatternFill("solid", fgColor="FFE4B5"),
    "nieuw":   None,
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


def _product_to_hextom_row(p: dict) -> dict:
    """seo_products-rij → Hextom-kolommen (veldnamen gemapt)."""
    return {
        "Variant SKU":                               p.get("sku", ""),
        "":                                          "",
        "Product Handle":                            p.get("handle", ""),
        "Product Title":                             p.get("product_title_nl", ""),
        "Product Vendor":                            p.get("merk", ""),
        "Product Type":                              p.get("hoofdcategorie", ""),
        "Variant Barcode":                           str(p.get("ean_shopify", "") or ""),
        "Variant Price":                             _clean_decimal(p.get("rrp_stuk_eur")),
        "Variant Cost":                              _clean_decimal(p.get("inkoopprijs_stuk_eur")),
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


def _build_hextom_excel(products: list[dict]) -> bytes:
    wb = openpyxl.Workbook()
    ws = wb.active

    header_fill = PatternFill("solid", fgColor="1F4E79")
    header_font = Font(bold=True, color="FFFFFF", size=10)
    for col_idx, col_name in enumerate(HEXTOM_COLUMNS, start=1):
        cell = ws.cell(row=1, column=col_idx, value=col_name if col_name else "")
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center")

    for row_idx, product in enumerate(products, start=2):
        row_data = _product_to_hextom_row(product)
        row_fill = STATUS_FILL.get(product.get("status_shopify") or "nieuw")
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

SELECT_COLS = (
    "id,sku,ean_shopify,ean_piece,handle,product_name_raw,product_title_nl,"
    "merk,fase,status,status_shopify,"
    "hoofdcategorie,subcategorie,sub_subcategorie,"
    "rrp_stuk_eur,rrp_gb_eur,inkoopprijs_stuk_eur,"
    "meta_description,tags,collectie,"
    "designer,materiaal_nl,kleur_nl,"
    "hoogte_cm,lengte_cm,breedte_cm,"
    "photo_packshot_1,photo_packshot_2,photo_packshot_3,"
    "photo_packshot_4,photo_packshot_5,"
    "photo_lifestyle_1,photo_lifestyle_2,photo_lifestyle_3,"
    "photo_lifestyle_4,photo_lifestyle_5"
)


@st.cache_data(ttl=30, show_spinner=False)
def _load_producten(merk: str, status_shopify: str, fase: str,
                    zoek: str, limit: int) -> list[dict]:
    sb = get_supabase()
    q = sb.table("seo_products").select(SELECT_COLS)
    if merk != "Alle":
        q = q.eq("merk", merk)
    if status_shopify != "Alle":
        q = q.eq("status_shopify", status_shopify)
    if fase != "Alle":
        q = q.eq("fase", fase)
    if zoek:
        q = q.or_(
            f"sku.ilike.%{zoek}%,"
            f"product_name_raw.ilike.%{zoek}%,"
            f"product_title_nl.ilike.%{zoek}%"
        )
    return q.order("merk").limit(limit).execute().data or []


# ── Render ────────────────────────────────────────────────────────────────────

def render() -> None:
    st.subheader("♻️ Archief herverwerken")
    st.caption(
        "Haal producten rechtstreeks op uit de database (geen Hextom-export nodig). "
        "Selecteer een subset → reset de status → exporteer of stuur naar Transform."
    )

    # ── Filters ───────────────────────────────────────────────────────────────
    f1, f2, f3, f4 = st.columns([2, 2, 1, 2])
    with f1:
        merk = st.selectbox("Merk", ["Alle"] + MERKEN, index=1, key="hv_merk")
    with f2:
        status_shopify = st.selectbox(
            "Status Shopify", ["Alle"] + STATUS_SHOPIFY_OPTS, index=1, key="hv_ss"
        )  # default: archief
    with f3:
        fase = st.selectbox("Fase", ["Alle"] + FASES, key="hv_fase")
    with f4:
        zoek = st.text_input("Zoek (SKU / naam)", placeholder="bijv. B4020040", key="hv_zoek")

    col_lim, col_btn, col_ref = st.columns([2, 1, 1])
    with col_lim:
        limit = st.number_input("Max te laden", 50, 2000, 500, 50, key="hv_limit")
    with col_btn:
        st.caption("&nbsp;")
        laden = st.button("🔍 Laad producten", type="primary", key="hv_laden")
    with col_ref:
        st.caption("&nbsp;")
        if st.button("🔄 Ververs cache", key="hv_refresh"):
            _load_producten.clear()
            st.rerun()

    if laden or st.session_state.get("hv_geladen"):
        st.session_state["hv_geladen"] = True
    else:
        st.info("Stel filters in en klik **Laad producten** om te beginnen.")
        return

    # ── Laden ─────────────────────────────────────────────────────────────────
    with st.spinner("Ophalen uit Supabase..."):
        rows = _load_producten(merk, status_shopify, fase, zoek.strip(), int(limit))

    if not rows:
        st.warning("Geen producten gevonden met deze filters.")
        return

    # ── Tabel met checkboxes ──────────────────────────────────────────────────
    WEERGAVE_COLS = [
        "sku", "product_name_raw", "product_title_nl",
        "merk", "fase", "status", "status_shopify",
        "hoofdcategorie", "rrp_stuk_eur",
        "meta_description",
    ]

    df = pd.DataFrame(rows)
    for col in WEERGAVE_COLS:
        if col not in df.columns:
            df[col] = None

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Gevonden", len(rows))
    m2.metric("Met meta-description", df["meta_description"].notna().sum())
    m3.metric("Met product-titel (NL)", df["product_title_nl"].notna().sum())
    m4.metric("Met foto", df["photo_packshot_1"].notna().sum() if "photo_packshot_1" in df else 0)

    st.markdown("**Selecteer producten** — vink aan welke je wilt herverwerken:")

    edited = st.data_editor(
        df[["id"] + WEERGAVE_COLS].assign(_select=False),
        column_config={
            "_select":           st.column_config.CheckboxColumn("✔", default=False, width="small"),
            "id":                st.column_config.NumberColumn("ID", disabled=True, width="small"),
            "sku":               st.column_config.TextColumn("SKU", disabled=True, width="small"),
            "product_name_raw":  st.column_config.TextColumn("Naam (raw)", disabled=True, width="large"),
            "product_title_nl":  st.column_config.TextColumn("Titel NL", disabled=True, width="large"),
            "merk":              st.column_config.TextColumn("Merk", disabled=True, width="small"),
            "fase":              st.column_config.TextColumn("Fase", disabled=True, width="small"),
            "status":            st.column_config.TextColumn("Status pipeline", disabled=True, width="small"),
            "status_shopify":    st.column_config.TextColumn("Status Shopify", disabled=True, width="small"),
            "hoofdcategorie":    st.column_config.TextColumn("Categorie", disabled=True, width="medium"),
            "rrp_stuk_eur":      st.column_config.NumberColumn("Prijs", disabled=True, width="small", format="€ %.2f"),
            "meta_description":  st.column_config.TextColumn("Meta desc", disabled=True, width="large"),
        },
        column_order=["_select", "id", "sku", "product_name_raw", "product_title_nl",
                      "merk", "fase", "status", "status_shopify",
                      "hoofdcategorie", "rrp_stuk_eur", "meta_description"],
        hide_index=True,
        disabled=["id"] + WEERGAVE_COLS,
        width="stretch",
        key="hv_editor",
    )

    selected_ids = edited.loc[edited["_select"], "id"].tolist()
    selected_rows = [r for r in rows if r["id"] in selected_ids]

    st.caption(f"**{len(selected_ids)} van {len(rows)} geselecteerd**")

    if not selected_ids:
        st.info("Selecteer eerst producten via de checkboxes hierboven.")
        return

    st.divider()
    st.markdown(f"### ⚡ Acties voor {len(selected_ids)} geselecteerde producten")

    a1, a2, a3 = st.columns(3)

    # ── Actie 1: Status resetten ───────────────────────────────────────────────
    with a1:
        st.markdown("**Stap A — Klaar voor Transform**")
        st.caption(
            "Reset de pipeline-status naar `matched` zodat de producten "
            "in het Transform-scherm verschijnen voor nieuwe AI-titels/descriptions."
        )
        nieuwe_ss = st.selectbox(
            "Verander status_shopify naar",
            ["(niet wijzigen)", "actief", "nieuw", "archief"],
            key="hv_new_ss",
        )
        if st.button(
            f"🔄 Reset {len(selected_ids)} producten → matched",
            type="primary", key="hv_reset"
        ):
            sb = get_supabase()
            update_payload: dict = {"status": "matched"}
            if nieuwe_ss != "(niet wijzigen)":
                update_payload["status_shopify"] = nieuwe_ss
            try:
                sb.table("seo_products").update(update_payload).in_("id", selected_ids).execute()
                _load_producten.clear()
                st.success(
                    f"✅ {len(selected_ids)} producten op `matched` gezet. "
                    "Ga naar het **Transform**-scherm om AI-titels te genereren."
                )
            except Exception as e:
                st.error(f"Fout: {e}")

    # ── Actie 2: Direct exporteren ─────────────────────────────────────────────
    with a2:
        st.markdown("**Stap B — Exporteer naar Hextom**")
        st.caption(
            "Exporteer de selectie direct als Hextom Excel. "
            "Gebruik dit als de producten al volledige data hebben "
            "(titel, meta, categorie, foto's)."
        )
        compleet = sum(
            1 for r in selected_rows
            if r.get("product_title_nl") and r.get("meta_description") and r.get("hoofdcategorie")
        )
        onvolledig = len(selected_ids) - compleet
        if onvolledig:
            st.warning(f"⚠️ {onvolledig} van {len(selected_ids)} hebben geen volledige titel/meta/categorie.")

        if st.button(f"📥 Download {len(selected_ids)} als Hextom Excel", key="hv_export"):
            with st.spinner("Excel bouwen..."):
                xlsx_bytes = _build_hextom_excel(selected_rows)
            merk_label = merk.replace(" ", "_").replace("/", "-")
            ss_label = status_shopify if status_shopify != "Alle" else "mix"
            st.download_button(
                label=f"💾 Download ({len(selected_ids)} producten)",
                data=xlsx_bytes,
                file_name=f"hextom_{merk_label}_{ss_label}_{len(selected_ids)}st.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="hv_dl",
            )

    # ── Actie 3: Stuur naar Transform ─────────────────────────────────────────
    with a3:
        st.markdown("**Stap C — Stuur naar Transform**")
        st.caption(
            "Zet de IDs klaar in de sessie zodat je ze kunt ophalen "
            "in het Transform-scherm (max 25 per batch)."
        )
        if len(selected_ids) > 25:
            st.warning(f"Transform heeft een cap van 25. Je hebt {len(selected_ids)} geselecteerd — de eerste 25 worden gestuurd.")
        if st.button(
            f"✨ Stuur {min(len(selected_ids), 25)} naar Transform",
            key="hv_to_transform"
        ):
            st.session_state["selected_ids"] = selected_ids[:25]
            st.session_state["transform_from_producten"] = True
            st.success(
                f"✅ {min(len(selected_ids), 25)} IDs klaargezet. "
                "Ga naar **Transform** in het navigatiemenu."
            )

    st.divider()

    # ── Volledigheidscheck ────────────────────────────────────────────────────
    with st.expander(f"🔍 Volledigheidscheck ({len(selected_ids)} producten)"):
        check_rows = []
        for r in selected_rows:
            check_rows.append({
                "SKU":          r.get("sku", "—"),
                "Titel NL":     "✅" if r.get("product_title_nl") else "❌",
                "Meta desc":    "✅" if r.get("meta_description") else "❌",
                "Categorie":    "✅" if r.get("hoofdcategorie") else "❌",
                "Prijs":        "✅" if r.get("rrp_stuk_eur") else "❌",
                "Foto":         "✅" if r.get("photo_packshot_1") else "❌",
                "Handle":       "✅" if r.get("handle") else "❌",
            })
        st.dataframe(pd.DataFrame(check_rows), hide_index=True, use_container_width=True)
