"""Tab — Archief herverwerken.

Primaire bron: shopify_meta_audit (heeft ALLE Shopify-producten met hun status).
Verrijkt met products_curated (pipeline-data) en products_raw (foto's, EAN, afmetingen).
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


LEVERANCIERS = ["Pottery Pots", "Serax", "Salt & Pepper", "Printworks",
                "BONBISTRO", "ONA", "Urban Nature Culture"]
SHOPIFY_STATUSSEN = ["archived", "active", "draft"]

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


def _build_hextom_excel(rows: list[dict]) -> bytes:
    wb = openpyxl.Workbook()
    ws = wb.active
    header_fill = PatternFill("solid", fgColor="1F4E79")
    header_font = Font(bold=True, color="FFFFFF", size=10)
    for col_idx, col_name in enumerate(HEXTOM_COLUMNS, start=1):
        cell = ws.cell(row=1, column=col_idx, value=col_name if col_name else "")
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center")

    for row_idx, p in enumerate(rows, start=2):
        row_data = {
            "Variant SKU":                               p.get("sku", ""),
            "":                                          "",
            "Product Handle":                            p.get("handle", ""),
            "Product Title":                             p.get("product_title_nl") or p.get("product_title", ""),
            "Product Vendor":                            p.get("vendor", ""),
            "Product Type":                              p.get("hoofdcategorie") or p.get("product_type", ""),
            "Variant Barcode":                           str(p.get("ean_shopify", "") or ""),
            "Variant Price":                             _clean_decimal(p.get("verkoopprijs") or p.get("price")),
            "Variant Cost":                              _clean_decimal(p.get("inkoopprijs")),
            "Product Description":                       p.get("meta_description") or p.get("current_meta_description", "") or "",
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
            "Product Metafield custom.meta_description": p.get("meta_description") or p.get("current_meta_description", "") or "",
        }
        row_fill = STATUS_FILL.get(p.get("product_status", ""))
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


@st.cache_data(ttl=60, show_spinner=False)
def _load(vendor: str, shopify_status: str, zoek: str, limit: int) -> list[dict]:
    sb = _get_sb()

    # Stap 1: ophalen uit shopify_meta_audit (primaire bron)
    q = sb.table("shopify_meta_audit").select(
        "handle,product_title,vendor,product_type,product_status,"
        "price,tags,current_meta_title,current_meta_description,"
        "title_status,desc_status,has_image,image_count"
    )
    if vendor != "Alle":
        q = q.eq("vendor", vendor)
    if shopify_status != "Alle":
        q = q.eq("product_status", shopify_status)
    if zoek:
        q = q.or_(f"handle.ilike.%{zoek}%,product_title.ilike.%{zoek}%")

    audit_rows = q.order("vendor").limit(limit).execute().data or []
    if not audit_rows:
        return []

    handles = [r["handle"] for r in audit_rows if r.get("handle")]

    # Stap 2: verrijk met products_curated (NL-titel, categorie, meta_description verbeterd)
    curated_by_handle: dict[str, dict] = {}
    if handles:
        try:
            cur_res = sb.table("products_curated").select(
                "handle,sku,supplier,fase,product_title_nl,hoofdcategorie,"
                "subcategorie,sub_subcategorie,collectie,tags,materiaal_nl,"
                "kleur_nl,meta_title,meta_description,verkoopprijs,inkoopprijs,pipeline_status"
            ).in_("handle", handles[:500]).execute().data or []
            curated_by_handle = {r["handle"]: r for r in cur_res}
        except Exception:
            pass

    # Stap 3: verrijk met products_raw (foto's, EAN, afmetingen) via SKU
    skus = [c["sku"] for c in curated_by_handle.values() if c.get("sku")]
    raw_by_sku: dict[str, dict] = {}
    if skus:
        try:
            raw_res = sb.table("products_raw").select(
                "sku,ean_shopify,ean_piece,designer,"
                "hoogte_cm,lengte_cm,breedte_cm,"
                "photo_packshot_1,photo_packshot_2,photo_packshot_3,"
                "photo_packshot_4,photo_packshot_5,"
                "photo_lifestyle_1,photo_lifestyle_2,photo_lifestyle_3,"
                "photo_lifestyle_4,photo_lifestyle_5"
            ).in_("sku", skus[:500]).execute().data or []
            raw_by_sku = {r["sku"]: r for r in raw_res}
        except Exception:
            pass

    # Samenvoegen
    merged = []
    for a in audit_rows:
        handle = a.get("handle", "")
        cur = curated_by_handle.get(handle, {})
        raw = raw_by_sku.get(cur.get("sku", ""), {})
        merged.append({**a, **raw, **cur})

    return merged


def render() -> None:
    st.subheader("Archief herverwerken")
    st.caption(
        "Selecteer producten direct uit Shopify-data — geen Hextom-export nodig. "
        "Filter op merk en status, selecteer, en exporteer of herstart de pipeline."
    )

    f1, f2, f3 = st.columns([2, 2, 3])
    with f1:
        vendor = st.selectbox("Merk", ["Alle"] + LEVERANCIERS, key="hv_vendor")
    with f2:
        shopify_status = st.selectbox(
            "Status in Shopify", ["Alle"] + SHOPIFY_STATUSSEN, index=2, key="hv_sh"
        )  # default: archived
    with f3:
        zoek = st.text_input("Zoek (handle / productnaam)", placeholder="bijv. pottery-pots-vaas", key="hv_zoek")

    col_lim, col_btn, col_ref = st.columns([2, 1, 1])
    with col_lim:
        limit = st.number_input("Max te laden", 50, 2000, 500, 50, key="hv_limit")
    with col_btn:
        st.caption("&nbsp;")
        laden = st.button("🔍 Laad producten", type="primary", key="hv_laden")
    with col_ref:
        st.caption("&nbsp;")
        if st.button("🔄 Ververs", key="hv_refresh"):
            _load.clear()
            st.rerun()

    if laden:
        st.session_state["hv_geladen"] = True
    if not st.session_state.get("hv_geladen"):
        st.info("Stel filters in en klik **Laad producten** om te beginnen.")
        return

    with st.spinner("Ophalen uit Supabase..."):
        try:
            rows = _load(vendor, shopify_status, zoek.strip(), int(limit))
        except Exception as e:
            st.error(f"❌ Fout: {e}")
            return

    if not rows:
        st.warning(f"Geen producten gevonden — vendor='{vendor}', status='{shopify_status}'.")
        return

    df = pd.DataFrame(rows)
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Gevonden", len(rows))
    m2.metric("Met NL-titel", int(df["product_title_nl"].notna().sum()) if "product_title_nl" in df else 0)
    m3.metric("Met meta-desc", int(df["current_meta_description"].notna().sum()) if "current_meta_description" in df else 0)
    m4.metric("Met foto", int(df["has_image"].sum()) if "has_image" in df else 0)

    TOON = ["handle", "product_title", "vendor", "product_status",
            "pipeline_status", "hoofdcategorie", "price"]
    for col in TOON:
        if col not in df.columns:
            df[col] = None

    edited = st.data_editor(
        df[["handle"] + TOON[1:]].assign(_select=False),
        column_config={
            "_select":         st.column_config.CheckboxColumn("✔", default=False, width="small"),
            "handle":          st.column_config.TextColumn("Handle", disabled=True, width="medium"),
            "product_title":   st.column_config.TextColumn("Titel (Shopify)", disabled=True, width="large"),
            "vendor":          st.column_config.TextColumn("Merk", disabled=True, width="small"),
            "product_status":  st.column_config.TextColumn("Shopify", disabled=True, width="small"),
            "pipeline_status": st.column_config.TextColumn("Pipeline", disabled=True, width="small"),
            "hoofdcategorie":  st.column_config.TextColumn("Categorie", disabled=True, width="medium"),
            "price":           st.column_config.NumberColumn("Prijs", disabled=True, width="small", format="€ %.2f"),
        },
        column_order=["_select", "handle", "product_title", "vendor",
                      "product_status", "pipeline_status", "hoofdcategorie", "price"],
        hide_index=True,
        disabled=["handle"] + TOON[1:],
        width="stretch",
        key="hv_editor",
    )

    selected_handles = edited.loc[edited["_select"], "handle"].tolist()
    selected_rows = [r for r in rows if r.get("handle") in selected_handles]
    st.caption(f"**{len(selected_handles)} van {len(rows)} geselecteerd**")

    if not selected_handles:
        st.info("Selecteer producten via de checkboxes hierboven.")
        return

    st.divider()
    st.markdown(f"### ⚡ Acties voor {len(selected_handles)} geselecteerde producten")

    a1, a2, a3 = st.columns(3)

    with a1:
        st.markdown("**A — Exporteer naar Hextom**")
        st.caption("Download als Hextom Excel met beschikbare data.")
        compleet = sum(1 for r in selected_rows if r.get("handle") and r.get("product_title"))
        if st.button(f"📥 Download {len(selected_handles)} als Hextom Excel", key="hv_export"):
            with st.spinner("Excel bouwen..."):
                xlsx = _build_hextom_excel(selected_rows)
            vendor_label = vendor.replace(" ", "_").replace("/", "-")
            sh_label = shopify_status if shopify_status != "Alle" else "mix"
            st.download_button(
                label=f"💾 Download ({len(selected_handles)} producten)",
                data=xlsx,
                file_name=f"hextom_{vendor_label}_{sh_label}_{len(selected_handles)}st.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="hv_dl",
            )

    with a2:
        st.markdown("**B — Reset pipeline-status**")
        st.caption("Alleen voor producten die al in products_curated staan.")
        nieuwe_ps = st.selectbox("Nieuwe status", ["matched", "raw", "ready"], key="hv_new_ps")
        curated_ids = [r["id"] for r in selected_rows if r.get("id") and r.get("pipeline_status")]
        if curated_ids:
            if st.button(f"🔄 Reset {len(curated_ids)} → {nieuwe_ps}", type="primary", key="hv_reset"):
                try:
                    sb = _get_sb()
                    sb.table("products_curated").update({"pipeline_status": nieuwe_ps}) \
                      .in_("id", curated_ids).execute()
                    _load.clear()
                    st.success(f"✅ {len(curated_ids)} producten op `{nieuwe_ps}` gezet.")
                except Exception as e:
                    st.error(f"Fout: {e}")
        else:
            st.caption("_Geen van de geselecteerde producten zit in products_curated._")

    with a3:
        st.markdown("**C — Stuur naar Transform**")
        st.caption("Zet curated IDs klaar voor het Transform-scherm (max 25).")
        curated_ids_transform = [r["id"] for r in selected_rows if r.get("id") and r.get("pipeline_status")]
        n = min(len(curated_ids_transform), 25)
        if curated_ids_transform:
            if st.button(f"✨ Stuur {n} naar Transform", key="hv_to_transform"):
                st.session_state["selected_ids"] = curated_ids_transform[:25]
                st.session_state["transform_from_producten"] = True
                st.success(f"✅ {n} IDs klaargezet. Ga naar **Transform** in het menu.")
        else:
            st.caption("_Geen pipeline-producten in selectie._")

    st.divider()
    with st.expander(f"🔍 Volledigheidscheck ({len(selected_handles)} producten)"):
        check = []
        for r in selected_rows:
            check.append({
                "Handle":        r.get("handle", "—"),
                "SKU":           "✅" if r.get("sku") else "❌",
                "NL-titel":      "✅" if r.get("product_title_nl") else "❌",
                "Meta desc":     "✅" if (r.get("meta_description") or r.get("current_meta_description")) else "❌",
                "Categorie":     "✅" if r.get("hoofdcategorie") else "❌",
                "Foto":          "✅" if r.get("photo_packshot_1") or r.get("has_image") else "❌",
                "In pipeline":   "✅" if r.get("pipeline_status") else "❌",
            })
        st.dataframe(pd.DataFrame(check), hide_index=True, use_container_width=True)
