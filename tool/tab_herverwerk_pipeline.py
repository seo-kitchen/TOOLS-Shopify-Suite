"""Herverwerk-pipeline — review & bewerk geselecteerde producten.

Wordt geopend vanuit de Herverwerk-tab. Stap voor stap:
  1. AI-generatie: NL-titels (Haiku batch) + meta descriptions (Sonnet)
  2. Review & aanpassen: bewerkbaar overzicht per product
  3. Opslaan in products_curated
  4. Download Hextom Excel
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
        raise RuntimeError("SUPABASE_NEW_URL ontbreekt.")
    return create_client(url, key)


# ── AI-generatie ──────────────────────────────────────────────────────────────

def _genereer_ai(rows: list[dict]) -> list[dict]:
    """
    Stap 1: batch naam-vertaling (Haiku) + meta description per product (Sonnet).
    Werkt de rijen in-place bij en geeft ze terug.
    """
    import anthropic
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))
    n = len(rows)

    bar = st.progress(0.0, text="Stap 1 — Namen vertalen (Haiku)...")
    log = st.empty()

    # ── Batch naam-vertaling (1 Haiku-call) ──
    namen = [r.get("product_title", "").strip() for r in rows]
    uniek = list(dict.fromkeys(nm for nm in namen if nm))
    vertaling_map: dict[str, str] = {}

    try:
        prompt = (
            f"Vertaal deze {len(uniek)} Engelse productnamen naar het Nederlands "
            "voor een design webshop.\n\n"
            "REGELS:\n"
            "- Behoud eigennamen (collectie/designer) onveranderd\n"
            "- Title Case (eerste letter groot, behalve van/de/het/en/in/op)\n"
            "- Vertaal: Plate→Bord, Bowl→Kom, Vase→Vaas, Pot→Pot, Cup→Kopje, "
            "Mug→Mok, Tray→Dienblad, Jug→Kan, Mirror→Spiegel, "
            "Planter→Bloempot, Basket→Mand, Lantern→Lantaarn\n"
            "- Maatcodes uppercase: XS, S, M, L, XL, XXL\n"
            "- Één regel per naam, dezelfde volgorde, geen nummering\n\n"
            "INPUT:\n" + "\n".join(uniek) + "\n\nOUTPUT:"
        )
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=max(2000, len(uniek) * 16),
            messages=[{"role": "user", "content": prompt}],
        )
        lines = [l.strip() for l in resp.content[0].text.strip().split("\n") if l.strip()]
        if len(lines) == len(uniek):
            vertaling_map = dict(zip(uniek, lines))
            log.caption(f"✅ {len(vertaling_map)} namen vertaald")
        else:
            log.caption(f"⚠️ Naam-vertaling mismatch ({len(lines)} vs {len(uniek)}), originele namen gebruikt")
    except Exception as e:
        log.caption(f"⚠️ Naam-vertaling mislukt: {e}")

    # ── Meta description per product (Sonnet) ──
    for i, r in enumerate(rows):
        frac = (i + 1) / n
        bar.progress(frac, text=f"Stap 2 — Meta desc {i+1}/{n}: {r.get('sku') or r.get('handle', '')}")

        raw_title = (r.get("product_title") or "").strip()
        title_nl = vertaling_map.get(raw_title, raw_title)

        # Gebruik bestaande NL-titel als die al ingevuld is in curated
        if r.get("product_title_nl"):
            title_nl = r["product_title_nl"]

        r["product_title_nl"] = title_nl

        # Sla meta generation over als er al een goede meta description is
        bestaande_meta = r.get("meta_description") or r.get("current_meta_description") or ""
        if bestaande_meta and len(bestaande_meta) >= 100:
            r["meta_description"] = bestaande_meta
            continue

        try:
            vendor  = r.get("vendor", "")
            subcat  = r.get("subcategorie") or r.get("product_type") or ""
            mat     = r.get("materiaal_nl", "") or ""
            kleur   = r.get("kleur_nl", "") or ""
            hoogte  = r.get("hoogte_cm") or ""
            lengte  = r.get("lengte_cm") or ""
            breedte = r.get("breedte_cm") or ""
            afm = f"H {hoogte} x L {lengte} x B {breedte} cm" if all([hoogte, lengte, breedte]) else ""

            extra = ""
            if mat:    extra += f"Materiaal: {mat}\n"
            if kleur:  extra += f"Kleur: {kleur}\n"
            if subcat: extra += f"Categorie: {subcat}\n"
            if afm:    extra += f"Afmetingen: {afm}\n"

            meta_prompt = (
                f"Schrijf een Nederlandse SEO meta description (120–155 tekens).\n"
                f"Product: {title_nl}\nMerk: {vendor}\n{extra}\n"
                "Regels: 'je'-vorm, eindig met CTA, vermeld gratis verzending €75.\n"
                "Geef alleen de meta description terug, geen uitleg."
            )
            resp = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=200,
                messages=[{"role": "user", "content": meta_prompt}],
            )
            r["meta_description"] = resp.content[0].text.strip()[:155]
        except Exception as e:
            r["meta_description"] = bestaande_meta or ""
            log.caption(f"⚠️ Meta mislukt voor {r.get('handle', i)}: {e}")

    bar.progress(1.0, text="AI-generatie klaar.")
    return rows


# ── Hextom export ─────────────────────────────────────────────────────────────

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


def _clean(v) -> str:
    if v is None: return ""
    s = str(v).replace(",", ".")
    try:
        f = float(s)
        return f"{f:.10f}".rstrip("0").rstrip(".")
    except ValueError:
        return s


def _build_excel(rows: list[dict]) -> bytes:
    wb = openpyxl.Workbook()
    ws = wb.active
    hf = PatternFill("solid", fgColor="1F4E79")
    hfont = Font(bold=True, color="FFFFFF", size=10)
    for ci, col in enumerate(HEXTOM_COLUMNS, 1):
        c = ws.cell(row=1, column=ci, value=col if col else "")
        c.fill = hf; c.font = hfont
        c.alignment = Alignment(horizontal="center")

    for ri, p in enumerate(rows, 2):
        row_data = {
            "Variant SKU":                               p.get("sku", ""),
            "Product Handle":                            p.get("handle", ""),
            "Product Title":                             p.get("product_title_nl") or p.get("product_title", ""),
            "Product Vendor":                            p.get("vendor", ""),
            "Product Type":                              p.get("hoofdcategorie") or p.get("product_type", ""),
            "Variant Barcode":                           str(p.get("ean_shopify", "") or ""),
            "Variant Price":                             _clean(p.get("verkoopprijs") or p.get("price")),
            "Variant Cost":                              _clean(p.get("inkoopprijs")),
            "Product Description":                       p.get("meta_description", "") or "",
            "Product Tags":                              p.get("tags", "") or "",
            "Variant Metafield custom.collectie":        p.get("collectie", "") or "",
            "Product Metafield custom.designer":         p.get("designer", "") or "",
            "Product Metafield custom.materiaal":        p.get("materiaal_nl", "") or "",
            "Product Metafield custom.kleur":            p.get("kleur_nl", "") or "",
            "Product Metafield custom.hoogte_filter":    _clean(p.get("hoogte_cm")),
            "Product Metafield custom.lengte_filter":    _clean(p.get("lengte_cm")),
            "Product Metafield custom.breedte_filter":   _clean(p.get("breedte_cm")),
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
        for ci, col in enumerate(HEXTOM_COLUMNS, 1):
            val = row_data.get(col, "") if col else ""
            cell = ws.cell(row=ri, column=ci, value=val)
            if col in TEXT_FORMAT_COLUMNS and val:
                cell.value = str(val)
                cell.number_format = "@"

    for ci in range(1, len(HEXTOM_COLUMNS) + 1):
        ws.column_dimensions[get_column_letter(ci)].width = {1:18,4:40,5:50,8:16,11:60,15:50}.get(ci, 20)

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ── Render ────────────────────────────────────────────────────────────────────

def render() -> None:
    st.subheader("Herverwerk — review & aanpassen")

    rows: list[dict] = st.session_state.get("hv_pipeline_rows", [])
    if not rows:
        st.warning("Geen producten geladen. Ga terug naar **Archief herverwerken** en selecteer producten.")
        if st.button("Terug naar Archief herverwerken"):
            st.switch_page("pages/08_Herverwerk.py")
        return

    n = len(rows)
    st.caption(f"{n} producten geladen vanuit Archief herverwerken.")

    # ── Stap 1: AI-generatie ──────────────────────────────────────────────────
    if not st.session_state.get("hvp_ai_klaar"):
        st.markdown("### Stap 1 — AI genereert NL-titels en meta descriptions")
        kosten = n * 0.002
        st.caption(
            f"Haiku vertaalt alle namen in één batch-call. "
            f"Sonnet schrijft een meta description per product. "
            f"Geschatte kosten: ~€{kosten:.2f}"
        )
        if st.button(f"Start AI-generatie voor {n} producten", type="primary", key="hvp_start_ai"):
            rows = _genereer_ai(rows)
            st.session_state["hv_pipeline_rows"] = rows
            st.session_state["hvp_ai_klaar"] = True
            st.rerun()
        return

    st.success(f"✅ AI-generatie klaar voor {n} producten.")

    # ── Stap 2: Review & aanpassen ────────────────────────────────────────────
    st.markdown("### Stap 2 — Review en aanpassen")
    st.caption(
        "Pas aan wat nodig is. Klik in een cel om te bewerken. "
        "Foto's en EAN's worden NIET overschreven."
    )

    EDIT_COLS = [
        "handle", "sku", "vendor",
        "product_title_nl", "meta_description",
        "hoofdcategorie", "subcategorie", "sub_subcategorie",
        "collectie", "materiaal_nl", "kleur_nl",
        "hoogte_cm", "lengte_cm", "breedte_cm",
        "verkoopprijs",
    ]
    READONLY = ["handle", "sku", "vendor"]

    df = pd.DataFrame(rows)
    for col in EDIT_COLS:
        if col not in df.columns:
            df[col] = None

    col_config = {
        "handle":           st.column_config.TextColumn("Handle",        disabled=True,  width="medium"),
        "sku":              st.column_config.TextColumn("SKU",            disabled=True,  width="small"),
        "vendor":           st.column_config.TextColumn("Merk",           disabled=True,  width="small"),
        "product_title_nl": st.column_config.TextColumn("Titel NL",       disabled=False, width="large"),
        "meta_description": st.column_config.TextColumn("Meta description", disabled=False, width="large"),
        "hoofdcategorie":   st.column_config.TextColumn("Hoofdcategorie", disabled=False, width="medium"),
        "subcategorie":     st.column_config.TextColumn("Subcategorie",   disabled=False, width="medium"),
        "sub_subcategorie": st.column_config.TextColumn("Sub-subcategorie", disabled=False, width="medium"),
        "collectie":        st.column_config.TextColumn("Collectie",      disabled=False, width="small"),
        "materiaal_nl":     st.column_config.TextColumn("Materiaal NL",   disabled=False, width="small"),
        "kleur_nl":         st.column_config.TextColumn("Kleur NL",       disabled=False, width="small"),
        "hoogte_cm":        st.column_config.NumberColumn("H (cm)",       disabled=False, width="small", format="%.1f"),
        "lengte_cm":        st.column_config.NumberColumn("L (cm)",       disabled=False, width="small", format="%.1f"),
        "breedte_cm":       st.column_config.NumberColumn("B (cm)",       disabled=False, width="small", format="%.1f"),
        "verkoopprijs":     st.column_config.NumberColumn("Prijs (€)",    disabled=False, width="small", format="€ %.2f"),
    }

    edited_df = st.data_editor(
        df[EDIT_COLS],
        column_config=col_config,
        column_order=EDIT_COLS,
        hide_index=True,
        disabled=READONLY,
        width="stretch",
        key="hvp_editor",
    )

    # Kwaliteitscheck balk
    heeft_titel = edited_df["product_title_nl"].notna().sum()
    heeft_meta  = edited_df["meta_description"].notna().sum()
    heeft_cat   = edited_df["hoofdcategorie"].notna().sum()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Producten", n)
    c2.metric("Met NL-titel", int(heeft_titel))
    c3.metric("Met meta desc", int(heeft_meta))
    c4.metric("Met categorie", int(heeft_cat))

    st.divider()

    # ── Stap 3: Opslaan + exporteren ─────────────────────────────────────────
    st.markdown("### Stap 3 — Opslaan en exporteren")

    act1, act2 = st.columns(2)

    with act1:
        st.markdown("**Opslaan in database**")
        st.caption("Schrijft alle bewerkingen naar `products_curated` (upsert op handle). Foto's blijven ongewijzigd.")
        if st.button(f"Sla {n} producten op", type="primary", key="hvp_save"):
            sb = _get_sb()
            saved = errors = 0
            for _, row in edited_df.iterrows():
                handle = row.get("handle", "")
                if not handle:
                    continue
                # Zoek de originele rij voor velden die NIET in de editor staan (foto's etc.)
                orig = next((r for r in rows if r.get("handle") == handle), {})

                curated_data: dict = {
                    "handle":           handle,
                    "sku":              row.get("sku") or orig.get("sku", ""),
                    "supplier":         row.get("vendor") or orig.get("vendor", ""),
                    "product_title_nl": row.get("product_title_nl") or "",
                    "meta_description": row.get("meta_description") or "",
                    "pipeline_status":  "ready",
                }
                # Optionele velden alleen opslaan als ze ingevuld zijn
                for veld in ("hoofdcategorie", "subcategorie", "sub_subcategorie",
                             "collectie", "materiaal_nl", "kleur_nl"):
                    val = row.get(veld)
                    if pd.notna(val) and str(val).strip():
                        curated_data[veld] = str(val).strip()

                for veld in ("hoogte_cm", "lengte_cm", "breedte_cm", "verkoopprijs"):
                    val = row.get(veld)
                    if pd.notna(val):
                        try:
                            curated_data[veld] = float(val)
                        except (ValueError, TypeError):
                            pass

                try:
                    existing = sb.table("products_curated").select("id") \
                                 .eq("handle", handle).execute().data
                    if existing:
                        sb.table("products_curated").update(curated_data) \
                          .eq("handle", handle).execute()
                    else:
                        sb.table("products_curated").insert(curated_data).execute()
                    saved += 1
                except Exception as e:
                    errors += 1

            if errors:
                st.warning(f"✅ {saved} opgeslagen · ⚠️ {errors} fouten")
            else:
                st.success(f"✅ {saved} producten opgeslagen in de database.")

    with act2:
        st.markdown("**Download Hextom Excel**")
        st.caption("Combineert de bewerkte data met bestaande foto's en EAN's.")

        if st.button(f"Download {n} producten als Hextom Excel", key="hvp_export"):
            # Merge edited_df terug met originele rows (voor foto's etc.)
            merged_export = []
            for _, row in edited_df.iterrows():
                handle = row.get("handle", "")
                orig = next((r for r in rows if r.get("handle") == handle), {})
                merged = {**orig, **row.to_dict()}
                merged_export.append(merged)

            with st.spinner("Excel bouwen..."):
                xlsx = _build_excel(merged_export)
            vendor_label = (merged_export[0].get("vendor") or "producten").replace(" ", "_") if merged_export else "export"
            st.download_button(
                label=f"💾 Download ({n} producten)",
                data=xlsx,
                file_name=f"hextom_{vendor_label}_herverwerkt_{n}st.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="hvp_dl",
            )

    st.divider()

    # Terug-knop
    if st.button("Terug naar Archief herverwerken", key="hvp_terug"):
        st.session_state.pop("hvp_ai_klaar", None)
        st.switch_page("pages/08_Herverwerk.py")
