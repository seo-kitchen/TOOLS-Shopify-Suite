"""Categorieën — 3 views: Mapping, Website-tree, Orphans."""
from __future__ import annotations

from collections import Counter

import pandas as pd
import streamlit as st

from ui.layout import page_header
from ui.supabase_client import get_supabase
from ui.website_tree import load_active_subsubcategories, load_website_tree


page_header(
    "🏷️ Categorieën",
    subtitle="Mapping (leverancier → Shopify), live website-structuur en orphans.",
)


@st.cache_data(ttl=60, show_spinner=False)
def _mappings() -> pd.DataFrame:
    sb = get_supabase()
    rows = sb.table("seo_category_mapping").select("*").execute().data or []
    return pd.DataFrame(rows)


@st.cache_data(ttl=60, show_spinner=False)
def _product_counts_by_subsub() -> dict[str, int]:
    sb = get_supabase()
    rows = (
        sb.table("seo_products")
        .select("sub_subcategorie")
        .not_.is_("sub_subcategorie", "null")
        .execute()
        .data
        or []
    )
    c = Counter()
    for r in rows:
        val = (r.get("sub_subcategorie") or "").strip()
        if val:
            c[val] += 1
    return dict(c)


@st.cache_data(ttl=60, show_spinner=False)
def _orphans() -> pd.DataFrame:
    """Unique (leverancier, cat, item_cat) tuples in seo_products that have no mapping row."""
    sb = get_supabase()
    prod = (
        sb.table("seo_products")
        .select("merk,leverancier_category,leverancier_item_cat")
        .execute()
        .data
        or []
    )
    mapped = (
        sb.table("seo_category_mapping")
        .select("leverancier_category,leverancier_item_cat")
        .execute()
        .data
        or []
    )
    mapped_keys = {(m.get("leverancier_category") or "", m.get("leverancier_item_cat") or "") for m in mapped}

    counter = Counter()
    for p in prod:
        lc = (p.get("leverancier_category") or "").strip()
        li = (p.get("leverancier_item_cat") or "").strip()
        if not lc and not li:
            continue
        if (lc, li) in mapped_keys:
            continue
        counter[(p.get("merk") or "?", lc, li)] += 1

    return pd.DataFrame(
        [
            {"merk": m, "leverancier_category": lc, "leverancier_item_cat": li, "aantal_producten": n}
            for (m, lc, li), n in sorted(counter.items(), key=lambda kv: -kv[1])
        ]
    )


tab_mapping, tab_tree, tab_orphans = st.tabs(["📋 Mapping", "🌳 Website-tree", "👻 Orphans"])


# ── Tab: Mapping ─────────────────────────────────────────────────────────────

with tab_mapping:
    try:
        df = _mappings()
    except Exception as e:
        st.error(f"Kon mapping niet laden: {e}")
        df = pd.DataFrame()

    if df.empty:
        st.info("Geen mapping-rijen. Seed via Setup → Categorie-seed.")
    else:
        counts = _product_counts_by_subsub()
        df["aantal_producten"] = df["sub_subcategorie"].map(lambda v: counts.get(str(v or "").strip(), 0))

        zoek = st.text_input("Zoek in mapping", placeholder="bv. 'plate' of 'Bloempot'", key="cat_zoek")
        if zoek:
            mask = df.apply(
                lambda r: any(zoek.lower() in str(v).lower() for v in r.values),
                axis=1,
            )
            df = df[mask]

        st.caption(f"{len(df)} mapping-rijen")
        st.dataframe(
            df.sort_values("aantal_producten", ascending=False),
            hide_index=True,
            width="stretch",
            column_config={"aantal_producten": st.column_config.NumberColumn("Producten", format="%d")},
        )


# ── Tab: Website-tree ────────────────────────────────────────────────────────

with tab_tree:
    try:
        tree = load_website_tree()
        actief_set = load_active_subsubcategories()
        counts = _product_counts_by_subsub()
    except Exception as e:
        st.error(f"Kon website-tree niet laden: {e}")
        tree = {}
        actief_set = set()
        counts = {}

    if not tree:
        st.warning("`Master Files/Website indeling (1).xlsx` niet gevonden of leeg.")
    else:
        st.caption(
            "🟢 actief op webshop · 🟡 geen producten · 🔴 niet actief. "
            f"Bron: Master Files xlsx, fills: {len(actief_set)} actieve sub-subcats."
        )
        for hoofdcat in sorted(tree.keys()):
            subcats = tree[hoofdcat]
            totaal_hc = sum(
                counts.get(ssc.strip(), 0)
                for sc, subsubs in subcats.items()
                for ssc in subsubs
            )
            with st.expander(f"**{hoofdcat}** ({totaal_hc} producten)", expanded=False):
                for subcat in sorted(subcats.keys()):
                    subsubs = subcats[subcat]
                    totaal_sc = sum(counts.get(ssc.strip(), 0) for ssc in subsubs)
                    st.markdown(f"_{subcat}_  ({totaal_sc} producten)")
                    rows = []
                    for ssc in subsubs:
                        n = counts.get(ssc.strip(), 0)
                        is_actief = ssc.strip().lower() in actief_set
                        icon = "🟢" if is_actief else ("🟡" if n == 0 else "🔴")
                        rows.append({"": icon, "sub-subcategorie": ssc, "producten": n})
                    if rows:
                        st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")


# ── Tab: Orphans ─────────────────────────────────────────────────────────────

with tab_orphans:
    try:
        orphans = _orphans()
    except Exception as e:
        st.error(f"Kon orphans niet berekenen: {e}")
        orphans = pd.DataFrame()

    if orphans.empty:
        st.success("✅ Geen orphan-categorieën — alle leverancier-codes in seo_products hebben een mapping.")
    else:
        st.caption(
            f"**{len(orphans)}** unieke leverancier-combinaties zonder mapping. "
            "Voeg een mapping toe via Setup → Categorie uitbreiden."
        )
        st.dataframe(
            orphans,
            hide_index=True,
            width="stretch",
            column_config={"aantal_producten": st.column_config.NumberColumn("Producten", format="%d")},
        )
