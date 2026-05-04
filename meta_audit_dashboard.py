"""
Streamlit review-dashboard voor de meta title/description audit.

Workflow:
  1. Bulk generator heeft al suggested_meta_title/description gevuld in Supabase
  2. Hier scroll je door producten, keur je goed / bewerk / skip
  3. Approved producten gaan naar Shopify via execution/meta_audit_push.py (nog te bouwen)

Starten:
    streamlit run meta_audit_dashboard.py
"""

import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Meta Audit Review",
    page_icon="📝",
    layout="wide",
)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")

TITLE_MAX = 58
DESC_MAX = 155
DESC_MIN = 120


# ── Supabase ──────────────────────────────────────────────────────────────────

@st.cache_resource
def get_supabase():
    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_counts(sb) -> dict:
    """Fetch aggregate counts per review_status."""
    out = {"total": 0, "pending": 0, "approved": 0, "skipped": 0,
           "exported": 0, "pushed": 0, "no_suggestion": 0}
    # total
    r = sb.table("shopify_meta_audit").select("review_status", count="exact").execute()
    out["total"] = r.count or 0
    # per status
    for status in ["pending", "approved", "skipped", "exported", "pushed"]:
        r = sb.table("shopify_meta_audit").select("id", count="exact")\
            .eq("review_status", status)\
            .not_.is_("suggested_meta_title", "null").execute()
        out[status] = r.count or 0
    # with no suggestion yet (bulk still running)
    r = sb.table("shopify_meta_audit").select("id", count="exact")\
        .is_("suggested_meta_title", "null").execute()
    out["no_suggestion"] = r.count or 0
    return out


def fetch_vendors(sb) -> list[str]:
    """List of distinct vendors (alphabetical)."""
    all_rows, offset, page = [], 0, 1000
    while True:
        r = sb.table("shopify_meta_audit").select("vendor")\
            .range(offset, offset + page - 1).execute().data
        if not r:
            break
        all_rows.extend(r)
        if len(r) < page:
            break
        offset += page
    vendors = sorted({row["vendor"] for row in all_rows if row.get("vendor")})
    return vendors


def fetch_products(sb, vendor: str | None, status: str, search: str,
                   only_with_suggestion: bool = True) -> list[dict]:
    q = sb.table("shopify_meta_audit").select("*")
    if only_with_suggestion:
        q = q.not_.is_("suggested_meta_title", "null")
    if vendor and vendor != "(alle)":
        q = q.eq("vendor", vendor)
    if status and status != "(alle)":
        q = q.eq("review_status", status)
    if search:
        q = q.ilike("product_title", f"%{search}%")
    q = q.order("vendor").order("product_title")
    rows, offset, page = [], 0, 1000
    while True:
        batch = q.range(offset, offset + page - 1).execute().data
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page:
            break
        offset += page
    return rows


def update_product(sb, pid: str, updates: dict):
    sb.table("shopify_meta_audit").update(updates)\
        .eq("shopify_product_id", pid).execute()


def approve_product(sb, pid: str, title: str, desc: str):
    update_product(sb, pid, {
        "approved_title": title,
        "approved_desc": desc,
        "review_status": "approved",
        "approved_at": datetime.now(timezone.utc).isoformat(),
    })


def bulk_approve_pending(sb, products: list[dict]) -> int:
    """Approve alle pending producten in de gegeven lijst. Gebruikt suggested_* als
    approved_*. Geeft aantal daadwerkelijk goedgekeurde producten terug."""
    to_approve = [
        p for p in products
        if p.get("review_status") == "pending"
        and p.get("suggested_meta_title")
    ]
    if not to_approve:
        return 0
    now = datetime.now(timezone.utc).isoformat()
    # Individueel updaten want approved_title/desc verschillen per product
    for p in to_approve:
        sb.table("shopify_meta_audit").update({
            "approved_title": p["suggested_meta_title"],
            "approved_desc": p.get("suggested_meta_description") or p.get("current_meta_description"),
            "review_status": "approved",
            "approved_at": now,
        }).eq("shopify_product_id", p["shopify_product_id"]).execute()
    return len(to_approve)


def skip_product(sb, pid: str):
    update_product(sb, pid, {"review_status": "skipped"})


def reset_product(sb, pid: str):
    update_product(sb, pid, {
        "approved_title": None, "approved_desc": None,
        "review_status": "pending", "approved_at": None,
    })


def fetch_approved_for_export(sb, vendor: str | None, limit: int,
                              include_already_exported: bool) -> list[dict]:
    """Haal producten op, oudste approvals eerst. Excludeer al geëxporteerde tenzij include=True."""
    rows, offset, page = [], 0, 1000
    while True:
        q = sb.table("shopify_meta_audit").select(
            "shopify_product_id, handle, product_title, vendor, "
            "current_meta_title, current_meta_description, "
            "approved_title, approved_desc, approved_at, review_status, export_batch"
        )
        if include_already_exported:
            # Alles met approved_title (dus zowel approved, exported als pushed)
            q = q.in_("review_status", ["approved", "exported", "pushed"])
        else:
            q = q.eq("review_status", "approved")
        if vendor and vendor != "(alle)":
            q = q.eq("vendor", vendor)
        q = q.order("approved_at")
        batch = q.range(offset, offset + page - 1).execute().data
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page:
            break
        offset += page
    return rows[:limit]


def mark_exported(sb, pids: list[str], batch_name: str):
    """Zet review_status='exported' en export_batch voor een lijst product IDs.
    Als review_status al 'pushed' is, blijft dat staan (dan is batch alleen een label)."""
    for i in range(0, len(pids), 100):
        chunk = pids[i:i + 100]
        # Alleen status upgraden als het nog 'approved' is — pushed laten staan
        sb.table("shopify_meta_audit").update({
            "review_status": "exported",
            "export_batch": batch_name,
        }).in_("shopify_product_id", chunk).eq("review_status", "approved").execute()
        # Voor al geëxporteerde/pushed: alleen batch-label bijwerken
        sb.table("shopify_meta_audit").update({
            "export_batch": batch_name,
        }).in_("shopify_product_id", chunk).in_("review_status",
                                                ["exported", "pushed"]).execute()


def fetch_export_batches(sb) -> list[dict]:
    """Geef alle batches terug met counts, oudste eerst."""
    rows, offset, page = [], 0, 1000
    while True:
        r = sb.table("shopify_meta_audit").select("export_batch")\
            .not_.is_("export_batch", "null")\
            .range(offset, offset + page - 1).execute().data
        if not r:
            break
        rows.extend(r)
        if len(r) < page:
            break
        offset += page
    from collections import Counter
    counts = Counter(r["export_batch"] for r in rows if r.get("export_batch"))
    return [{"batch_name": name, "count": c}
            for name, c in sorted(counts.items())]


def fetch_rows_for_batch(sb, batch_name: str) -> list[dict]:
    """Haal alle producten op voor een specifieke batch (voor re-download)."""
    rows, offset, page = [], 0, 1000
    while True:
        r = sb.table("shopify_meta_audit").select(
            "shopify_product_id, handle, product_title, vendor, "
            "current_meta_title, current_meta_description, "
            "approved_title, approved_desc"
        ).eq("export_batch", batch_name)\
         .range(offset, offset + page - 1).execute().data
        if not r:
            break
        rows.extend(r)
        if len(r) < page:
            break
        offset += page
    return rows


@st.cache_data
def load_sku_map() -> dict:
    """Laad SKU-map uit de Shopify-export Excel. Product ID → SKU."""
    import pandas as pd
    df = pd.read_excel(
        "master files/Alle Active Producten.xlsx"
    ).drop_duplicates(subset=["Product ID"])
    out = {}
    for _, r in df.iterrows():
        pid = r["Product ID"]
        if pd.isna(pid):
            continue
        try:
            pid = str(int(float(pid)))
        except Exception:
            continue
        sku = r.get("Variant SKU")
        if pd.notna(sku) and str(sku).strip():
            out[pid] = str(sku).strip()
    return out


def build_csv_bytes(rows: list[dict], mode: str) -> bytes:
    """mode = 'import' (new values) or 'rollback' (current values)."""
    import csv, io
    sku_map = load_sku_map()
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Handle", "Product ID", "SKU", "Title", "Vendor",
                "SEO Title", "SEO Description"])
    for r in rows:
        if mode == "import":
            t, d = r.get("approved_title") or "", r.get("approved_desc") or ""
        else:
            t, d = r.get("current_meta_title") or "", r.get("current_meta_description") or ""
        w.writerow([
            r["handle"], r["shopify_product_id"],
            sku_map.get(r["shopify_product_id"], ""),
            r.get("product_title") or "", r.get("vendor") or "",
            t, d,
        ])
    return buf.getvalue().encode("utf-8-sig")  # BOM voor Excel-compat


# ── UI ────────────────────────────────────────────────────────────────────────

sb = get_supabase()

st.title("📝 Meta Audit Review")
st.caption("Beoordeel voorgestelde meta titles en descriptions voor Shopify.")

with st.expander("💡 Tips voor snel reviewen", expanded=False):
    st.markdown(
        "1. **Filter op 1 vendor tegelijk** — dan zie je patronen en kun je "
        "snel beslissen (Serax heeft een andere stijl dan Printworks).\n"
        "2. **Bulk-approve wat er goed uitziet** — als een hele pagina er "
        "strak uit ziet, klik **`Approve alle X op deze pagina`** i.p.v. per product.\n"
        "3. **Kleine tweak nodig?** Bewerk direct in de rechter textarea "
        "(title of desc), de char-counter wordt live geüpdatet — klik dan "
        "**Approve**. De bewerkte versie wordt opgeslagen als goedgekeurd."
    )

# Top stats
with st.container(border=True):
    counts = fetch_counts(sb)
    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
    c1.metric("Totaal", counts["total"])
    c2.metric("Geen voorstel", counts["no_suggestion"])
    c3.metric("Pending", counts["pending"])
    c4.metric("Approved", counts["approved"])
    c5.metric("Skipped", counts["skipped"])
    c6.metric("Exported", counts["exported"])
    c7.metric("Pushed", counts["pushed"])
    if counts["no_suggestion"] > 0:
        st.info(f"⏳ Bulk generator draait nog — {counts['no_suggestion']} producten wachten op voorstel.")

# ── Export sectie ─────────────────────────────────────────────────────────────
with st.expander(
    f"📥 Export naar CSV (Hextom) — {counts['approved']} klaar voor export, "
    f"{counts['exported']} al geëxporteerd",
    expanded=False,
):
    st.markdown(
        "Exporteer goedgekeurde producten als CSV voor import via Hextom "
        "Bulk Product Edit. Er worden **2 bestanden** gegenereerd: één voor "
        "import, één als rollback (bewaar dit!)."
    )

    # Include-toggle moet bovenaan zodat max-slider correct is
    include_already = st.checkbox(
        "Includeer al geëxporteerde producten",
        value=False,
        help="Standaard exporteer je alleen NIEUWE approvals. "
             "Zet aan om ook al eerder geëxporteerde producten mee te nemen.",
        key="exp_include",
    )

    # Bepaal max_available op basis van toggle
    if include_already:
        max_available = counts["approved"] + counts["exported"] + counts["pushed"]
    else:
        max_available = counts["approved"]

    ec1, ec2, ec3 = st.columns([2, 2, 2])
    with ec1:
        export_vendor = st.selectbox(
            "Vendor", ["(alle)"] + fetch_vendors(sb), key="exp_vendor"
        )
    with ec2:
        default_batch = min(200, max_available) if max_available > 0 else 1
        export_n = st.number_input(
            f"Aantal producten (max {max_available} beschikbaar)",
            min_value=1, max_value=max(1, max_available),
            value=default_batch if max_available > 0 else 1,
            step=50, key="exp_n",
        )
    with ec3:
        from datetime import date
        default_batch_name = f"batch_{date.today().isoformat()}"
        batch_name = st.text_input(
            "Batch-naam",
            value=default_batch_name,
            help="Label voor deze export. Wordt opgeslagen per product "
                 "zodat je later deze batch kan terugvinden.",
            key="exp_batch_name",
        )

    if max_available == 0:
        st.warning("Geen producten beschikbaar. "
                   "Keur eerst producten goed hierboven "
                   "(of zet 'Includeer al geëxporteerde' aan).")
    else:
        if st.button("🔨 Genereer CSV-bestanden",
                     type="primary", key="exp_generate",
                     disabled=not batch_name.strip()):
            with st.spinner(f"Ophalen {export_n} producten..."):
                vendor_filter = None if export_vendor == "(alle)" else export_vendor
                export_rows = fetch_approved_for_export(
                    sb, vendor_filter, int(export_n), include_already
                )
                if not export_rows:
                    st.error("Geen producten gevonden met deze filters.")
                else:
                    st.session_state.exp_rows = export_rows
                    st.session_state.exp_import_csv = build_csv_bytes(
                        export_rows, "import"
                    )
                    st.session_state.exp_rollback_csv = build_csv_bytes(
                        export_rows, "rollback"
                    )
                    st.session_state.exp_batch = batch_name.strip()
                    # Mark met batch-naam
                    pids = [r["shopify_product_id"] for r in export_rows]
                    mark_exported(sb, pids, batch_name.strip())
                    st.success(
                        f"✅ {len(export_rows)} producten geëxporteerd in "
                        f"batch **'{batch_name.strip()}'**. Download hieronder."
                    )

        # Show download buttons if CSV is ready
        if st.session_state.get("exp_import_csv"):
            from datetime import datetime as _dt
            stamp = _dt.now().strftime("%Y%m%d_%H%M")
            n = len(st.session_state.get("exp_rows", []))
            dc1, dc2 = st.columns(2)
            with dc1:
                st.download_button(
                    f"⬇️ Import CSV ({n} rijen) — push naar Hextom",
                    data=st.session_state.exp_import_csv,
                    file_name=f"meta_audit_import_{stamp}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
            with dc2:
                st.download_button(
                    f"⬇️ Rollback CSV ({n} rijen) — bewaar!",
                    data=st.session_state.exp_rollback_csv,
                    file_name=f"meta_audit_rollback_{stamp}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
            st.caption(
                "💡 Na succesvolle import in Hextom + controle op live site: "
                "klik hieronder om deze batch als 'pushed' te markeren."
            )
            if st.button("✅ Markeer batch als 'pushed' (live op Shopify)",
                         key="exp_mark_pushed"):
                pids = [r["shopify_product_id"]
                        for r in st.session_state.get("exp_rows", [])]
                now = datetime.now(timezone.utc).isoformat()
                for i in range(0, len(pids), 100):
                    chunk = pids[i:i + 100]
                    sb.table("shopify_meta_audit").update({
                        "review_status": "pushed", "pushed_at": now,
                    }).in_("shopify_product_id", chunk).execute()
                st.success(f"{len(pids)} producten gemarkeerd als 'pushed'.")
                for k in ("exp_rows", "exp_import_csv", "exp_rollback_csv", "exp_batch"):
                    st.session_state.pop(k, None)
                st.rerun()

    # ── Batch-historie ─────────────────────────────────────────────────
    st.divider()
    st.markdown("#### 📚 Batch-historie")
    batches = fetch_export_batches(sb)
    if not batches:
        st.caption("Nog geen batches. Exports die je maakt komen hier te staan.")
    else:
        st.caption(
            f"{len(batches)} batches in totaal. Je kunt elke batch opnieuw "
            f"downloaden (bv. om een rollback-CSV terug te vinden)."
        )
        for b in batches:
            bc1, bc2, bc3 = st.columns([3, 1, 2])
            with bc1:
                st.markdown(f"**{b['batch_name']}** — {b['count']} producten")
            with bc2:
                if st.button("📥 Laad", key=f"load_{b['batch_name']}"):
                    rows = fetch_rows_for_batch(sb, b["batch_name"])
                    st.session_state[f"batch_import_{b['batch_name']}"] = \
                        build_csv_bytes(rows, "import")
                    st.session_state[f"batch_rollback_{b['batch_name']}"] = \
                        build_csv_bytes(rows, "rollback")
                    st.session_state[f"batch_count_{b['batch_name']}"] = len(rows)
                    st.rerun()
            with bc3:
                if st.session_state.get(f"batch_import_{b['batch_name']}"):
                    n = st.session_state[f"batch_count_{b['batch_name']}"]
                    bd1, bd2 = st.columns(2)
                    with bd1:
                        st.download_button(
                            f"Import ({n})",
                            data=st.session_state[f"batch_import_{b['batch_name']}"],
                            file_name=f"{b['batch_name']}_import.csv",
                            mime="text/csv",
                            key=f"dl_imp_{b['batch_name']}",
                            use_container_width=True,
                        )
                    with bd2:
                        st.download_button(
                            f"Rollback ({n})",
                            data=st.session_state[f"batch_rollback_{b['batch_name']}"],
                            file_name=f"{b['batch_name']}_rollback.csv",
                            mime="text/csv",
                            key=f"dl_rb_{b['batch_name']}",
                            use_container_width=True,
                        )

# Filters
st.subheader("Filters")
fc1, fc2, fc3, fc4 = st.columns([2, 2, 3, 1])
with fc1:
    vendors = ["(alle)"] + fetch_vendors(sb)
    vendor = st.selectbox("Vendor", vendors, key="flt_vendor")
with fc2:
    status = st.selectbox(
        "Status",
        ["pending", "approved", "exported", "skipped", "pushed", "(alle)"],
        key="flt_status"
    )
with fc3:
    search = st.text_input("Zoek in productnaam", key="flt_search", placeholder="bv. 'placemat'")
with fc4:
    per_page = st.selectbox("Per pagina", [10, 20, 50], index=1, key="flt_pp")

products = fetch_products(sb, vendor, status, search)
st.write(f"**{len(products)} producten** voldoen aan de filter")

# ── Bulk approve sectie (scoped op huidige filter) ────────────────────────────
pending_in_filter = [p for p in products if p.get("review_status") == "pending"
                     and p.get("suggested_meta_title")]

if pending_in_filter:
    scope_label = (
        f"vendor '{vendor}'" if vendor and vendor != "(alle)"
        else "alle vendors"
    )
    if search:
        scope_label += f" + zoekterm '{search}'"

    with st.expander(
        f"⚡ Bulk-approve — {len(pending_in_filter)} pending producten "
        f"in huidige filter ({scope_label})",
        expanded=False,
    ):
        st.warning(
            f"Dit keurt **{len(pending_in_filter)} producten** in één keer goed "
            f"(alleen status='pending' binnen de huidige filter). "
            f"Reeds approved/skipped/exported blijven zoals ze zijn."
        )
        confirm = st.checkbox(
            f"Ja, ik heb de voorstellen bekeken en wil alle "
            f"{len(pending_in_filter)} producten goedkeuren",
            key="bulk_approve_confirm",
        )
        if st.button(
            f"✅ Approve alle {len(pending_in_filter)} pending producten",
            type="primary", disabled=not confirm, key="bulk_approve_btn",
        ):
            with st.spinner(f"Goedkeuren {len(pending_in_filter)} producten..."):
                n = bulk_approve_pending(sb, pending_in_filter)
            st.success(f"✅ {n} producten goedgekeurd.")
            st.session_state.pop("bulk_approve_confirm", None)
            st.rerun()

# Pagination
if "page_idx" not in st.session_state:
    st.session_state.page_idx = 0

total_pages = max(1, (len(products) + per_page - 1) // per_page)
# Reset page index if out of range
st.session_state.page_idx = min(st.session_state.page_idx, total_pages - 1)

pc1, pc2, pc3 = st.columns([1, 2, 1])
with pc1:
    if st.button("◀ Vorige", disabled=st.session_state.page_idx == 0):
        st.session_state.page_idx -= 1
        st.rerun()
with pc2:
    st.markdown(
        f"<div style='text-align:center;padding-top:8px'>"
        f"Pagina <b>{st.session_state.page_idx + 1}</b> / {total_pages}</div>",
        unsafe_allow_html=True,
    )
with pc3:
    if st.button("Volgende ▶", disabled=st.session_state.page_idx >= total_pages - 1):
        st.session_state.page_idx += 1
        st.rerun()

# Bulk approve huidige pagina
start = st.session_state.page_idx * per_page
end = start + per_page
page_products = products[start:end]

bulk_col = st.columns([3, 1, 1])
with bulk_col[1]:
    if st.button(f"✅ Approve alle {len(page_products)} op deze pagina",
                 disabled=not page_products, use_container_width=True):
        for p in page_products:
            if p.get("review_status") == "pending":
                approve_product(
                    sb, p["shopify_product_id"],
                    p["suggested_meta_title"], p["suggested_meta_description"]
                )
        st.success(f"{len(page_products)} producten approved.")
        st.rerun()
with bulk_col[2]:
    if st.button("Refresh", use_container_width=True):
        st.rerun()

st.divider()

# Per-product cards
for p in page_products:
    pid = p["shopify_product_id"]
    key_prefix = f"p_{pid}"
    review_status = p.get("review_status", "pending")

    status_badge = {
        "pending": "🟡 Pending",
        "approved": "✅ Approved",
        "exported": "📥 Exported",
        "skipped": "⏭️ Skipped",
        "pushed": "🚀 Pushed",
    }.get(review_status, review_status)

    with st.container(border=True):
        header_col = st.columns([5, 1])
        with header_col[0]:
            st.markdown(f"**[{p.get('vendor') or '(geen vendor)'}] "
                        f"{p['product_title']}** · {status_badge}")
            st.caption(f"handle: `{p['handle']}` · id: `{pid}`")
        with header_col[1]:
            shopify_url = f"https://interieur-shop.nl/products/{p['handle']}"
            st.markdown(f"[🔗 Bekijk op website]({shopify_url})")

        # Initial approval values (suggested if not yet edited)
        initial_title = p.get("approved_title") or p.get("suggested_meta_title") or ""
        initial_desc = p.get("approved_desc") or p.get("suggested_meta_description") or ""

        # Side-by-side columns
        left, right = st.columns(2)

        with left:
            st.markdown("##### Huidige Shopify")
            st.text_area(
                f"Title ({p.get('current_title_length') or 0} chars) "
                f"[{p.get('title_status') or '-'}]",
                p.get("current_meta_title") or "(leeg)",
                key=f"{key_prefix}_cur_t",
                disabled=True, height=80,
            )
            st.text_area(
                f"Description ({p.get('current_desc_length') or 0} chars) "
                f"[{p.get('desc_status') or '-'}]",
                p.get("current_meta_description") or "(leeg)",
                key=f"{key_prefix}_cur_d",
                disabled=True, height=120,
            )

        with right:
            st.markdown("##### Voorstel (bewerkbaar)")
            new_title = st.text_area(
                f"Title (max {TITLE_MAX})",
                initial_title, key=f"{key_prefix}_new_t",
                height=80,
            )
            tl = len(new_title)
            tl_color = "🟢" if tl <= TITLE_MAX else "🔴"
            st.caption(f"{tl_color} {tl} chars")

            new_desc = st.text_area(
                f"Description ({DESC_MIN}-{DESC_MAX})",
                initial_desc, key=f"{key_prefix}_new_d",
                height=120,
            )
            dl = len(new_desc)
            if dl < DESC_MIN:
                dl_color = "🟠"
            elif dl > DESC_MAX:
                dl_color = "🔴"
            else:
                dl_color = "🟢"
            st.caption(f"{dl_color} {dl} chars")

        # Actions
        a1, a2, a3, a4 = st.columns(4)
        with a1:
            if st.button("✅ Approve", key=f"{key_prefix}_approve",
                         type="primary", use_container_width=True):
                approve_product(sb, pid, new_title, new_desc)
                st.rerun()
        with a2:
            if st.button("⏭️ Skip", key=f"{key_prefix}_skip",
                         use_container_width=True):
                skip_product(sb, pid)
                st.rerun()
        with a3:
            if review_status in ("approved", "skipped"):
                if st.button("↩️ Reset", key=f"{key_prefix}_reset",
                             use_container_width=True):
                    reset_product(sb, pid)
                    st.rerun()
        with a4:
            st.write("")  # spacer

        if p.get("approved_at"):
            st.caption(f"Goedgekeurd op {p['approved_at']}")
