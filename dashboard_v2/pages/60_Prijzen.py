"""Prijzen updaten — nieuwe supplier prijslijst → DB + Hextom bulk + CRX overview."""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import streamlit as st

from ui.layout import explainer, page_header

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))


page_header("💶 Prijzen updaten")

explainer(
    "Upload een nieuwe leverancier-prijslijst. Script matcht op SKU, past de "
    "**giftbox-regel** toe (qty > 1 → ALTIJD giftbox-prijs, NOOIT stuksprijs), "
    "en produceert: (1) DB update, (2) Hextom bulk-price Excel, (3) CRX overview."
)


# ── Giftbox test ─────────────────────────────────────────────────────────────

with st.expander("🧪 Test giftbox-logica (4 cases)"):
    st.caption(
        "Deze test draait de giftbox-rule op 4 bekende cases om te verifiëren dat "
        "update_prices.py zich correct gedraagt vóór een echte run."
    )
    if st.button("▶️ Run giftbox tests"):
        try:
            from execution.update_prices import extract_prices
        except ImportError:
            st.error("extract_prices helper niet gevonden in update_prices.py.")
            st.stop()

        cases = [
            ("qty=1 single",      {"giftbox_qty": 1, "rrp_stuk_eur": 12.5, "rrp_gb_eur": None}, 12.5),
            ("qty=2 giftbox",     {"giftbox_qty": 2, "rrp_stuk_eur": 10.0, "rrp_gb_eur": 19.5}, 19.5),
            ("qty=3 geen GB-prijs", {"giftbox_qty": 3, "rrp_stuk_eur": 8.0, "rrp_gb_eur": None}, None),
            ("qty=1 zonder stuksprijs", {"giftbox_qty": 1, "rrp_stuk_eur": None, "rrp_gb_eur": 20.0}, None),
        ]
        results = []
        try:
            for name, inp, expected in cases:
                try:
                    got = extract_prices(inp)
                    if isinstance(got, tuple):
                        got = got[0] if got else None
                except Exception as e:
                    got = f"error: {e}"
                ok = got == expected
                results.append({"case": name, "expected": expected, "got": got, "pass": "✅" if ok else "❌"})
            import pandas as pd
            st.dataframe(pd.DataFrame(results), hide_index=True, width="stretch")
        except Exception as e:
            st.warning(f"extract_prices signature verschilt van verwacht — test handmatig: {e}")


# ── Upload & run ─────────────────────────────────────────────────────────────

st.divider()

uploaded = st.file_uploader("Nieuwe prijslijst (.xlsx)", type=["xlsx", "xls"], key="pr_file")

c1, c2 = st.columns(2)
with c1:
    dry_run = st.button("🧪 Dry-run (alleen rapporteren)", width="stretch", disabled=uploaded is None)
with c2:
    real_run = st.button("🚀 Run (schrijft naar DB + maakt Excels)", type="primary",
                          width="stretch", disabled=uploaded is None)


def _run(dry: bool):
    from execution.update_prices import run_price_update

    tmpdir = Path(tempfile.mkdtemp(prefix="pr_"))
    path = tmpdir / uploaded.name
    path.write_bytes(uploaded.getvalue())

    prog = st.progress(0.0)
    log_area = st.empty()
    log_lines: list[str] = []
    def _log(msg): log_lines.append(str(msg)); log_area.code("\n".join(log_lines[-30:]))
    def _prog(i, n, msg=""): prog.progress(min(max(i / max(n, 1), 0.0), 1.0)); msg and _log(msg)

    try:
        result = run_price_update(
            file_path=str(path),
            dry_run=dry,
            output_dir="./exports",
            progress=_prog,
            logger=_log,
        )
        m1, m2, m3 = st.columns(3)
        m1.metric("Matched", getattr(result, "matched_count", 0))
        m2.metric("Updated", getattr(result, "updated_count", 0))
        m3.metric("Niet gevonden", len(getattr(result, "not_found_rows", []) or []))

        if dry:
            st.info("🧪 Dry-run: geen DB writes, geen Excels geschreven.")
        else:
            hex_path = Path(getattr(result, "hextom_xlsx_path", "") or "")
            crx_path = Path(getattr(result, "crx_xlsx_path", "") or "")
            if hex_path.exists():
                st.download_button(f"📥 Hextom bulk ({hex_path.name})", data=hex_path.read_bytes(),
                                   file_name=hex_path.name)
            if crx_path.exists():
                st.download_button(f"📥 CRX overview ({crx_path.name})", data=crx_path.read_bytes(),
                                   file_name=crx_path.name)

        nf = getattr(result, "not_found_rows", []) or []
        if nf:
            with st.expander(f"⚠️ {len(nf)} SKUs niet gevonden in DB"):
                st.json(nf[:50])
    except Exception as e:
        st.error(f"❌ {e}")
        import traceback
        with st.expander("Traceback"):
            st.code(traceback.format_exc())


if dry_run:
    _run(dry=True)
if real_run:
    _run(dry=False)
