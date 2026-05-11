"""Quick Update — upload bestand, geef NL-instructie, download aangepast bestand.

Voor één-shot correcties op een bestaande export. Bv:
  - "Alle Vendor naar Serax behalve waar Titel 'Advies' bevat"
  - "Verwijder Ferd Ridge en Horace Ridge uit alle titels"
  - "Zet meta_description leeg voor SKU's beginnend met test_"

Sonnet parset de instructie naar een lijst gestructureerde operaties.
Python past ze toe (geen LLM-execution).
"""
from __future__ import annotations

import io
import json
import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()


SYSTEM = """Je vertaalt gebruiker-instructies naar gestructureerde DataFrame-operaties.

Input: kolommen + sample-rijen + een instructie in normaal Nederlands.
Output: JSON met een lijst operaties.

Beschikbare operatie-types:
- set     — zet een kolom op een vaste waarde (eventueel met filter)
- replace — vervang tekst-substring in een kolom (eventueel met filter)
- strip   — verwijder woorden uit een kolom
- prefix  — zet tekst voor een waarde
- suffix  — zet tekst achter een waarde
- clear   — maak kolom leeg
- copy    — kopieer waarde van kolom A naar kolom B
- drop_row — verwijder rij (alleen met filter)

Filter-structuur (optioneel — zonder filter = alle rijen):
{
  "column": "Kolomnaam",
  "match": "contains" | "equals" | "starts_with" | "ends_with" | "regex" | "is_empty" | "not_empty",
  "value": "tekst",
  "negate": false,
  "case_sensitive": false
}

Output JSON:
{
  "operations": [
    {
      "type": "set",
      "target": "Vendor",
      "value": "Serax",
      "filter": {"column": "Titel", "match": "contains", "value": "Advies", "negate": true}
    }
  ],
  "explanation": "Eén-zin uitleg van wat er gaat gebeuren",
  "affected_estimate": "geschat aantal rijen, of 'alle'"
}

Belangrijk:
- Gebruik EXACT de kolomnamen uit de input.
- Bij twijfel: kies de meest expliciete operation, voeg een filter toe om collateral damage te voorkomen.
- Als instructie onduidelijk is: return {"operations": [], "explanation": "...", "needs_clarification": "wat onduidelijk is"}

Geef ALLEEN valide JSON, geen markdown."""


def _read_upload(uploaded) -> tuple[pd.DataFrame, str]:
    """Lees CSV of Excel; return (df, format_string)."""
    naam = uploaded.name.lower()
    raw = uploaded.read()
    if naam.endswith((".xlsx", ".xls")):
        df = pd.read_excel(io.BytesIO(raw), dtype=str, keep_default_na=False)
        return df, "xlsx"
    # CSV — probeer UTF-8-sig (Excel-export met BOM), fallback UTF-8 en cp1252
    for enc in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            df = pd.read_csv(io.BytesIO(raw), dtype=str, keep_default_na=False, encoding=enc)
            return df, "csv"
        except UnicodeDecodeError:
            continue
    raise ValueError("Kon bestand niet lezen — onbekende encoding.")


def _interpret(headers: list[str], sample_rows: list[dict], instruction: str) -> dict | None:
    import anthropic
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))

    user = (
        f"Kolommen: {headers}\n\n"
        f"Sample (eerste {len(sample_rows)} rijen):\n"
        f"{json.dumps(sample_rows, ensure_ascii=False, indent=2)}\n\n"
        f"Instructie van gebruiker:\n{instruction}"
    )
    try:
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1500,
            system=SYSTEM,
            messages=[{"role": "user", "content": user}],
        )
        text = resp.content[0].text.strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text.strip())
    except Exception as e:
        st.error(f"Parse-fout: {e}")
        return None


def _build_mask(df: pd.DataFrame, flt: dict | None) -> pd.Series:
    """Bouw een boolean-mask op basis van filter-spec."""
    if not flt:
        return pd.Series([True] * len(df), index=df.index)

    col = flt.get("column", "")
    if col not in df.columns:
        return pd.Series([False] * len(df), index=df.index)

    match = (flt.get("match") or "contains").lower()
    val = flt.get("value", "")
    negate = bool(flt.get("negate", False))
    cs = bool(flt.get("case_sensitive", False))

    serie = df[col].fillna("").astype(str)
    if not cs:
        serie_l = serie.str.lower()
        val_l = str(val).lower()
    else:
        serie_l = serie
        val_l = str(val)

    if match == "contains":
        mask = serie_l.str.contains(re.escape(val_l), regex=True, na=False)
    elif match == "equals":
        mask = serie_l == val_l
    elif match == "starts_with":
        mask = serie_l.str.startswith(val_l, na=False)
    elif match == "ends_with":
        mask = serie_l.str.endswith(val_l, na=False)
    elif match == "regex":
        try:
            mask = serie_l.str.contains(val_l, regex=True, na=False)
        except re.error:
            mask = pd.Series([False] * len(df), index=df.index)
    elif match == "is_empty":
        mask = (serie.str.strip() == "")
    elif match == "not_empty":
        mask = (serie.str.strip() != "")
    else:
        mask = pd.Series([False] * len(df), index=df.index)

    return ~mask if negate else mask


def _apply_op(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, int]:
    """Pas één operatie toe. Return: (df, aantal_geraakt)."""
    tp = (op.get("type") or "").lower()
    target = op.get("target", "")
    val = op.get("value", "")
    flt = op.get("filter")
    mask = _build_mask(df, flt)
    raakt = int(mask.sum())

    if tp == "drop_row":
        return df[~mask].reset_index(drop=True), raakt

    if target and target not in df.columns:
        # nieuwe kolom toestaan voor set/copy
        if tp in ("set", "copy", "clear"):
            df[target] = ""
        else:
            return df, 0

    if tp == "set":
        df.loc[mask, target] = val
    elif tp == "clear":
        df.loc[mask, target] = ""
    elif tp == "replace":
        # value = {"from": "...", "to": "..."} of list daarvan
        items = val if isinstance(val, list) else [val] if isinstance(val, dict) else []
        for item in items:
            fr = str(item.get("from", "") if isinstance(item, dict) else "")
            to = str(item.get("to", "") if isinstance(item, dict) else "")
            if fr:
                df.loc[mask, target] = df.loc[mask, target].fillna("").astype(str).str.replace(
                    fr, to, regex=False
                )
    elif tp == "strip":
        woorden = val if isinstance(val, list) else [val]
        for w in woorden:
            w = str(w)
            if not w:
                continue
            pat = re.compile(rf"\s*[-–—]?\s*{re.escape(w)}\s*[-–—]?\s*", re.IGNORECASE)
            df.loc[mask, target] = df.loc[mask, target].fillna("").astype(str).apply(
                lambda x: re.sub(r"\s{2,}", " ", pat.sub(" ", x)).strip(" -–—")
            )
    elif tp == "prefix":
        df.loc[mask, target] = str(val) + df.loc[mask, target].fillna("").astype(str)
    elif tp == "suffix":
        df.loc[mask, target] = df.loc[mask, target].fillna("").astype(str) + str(val)
    elif tp == "copy":
        bron = op.get("source", "")
        if bron and bron in df.columns:
            df.loc[mask, target] = df.loc[mask, bron]

    return df, raakt


# ── Render ───────────────────────────────────────────────────────────────────

def render() -> None:
    st.subheader("Quick Update")
    st.caption(
        "Upload een Excel/CSV (bv. een Hextom-export), geef in normaal Nederlands aan "
        "wat er moet wijzigen, en download het aangepaste bestand. Geen LLM op je rijen — "
        "Sonnet parset alleen jouw instructie naar een operatie, Python past 'm toe."
    )

    uploaded = st.file_uploader(
        "Sleep een bestand hierin",
        type=["csv", "xlsx", "xls"],
        key="qu_upload",
    )
    if not uploaded:
        st.info("Wacht op upload…")
        return

    try:
        df_orig, fmt = _read_upload(uploaded)
    except Exception as e:
        st.error(f"Lees-fout: {e}")
        return

    if "qu_df" not in st.session_state or st.session_state.get("qu_filename") != uploaded.name:
        st.session_state["qu_df"] = df_orig.copy()
        st.session_state["qu_filename"] = uploaded.name
        st.session_state["qu_log"] = []

    df: pd.DataFrame = st.session_state["qu_df"]
    n_rows, n_cols = df.shape
    st.success(f"✅ {uploaded.name} — {n_rows} rijen, {n_cols} kolommen")

    with st.expander("Preview (eerste 10 rijen)", expanded=False):
        st.dataframe(df.head(10), use_container_width=True, hide_index=True)

    with st.expander("Kolomnamen", expanded=False):
        st.write(list(df.columns))

    st.markdown("---")
    st.markdown("**Wat moet er aangepast worden?**")
    st.caption(
        "Voorbeelden:\n"
        "- *Alle Vendor naar Serax behalve waar Titel 'Advies' bevat*\n"
        "- *Verwijder 'Ferd Ridge' en 'Horace Ridge' uit alle titels*\n"
        "- *Zet meta_description leeg voor SKU's beginnend met test_*\n"
        "- *In Product Description: vervang 'Ontdek' door 'Bekijk'*"
    )
    # Clear-flag uitvoeren VOOR widget wordt aangemaakt
    if st.session_state.pop("qu_instr_clear", False):
        st.session_state["qu_instr"] = ""

    instr = st.text_area("Instructie", height=100, key="qu_instr",
                          placeholder="bv. 'Alle Vendor naar Serax behalve waar Titel Advies bevat'")

    c1, c2, c3 = st.columns([1, 1, 3])
    with c1:
        do_apply = st.button("Pas toe", type="primary", disabled=not instr.strip(),
                              key="qu_apply")
    with c2:
        if st.button("↺ Reset", key="qu_reset"):
            st.session_state["qu_df"] = df_orig.copy()
            st.session_state["qu_log"] = []
            st.rerun()

    if do_apply and instr.strip():
        sample = df.head(8).to_dict(orient="records")
        with st.spinner("Sonnet parset instructie..."):
            parsed = _interpret(list(df.columns), sample, instr.strip())
        if not parsed:
            return

        if parsed.get("needs_clarification"):
            st.warning(f"Onduidelijk: {parsed['needs_clarification']}")
            return

        ops = parsed.get("operations") or []
        if not ops:
            st.warning("Geen operaties gegenereerd.")
            return

        st.markdown("**Plan:**")
        st.info(parsed.get("explanation", ""))
        with st.expander("Operaties (JSON)"):
            st.json(parsed)

        # Toepassen
        total_hit = 0
        df_new = df.copy()
        for op in ops:
            df_new, hit = _apply_op(df_new, op)
            total_hit += hit
            st.session_state["qu_log"].append({
                "ts": datetime.utcnow().isoformat()[:19],
                "instr": instr.strip(),
                "op": op,
                "hit": hit,
            })

        st.session_state["qu_df"] = df_new
        st.session_state["qu_instr_clear"] = True
        st.success(f"✅ {total_hit} cellen / rijen aangepast.")
        st.rerun()

    # ── Log van wijzigingen ──
    if st.session_state.get("qu_log"):
        with st.expander(f"Wijzigingslog ({len(st.session_state['qu_log'])} stappen)", expanded=False):
            for entry in reversed(st.session_state["qu_log"]):
                st.text(f"{entry['ts']} — {entry['hit']} hits — {entry['instr'][:80]}")

    # ── Huidige staat preview ──
    st.markdown("**Huidige staat (na wijzigingen):**")
    st.dataframe(df.head(15), use_container_width=True, hide_index=True)

    # ── Download ──
    st.markdown("---")
    base = Path(uploaded.name).stem
    if fmt == "xlsx":
        buf = io.BytesIO()
        df.to_excel(buf, index=False, engine="openpyxl")
        st.download_button(
            "💾 Download aangepast Excel",
            data=buf.getvalue(),
            file_name=f"{base}_aangepast.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
            key="qu_dl_x",
        )
    else:
        csv_bytes = df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        st.download_button(
            "💾 Download aangepast CSV (UTF-8 BOM)",
            data=csv_bytes,
            file_name=f"{base}_aangepast.csv",
            mime="text/csv",
            type="primary",
            key="qu_dl_c",
        )
