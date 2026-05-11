"""Herverwerk-pipeline — stap-voor-stap wizard.

Stap 1 — Namen:        Haiku vertaalt namen → jij keurt goed / past aan
Stap 2 — Categorieën:  mapping-tabel + materiaal/kleur → jij keurt goed / koppelt
Stap 3 — Meta:         Sonnet schrijft descriptions → jij keurt goed / past aan
Stap 4 — Opslaan:      schrijf naar products_curated + download Hextom Excel

Foto's, EAN en barcodes worden nooit aangeraakt.
"""
from __future__ import annotations

import io
import json
import os
import re
import sys
import uuid
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

_ROOT = Path(__file__).resolve().parent.parent / "dashboard_v2"
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


@st.cache_resource
def _get_sb():
    from supabase import create_client
    url = os.getenv("SUPABASE_NEW_URL", "")
    key = os.getenv("SUPABASE_NEW_SERVICE_KEY", "") or os.getenv("SUPABASE_NEW_KEY", "")
    if not url or not key:
        raise RuntimeError("SUPABASE_NEW_URL ontbreekt.")
    return create_client(url, key)


# ── Voortgangsbalk ────────────────────────────────────────────────────────────

def _voortgang(stap: int) -> None:
    stappen = ["1. Namen", "2. Categorie & kleur", "3. Meta description", "4. Opslaan"]
    cols = st.columns(len(stappen))
    for i, (col, label) in enumerate(zip(cols, stappen), 1):
        if i < stap:
            col.markdown(f"<div style='text-align:center;color:#4F7A4A;font-size:13px'>✅ {label}</div>",
                         unsafe_allow_html=True)
        elif i == stap:
            col.markdown(f"<div style='text-align:center;font-weight:600;font-size:13px'>▶ {label}</div>",
                         unsafe_allow_html=True)
        else:
            col.markdown(f"<div style='text-align:center;color:#aaa;font-size:13px'>{label}</div>",
                         unsafe_allow_html=True)
    st.divider()


# ── Chat-correctie per stap ───────────────────────────────────────────────────

_CHAT_PROMPTS = {
    1: """Stap: titel-vertaling.
Beschikbare rule_types:
- title_strip — verwijder woorden/namen uit alle titels (bv. collectie-namen die per ongeluk in de Engelse titel staan)
  action: {"strip": ["Ferd Ridge", "Horace Ridge"]}
- title_replace — vervang X door Y in alle titels
  action: {"replace": [{"from": "...", "to": "..."}]}
- title_instruction — vrije regel voor toekomstige Haiku-vertalingen
  action: {"instruction": "Behoud nooit de collectie-naam in de titel"}""",
    2: """Stap: categorie + materiaal + kleur.
Beschikbare rule_types:
- name_rule — als productnaam zoekwoord bevat → sub_subcategorie
  action: {"zoekwoord": "...", "sub_subcategorie": "...", "is_extra": false}
  (is_extra=true betekent: voeg toe als tweede categorie i.p.v. overschrijven)
- name_rule_bulk — meerdere regels tegelijk
  action: {"regels": [{"zoekwoord": "...", "sub_subcategorie": "...", "is_extra": false}, ...]}
- translation — en→nl voor materiaal of kleur
  action: {"veld": "materiaal" of "kleur", "en": "...", "nl": "..."}
- category_override — voor specifieke SKU's één categorie zetten (eventueel met 2e subcat)
  action: {"skus": ["..."], "hoofdcategorie": "...", "subcategorie": "...", "sub_subcategorie": "...", "sub_subcategorie_2": "..."}""",
    3: """Stap: meta description.
Beschikbare rule_types:
- meta_replace — vervang X door Y in alle meta descriptions
  action: {"replace": [{"from": "...", "to": "..."}]}
- meta_instruction — vrije regel voor toekomstige Sonnet meta-generatie
  action: {"instruction": "Begin nooit met 'Ontdek'"}""",
}

_CHAT_STAP_NAAM = {1: "titel", 2: "categorie", 3: "meta"}


def _interpret_chat(stap: int, input_text: str, voorbeelden: list[str]) -> dict | None:
    """Vraag Sonnet om NL-feedback te parsen naar een gestructureerde regel."""
    import anthropic
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))

    system = f"""Je parset gebruikerfeedback naar JSON-regels voor de SEOkitchen pipeline.

{_CHAT_PROMPTS[stap]}

Output JSON:
{{
  "rule_type": "...",
  "action": {{...}},
  "scope": "alle",
  "explanation": "korte uitleg in 1 zin van wat je gaat doen"
}}

Geef ALLEEN valide JSON, geen markdown of uitleg eromheen."""

    sample = "\n".join(f"- {v}" for v in voorbeelden[:10])
    user = f"Huidige voorbeelden uit de batch:\n{sample}\n\nFeedback van gebruiker:\n{input_text}"

    try:
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=800,
            system=system,
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


def _apply_rule_locally(stap: int, rule_type: str, action: dict, data: list[dict]) -> int:
    """Pas regel direct toe op de in-view data. Return: aantal records geraakt."""
    raakt = 0
    act = action or {}

    if stap == 1 and rule_type == "title_strip":
        woorden = [w for w in (act.get("strip") or []) if w]
        for r in data:
            oud = r.get("product_title_nl", "") or ""
            nieuw = oud
            for w in woorden:
                nieuw = re.sub(rf"\s*[-–—]?\s*{re.escape(w)}\s*[-–—]?\s*", " ", nieuw, flags=re.IGNORECASE)
            nieuw = re.sub(r"\s{2,}", " ", nieuw).strip(" -–—")
            if nieuw != oud:
                r["product_title_nl"] = nieuw
                raakt += 1

    elif stap == 1 and rule_type == "title_replace":
        paren = [(p.get("from", ""), p.get("to", "")) for p in (act.get("replace") or [])]
        for r in data:
            oud = r.get("product_title_nl", "") or ""
            nieuw = oud
            for fr, to in paren:
                if fr:
                    nieuw = re.sub(re.escape(fr), to, nieuw, flags=re.IGNORECASE)
            if nieuw != oud:
                r["product_title_nl"] = nieuw
                raakt += 1

    elif stap == 1 and rule_type == "title_instruction":
        # alleen opslaan voor toekomstige Haiku-runs, geen lokale wijziging
        pass

    elif stap == 2 and rule_type == "translation":
        veld = (act.get("veld") or "").lower()
        en = (act.get("en") or "").strip().lower()
        nl = (act.get("nl") or "").strip()
        if veld == "materiaal":
            for r in data:
                if (r.get("materiaal_nl", "") or "").strip().lower() == en:
                    r["materiaal_nl"] = nl
                    raakt += 1
        elif veld == "kleur":
            for r in data:
                if (r.get("kleur_nl", "") or "").strip().lower() == en:
                    r["kleur_nl"] = nl
                    raakt += 1

    elif stap == 2 and rule_type in ("name_rule", "name_rule_bulk"):
        regels = [act] if rule_type == "name_rule" else (act.get("regels") or [])
        for rl in regels:
            zoek = (rl.get("zoekwoord") or "").strip().lower()
            sub = rl.get("sub_subcategorie") or ""
            is_extra = bool(rl.get("is_extra"))
            if not zoek or not sub:
                continue
            for r in data:
                naam = (r.get("product_title_nl", "") or r.get("product_title", "") or "").lower()
                if zoek in naam:
                    if is_extra:
                        if r.get("sub_subcategorie") and r.get("sub_subcategorie") != sub:
                            r["sub_subcategorie_2"] = sub
                        else:
                            r["sub_subcategorie"] = sub
                    else:
                        r["sub_subcategorie"] = sub
                    raakt += 1

    elif stap == 2 and rule_type == "category_override":
        skus = set((act.get("skus") or []))
        for r in data:
            if r.get("sku") in skus:
                for k in ("hoofdcategorie", "subcategorie", "sub_subcategorie", "sub_subcategorie_2"):
                    if act.get(k):
                        r[k] = act[k]
                r["collectie"] = act.get("subcategorie", r.get("collectie", ""))
                r["_cat_gemapt"] = True
                raakt += 1

    elif stap == 3 and rule_type == "meta_replace":
        paren = [(p.get("from", ""), p.get("to", "")) for p in (act.get("replace") or [])]
        for r in data:
            oud = r.get("meta_description", "") or ""
            nieuw = oud
            for fr, to in paren:
                if fr:
                    nieuw = re.sub(re.escape(fr), to, nieuw, flags=re.IGNORECASE)
            if nieuw != oud:
                r["meta_description"] = nieuw[:160]
                raakt += 1

    elif stap == 3 and rule_type == "meta_instruction":
        pass  # alleen opslaan

    return raakt


def _save_rule(stap: int, rule_type: str, action: dict, scope: str, input_text: str,
               explanation: str, raakt: int) -> bool:
    """Sla regel direct op als 'applied' in seo_learnings."""
    try:
        _get_sb().table("seo_learnings").insert({
            "stap": _CHAT_STAP_NAAM.get(stap, str(stap)),
            "rule_type": rule_type,
            "scope": scope or "alle",
            "input_text": input_text,
            "action": action or {},
            "raw_response": json.dumps({"rule_type": rule_type, "action": action,
                                        "explanation": explanation}),
            "status": "applied",
            "applied_at": datetime.utcnow().isoformat(),
            "applied_by": "chef@seokitchen.nl",
            "applied_rows": raakt,
            "example_before": None,
            "example_after": None,
        }).execute()
        return True
    except Exception as e:
        st.warning(f"Regel toegepast maar niet opgeslagen: {e}")
        return False


def _chat_box(stap: int, kolom_voorbeeld: str) -> None:
    """Render chat-input onderaan een stap.

    kolom_voorbeeld: welke key uit hvp_data tonen als sample (bv. 'product_title_nl').
    """
    data: list[dict] = st.session_state["hvp_data"]
    voorbeelden = [r.get(kolom_voorbeeld, "") for r in data if r.get(kolom_voorbeeld)][:10]

    with st.expander("💬 Correctie voor deze stap (wordt onthouden)", expanded=False):
        st.caption(
            "Typ in normaal Nederlands wat er mis gaat. De fix wordt nu toegepast én "
            "opgeslagen zodat het in toekomstige runs ook automatisch gebeurt."
        )
        key_in = f"hvp_chat_in_{stap}"
        key_btn = f"hvp_chat_btn_{stap}"
        key_clr = f"hvp_chat_clr_{stap}"

        txt = st.text_area(
            "Feedback",
            height=80,
            key=key_in,
            placeholder={
                1: "bv. 'Verwijder Ferd Ridge en Horace Ridge uit alle titels — dat zijn collectie-namen'",
                2: "bv. 'Producten met storage_pot in de naam zijn altijd Voorraadpotten'",
                3: "bv. 'Begin nooit met Ontdek, gebruik liever de productnaam'",
            }.get(stap, ""),
        )
        c1, c2 = st.columns([1, 5])
        with c1:
            doe = st.button("Pas toe + onthou", type="primary",
                            disabled=not txt.strip(), key=key_btn)
        with c2:
            if st.button("Wis", key=key_clr):
                st.session_state[key_in] = ""
                st.rerun()

        if doe and txt.strip():
            with st.spinner("Sonnet parset feedback..."):
                parsed = _interpret_chat(stap, txt.strip(), voorbeelden)
            if not parsed:
                return
            rt = parsed.get("rule_type")
            act = parsed.get("action") or {}
            expl = parsed.get("explanation", "")
            scope = parsed.get("scope", "alle")

            if rt == "unclear" or not rt:
                st.warning(f"Onduidelijk: {expl or 'parse-fout'}")
                return

            raakt = _apply_rule_locally(stap, rt, act, data)
            st.session_state["hvp_data"] = data
            opgeslagen = _save_rule(stap, rt, act, scope, txt.strip(), expl, raakt)

            msg = f"✅ {rt} — {raakt} records aangepast"
            if rt in ("title_instruction", "meta_instruction"):
                msg = f"✅ Regel onthouden voor toekomstige runs ({rt})"
            if opgeslagen:
                msg += " · opgeslagen in geheugen"
            st.success(msg)
            if expl:
                st.caption(expl)
            st.session_state[key_in] = ""
            st.rerun()


# ── Stap 1: Namen ─────────────────────────────────────────────────────────────

def _stap_namen() -> None:
    data: list[dict] = st.session_state["hvp_data"]
    n = len(data)

    st.markdown(f"### Namen vertalen ({n} producten)")
    st.caption("Haiku vertaalt alle namen in één batch. Pas aan waar nodig, dan goedkeuren.")

    if not st.session_state.get("hvp_s1_gerund"):
        kosten = max(n * 0.0001, 0.01)
        st.caption(f"Geschatte kosten: ~€{kosten:.2f} (Haiku batch)")
        if st.button("Vertaal namen via Haiku", type="primary", key="hvp_s1_run"):
            try:
                from execution.transform_v2 import (
                    vertaal_productnamen_batch, get_claude, load_active_learnings,
                )
                sb = _get_sb()
                claude = get_claude()
                title_lr = [L for L in load_active_learnings(sb) if L.get("stap") == "titel"]
                namen = [r.get("product_title", "") or r.get("product_name_raw", "") for r in data]
                with st.spinner(f"Haiku vertaalt ({len(title_lr)} actieve titel-regels)..."):
                    vertaling = vertaal_productnamen_batch(namen, claude, title_learnings=title_lr)
                for r in data:
                    raw = r.get("product_title", "") or r.get("product_name_raw", "") or ""
                    r["product_title_nl"] = vertaling.get(raw.strip(), raw)
                st.session_state["hvp_data"] = data
                st.session_state["hvp_s1_gerund"] = True
                st.rerun()
            except Exception as e:
                st.error(f"Fout: {e}")
        return

    # Tabel om namen te bewerken
    df = pd.DataFrame([{
        "sku":           r.get("sku", ""),
        "vendor":        r.get("vendor", "") or r.get("supplier", ""),
        "naam_origineel": r.get("product_title", "") or r.get("product_name_raw", ""),
        "naam_nl":       r.get("product_title_nl", ""),
    } for r in data])

    edited = st.data_editor(
        df,
        column_config={
            "sku":            st.column_config.TextColumn("SKU",        disabled=True, width="small"),
            "vendor":         st.column_config.TextColumn("Merk",       disabled=True, width="small"),
            "naam_origineel": st.column_config.TextColumn("Origineel",  disabled=True, width="large"),
            "naam_nl":        st.column_config.TextColumn("Naam NL ✏️", disabled=False, width="large"),
        },
        hide_index=True,
        disabled=["sku", "vendor", "naam_origineel"],
        width="stretch",
        key="hvp_edit_s1",
    )

    c1, c2, c3 = st.columns([2, 2, 2])
    with c1:
        if st.button("↺ Opnieuw vertalen", key="hvp_s1_reset"):
            st.session_state.pop("hvp_s1_gerund", None)
            st.rerun()
    with c3:
        if st.button("Goedkeuren → Stap 2", type="primary", key="hvp_s1_ok"):
            # Sla bewerkte namen op
            for _, row in edited.iterrows():
                for r in st.session_state["hvp_data"]:
                    if r.get("sku") == row["sku"]:
                        r["product_title_nl"] = row["naam_nl"]
                        break
            st.session_state["hvp_stap"] = 2
            st.rerun()

    _chat_box(stap=1, kolom_voorbeeld="product_title_nl")


# ── Stap 2: Categorie, materiaal, kleur ──────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def _laad_cats() -> list[dict]:
    try:
        rows = _get_sb().table("seo_category_mapping").select(
            "hoofdcategorie,subcategorie,sub_subcategorie"
        ).execute().data or []
        seen, result = set(), []
        for r in rows:
            k = (r.get("hoofdcategorie",""), r.get("subcategorie",""), r.get("sub_subcategorie",""))
            if k not in seen and k[0]:
                seen.add(k)
                result.append({"hoofdcategorie": k[0], "subcategorie": k[1], "sub_subcategorie": k[2]})
        return sorted(result, key=lambda x: (x["hoofdcategorie"], x["subcategorie"], x["sub_subcategorie"]))
    except Exception:
        return []


def _stap_categorie_kleur() -> None:
    data: list[dict] = st.session_state["hvp_data"]
    n = len(data)

    st.markdown(f"### Categorie, materiaal & kleur ({n} producten)")
    st.caption(
        "Categorisatie via mapping-tabel. Materiaal en kleur via vertaaltabellen "
        "(Sonnet alleen als fallback). Producten zonder mapping krijgen een categorie-kiezer."
    )

    if not st.session_state.get("hvp_s2_gerund"):
        kosten = n * 0.0005
        st.caption(f"Geschatte kosten: ~€{kosten:.2f} (alleen Sonnet voor onbekende materialen/kleuren)")
        if st.button("Run categorisatie + vertaling", type="primary", key="hvp_s2_run"):
            try:
                from execution.transform_v2 import (
                    lookup_category, translate_material, translate_color,
                    build_tags, apply_name_rules, apply_translation_learnings,
                    load_active_learnings, get_claude, get_supabase
                )
                sb = _get_sb()
                claude = get_claude()

                all_learnings = load_active_learnings(sb)
                cat_learnings = [L for L in all_learnings if L.get("stap") == "categorie"]
                extra_mat, extra_kl = apply_translation_learnings(
                    [L for L in all_learnings if L.get("stap") == "vertaling"]
                )

                bar = st.progress(0.0, text="Bezig...")
                for idx, r in enumerate(data):
                    bar.progress((idx + 1) / n, text=f"{idx+1}/{n}: {r.get('sku','')}")
                    sku = r.get("sku", "")

                    # Haal raw data op
                    raw_data = {}
                    if sku:
                        try:
                            res = sb.table("products_raw").select(
                                "sku,leverancier_category,leverancier_item_cat,"
                                "materiaal_raw,kleur_en,designer,giftbox,giftbox_qty,fase"
                            ).eq("sku", sku).execute().data or []
                            if res:
                                raw_data = res[0]
                        except Exception:
                            pass

                    # Categorie lookup
                    cat_row = lookup_category(
                        sb,
                        raw_data.get("leverancier_category", ""),
                        raw_data.get("leverancier_item_cat", ""),
                    )
                    if cat_row:
                        r["hoofdcategorie"] = cat_row["hoofdcategorie"]
                        r["subcategorie"] = cat_row["subcategorie"]
                        r["sub_subcategorie"] = cat_row["sub_subcategorie"]
                        r["collectie"] = cat_row["subcategorie"]
                        r["_cat_gemapt"] = True
                    else:
                        r["_cat_gemapt"] = False
                        r["_leverancier_category"] = raw_data.get("leverancier_category", "")
                        r["_leverancier_item_cat"] = raw_data.get("leverancier_item_cat", "")

                    # Name-rule learnings
                    updates = {"sub_subcategorie": r.get("sub_subcategorie", "")}
                    apply_name_rules(raw_data, updates, cat_learnings)
                    if updates.get("sub_subcategorie"):
                        r["sub_subcategorie"] = updates["sub_subcategorie"]

                    # Tags
                    r["tags"] = build_tags(
                        r.get("hoofdcategorie", ""),
                        r.get("subcategorie", ""),
                        r.get("sub_subcategorie", ""),
                        raw_data.get("fase", ""),
                        extra_tags=updates.get("_extra_tags"),
                    )

                    # Materiaal + kleur
                    mat_raw = raw_data.get("materiaal_raw", "") or ""
                    r["materiaal_nl"] = translate_material(mat_raw, claude, extra_mat) if mat_raw else ""
                    kl_en = raw_data.get("kleur_en", "") or ""
                    naam_raw = raw_data.get("product_name_raw", "") or r.get("product_title", "")
                    kl_filter, _ = translate_color(kl_en, naam_raw, claude, extra_kl) if kl_en else ("", "")
                    r["kleur_nl"] = kl_filter

                    # Designer, fase bewaren
                    if raw_data.get("designer"):
                        r["designer"] = raw_data["designer"]
                    if raw_data.get("fase"):
                        r["fase"] = raw_data["fase"]
                    if raw_data.get("giftbox"):
                        r["giftbox"] = raw_data["giftbox"]
                    if raw_data.get("giftbox_qty"):
                        r["giftbox_qty"] = raw_data["giftbox_qty"]

                bar.progress(1.0, text="Klaar.")
                st.session_state["hvp_data"] = data
                st.session_state["hvp_s2_gerund"] = True
                st.rerun()
            except Exception as e:
                st.error(f"Fout: {e}")
                import traceback
                with st.expander("Traceback"):
                    st.code(traceback.format_exc())
        return

    # ── Categorie-koppeling voor producten zonder mapping ──
    ongemapt = [r for r in data if not r.get("_cat_gemapt")]
    if ongemapt:
        cats = _laad_cats()
        hoofdcats = sorted(set(c["hoofdcategorie"] for c in cats))
        combo_to_rows: dict[tuple, list] = defaultdict(list)
        for r in ongemapt:
            k = (r.get("_leverancier_category",""), r.get("_leverancier_item_cat",""))
            combo_to_rows[k].append(r)

        with st.expander(
            f"⚠️ {len(ongemapt)} producten zonder categorie-mapping — koppel hier",
            expanded=True
        ):
            st.caption("Koppel de juiste categorie. Die wordt ook opgeslagen in de mapping-tabel voor toekomstige batches.")
            for idx, ((lc, lic), combo_rows) in enumerate(combo_to_rows.items()):
                st.markdown(f"**{lc}** / {lic or '(leeg)'}  —  {len(combo_rows)} producten")
                # Toon de daadwerkelijke productnamen zodat je ziet waar het over gaat
                namen_lijst = [
                    f"{r.get('sku','')} — {r.get('product_title_nl','') or r.get('product_title','')}"
                    for r in combo_rows
                ]
                with st.container(border=True):
                    for n in namen_lijst[:8]:
                        st.text(n)
                    if len(namen_lijst) > 8:
                        st.caption(f"+ {len(namen_lijst) - 8} meer…")
                c1, c2, c3 = st.columns(3)
                with c1:
                    hc = st.selectbox("Hoofdcategorie", hoofdcats, key=f"hck2_{idx}")
                with c2:
                    subcats = sorted(set(c["subcategorie"] for c in cats if c["hoofdcategorie"] == hc and c["subcategorie"]))
                    sc = st.selectbox("Subcategorie", subcats or ["—"], key=f"sck2_{idx}")
                with c3:
                    subsubs = sorted(set(c["sub_subcategorie"] for c in cats if c["hoofdcategorie"] == hc and c["subcategorie"] == sc and c["sub_subcategorie"]))
                    ssc = st.selectbox("Sub-subcategorie", subsubs or ["—"], key=f"ssck2_{idx}")

                # Optionele tweede sub-subcategorie (bv. Bloempotten binnen + buiten)
                alle_subsubs = sorted(set(c["sub_subcategorie"] for c in cats if c["sub_subcategorie"] and c["sub_subcategorie"] != ssc))
                ssc2 = st.selectbox(
                    "+ Tweede sub-subcategorie (optioneel)",
                    ["—"] + alle_subsubs,
                    key=f"ssck2b_{idx}",
                    help="Bv. een product hoort bij zowel Bloempotten binnen als buiten",
                )

                if st.button(f"Koppel ({len(combo_rows)} producten)", key=f"koppel2_{idx}"):
                    for r in combo_rows:
                        r["hoofdcategorie"] = hc
                        r["subcategorie"] = sc
                        r["sub_subcategorie"] = ssc
                        if ssc2 and ssc2 != "—":
                            r["sub_subcategorie_2"] = ssc2
                        r["collectie"] = sc
                        r["_cat_gemapt"] = True
                    try:
                        sb = _get_sb()
                        sb.table("seo_category_mapping").insert({
                            "id": str(uuid.uuid4()),
                            "leverancier_category": lc,
                            "leverancier_item_cat": lic or "?",
                            "hoofdcategorie": hc,
                            "subcategorie": sc,
                            "sub_subcategorie": ssc,
                        }).execute()
                        _laad_cats.clear()
                    except Exception:
                        pass
                    st.session_state["hvp_data"] = data
                    st.success(f"✅ Gekoppeld + opgeslagen in mapping-tabel.")
                    st.rerun()
                st.markdown("---")

    # ── Bewerkbare tabel ──
    st.markdown("**Controleer en pas aan:**")
    df = pd.DataFrame([{
        "sku":             r.get("sku", ""),
        "naam_nl":         r.get("product_title_nl", ""),
        "hoofdcategorie":  r.get("hoofdcategorie", ""),
        "subcategorie":    r.get("subcategorie", ""),
        "sub_subcategorie": r.get("sub_subcategorie", ""),
        "sub_subcategorie_2": r.get("sub_subcategorie_2", ""),
        "materiaal_nl":    r.get("materiaal_nl", ""),
        "kleur_nl":        r.get("kleur_nl", ""),
    } for r in data])

    edited = st.data_editor(
        df,
        column_config={
            "sku":              st.column_config.TextColumn("SKU",          disabled=True,  width="small"),
            "naam_nl":          st.column_config.TextColumn("Naam NL",      disabled=True,  width="medium"),
            "hoofdcategorie":   st.column_config.TextColumn("Hoofdcat ✏️",  disabled=False, width="medium"),
            "subcategorie":     st.column_config.TextColumn("Subcat ✏️",    disabled=False, width="medium"),
            "sub_subcategorie": st.column_config.TextColumn("Sub-subcat ✏️", disabled=False, width="medium"),
            "sub_subcategorie_2": st.column_config.TextColumn("+ Tweede sub-subcat ✏️", disabled=False, width="medium",
                                   help="Optioneel: tweede categorie (bv. Bloempotten binnen + buiten)"),
            "materiaal_nl":     st.column_config.TextColumn("Materiaal ✏️", disabled=False, width="small"),
            "kleur_nl":         st.column_config.TextColumn("Kleur ✏️",     disabled=False, width="small"),
        },
        hide_index=True,
        disabled=["sku", "naam_nl"],
        width="stretch",
        key="hvp_edit_s2",
    )

    c1, c2, c3 = st.columns([2, 2, 2])
    with c1:
        if st.button("← Terug naar namen", key="hvp_s2_back"):
            st.session_state["hvp_stap"] = 1
            st.session_state.pop("hvp_s2_gerund", None)
            st.rerun()
    with c2:
        if st.button("↺ Opnieuw runnen", key="hvp_s2_reset"):
            st.session_state.pop("hvp_s2_gerund", None)
            st.rerun()
    with c3:
        if st.button("Goedkeuren → Stap 3", type="primary", key="hvp_s2_ok"):
            from execution.transform_v2 import build_tags
            for _, row in edited.iterrows():
                for r in st.session_state["hvp_data"]:
                    if r.get("sku") == row["sku"]:
                        r["hoofdcategorie"] = row["hoofdcategorie"]
                        r["subcategorie"] = row["subcategorie"]
                        r["sub_subcategorie"] = row["sub_subcategorie"]
                        r["sub_subcategorie_2"] = (row.get("sub_subcategorie_2") or "").strip()
                        r["materiaal_nl"] = row["materiaal_nl"]
                        r["kleur_nl"] = row["kleur_nl"]
                        # Herbouw tags zodat tweede subcat als tag wordt meegenomen
                        extra = [r["sub_subcategorie_2"]] if r["sub_subcategorie_2"] else None
                        r["tags"] = build_tags(
                            r.get("hoofdcategorie", ""),
                            r.get("subcategorie", ""),
                            r.get("sub_subcategorie", ""),
                            r.get("fase", ""),
                            extra_tags=extra,
                        )
                        break
            st.session_state["hvp_stap"] = 3
            st.rerun()

    _chat_box(stap=2, kolom_voorbeeld="product_title_nl")


# ── Stap 3: Meta descriptions ─────────────────────────────────────────────────

def _stap_meta() -> None:
    data: list[dict] = st.session_state["hvp_data"]
    n = len(data)

    st.markdown(f"### Meta descriptions schrijven ({n} producten)")
    st.caption(
        "Sonnet schrijft een meta description per product op basis van naam, categorie, "
        "materiaal en kleur uit de vorige stappen. 120-155 tekens, je-vorm, eindigt met CTA."
    )

    if not st.session_state.get("hvp_s3_gerund"):
        kosten = n * 0.002
        st.caption(f"Geschatte kosten: ~€{kosten:.2f} (Sonnet, {n} calls)")
        if st.button(f"Schrijf {n} meta descriptions", type="primary", key="hvp_s3_run"):
            try:
                import anthropic
                client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY",""))
                bar = st.progress(0.0, text="Bezig...")

                for idx, r in enumerate(data):
                    bar.progress((idx + 1) / n, text=f"{idx+1}/{n}: {r.get('sku','')}")

                    title  = r.get("product_title_nl", "") or r.get("product_title", "")
                    vendor = r.get("vendor", "") or r.get("supplier", "")
                    subcat = r.get("subcategorie", "")
                    mat    = r.get("materiaal_nl", "")
                    kleur  = r.get("kleur_nl", "")
                    h = r.get("hoogte_cm","") or ""
                    l = r.get("lengte_cm","") or ""
                    b = r.get("breedte_cm","") or ""
                    afm = f"H {h} x L {l} x B {b} cm" if all([h, l, b]) else ""

                    extra = "\n".join(filter(None, [
                        f"Materiaal: {mat}" if mat else "",
                        f"Kleur: {kleur}" if kleur else "",
                        f"Subcategorie: {subcat}" if subcat else "",
                        f"Afmetingen: {afm}" if afm else "",
                    ]))

                    try:
                        resp = client.messages.create(
                            model="claude-sonnet-4-6",
                            max_tokens=200,
                            messages=[{"role": "user", "content":
                                f"Schrijf een Nederlandse SEO meta description (120–155 tekens).\n"
                                f"Product: {title}\nMerk: {vendor}\n{extra}\n\n"
                                "Regels: 'je'-vorm, eindig met CTA, vermeld gratis verzending €75 als dat past.\n"
                                "Geef alleen de meta description terug."}],
                        )
                        r["meta_description"] = resp.content[0].text.strip()[:155]
                    except Exception as e:
                        r["meta_description"] = ""

                bar.progress(1.0, text="Klaar.")
                st.session_state["hvp_data"] = data
                st.session_state["hvp_s3_gerund"] = True
                st.rerun()
            except Exception as e:
                st.error(f"Fout: {e}")
        return

    # Bewerkbare tabel met tekenteller
    df = pd.DataFrame([{
        "sku":              r.get("sku", ""),
        "naam_nl":          r.get("product_title_nl", ""),
        "meta_description": r.get("meta_description", ""),
        "tekens":           len(r.get("meta_description", "") or ""),
    } for r in data])

    edited = st.data_editor(
        df,
        column_config={
            "sku":              st.column_config.TextColumn("SKU",              disabled=True,  width="small"),
            "naam_nl":          st.column_config.TextColumn("Naam NL",          disabled=True,  width="medium"),
            "meta_description": st.column_config.TextColumn("Meta description ✏️", disabled=False, width="large"),
            "tekens":           st.column_config.NumberColumn("Tekens",          disabled=True,  width="small"),
        },
        hide_index=True,
        disabled=["sku", "naam_nl", "tekens"],
        width="stretch",
        key="hvp_edit_s3",
    )

    # Live tekentellers
    ok = int(((df["tekens"] >= 120) & (df["tekens"] <= 155)).sum()) if len(df) else 0
    m1, m2, m3 = st.columns(3)
    m1.metric("120-155 tekens ✅", ok)
    m2.metric("Te kort (<120)", int(((df["tekens"] < 120) & (df["tekens"] > 0)).sum()) if len(df) else 0)
    m3.metric("Leeg", int((df["tekens"] == 0).sum()) if len(df) else 0)

    c1, c2, c3 = st.columns([2, 2, 2])
    with c1:
        if st.button("← Terug naar categorie", key="hvp_s3_back"):
            st.session_state["hvp_stap"] = 2
            st.session_state.pop("hvp_s3_gerund", None)
            st.rerun()
    with c2:
        if st.button("↺ Opnieuw genereren", key="hvp_s3_reset"):
            st.session_state.pop("hvp_s3_gerund", None)
            st.rerun()
    with c3:
        if st.button("Goedkeuren → Opslaan", type="primary", key="hvp_s3_ok"):
            for _, row in edited.iterrows():
                for r in st.session_state["hvp_data"]:
                    if r.get("sku") == row["sku"]:
                        r["meta_description"] = row["meta_description"]
                        break
            st.session_state["hvp_stap"] = 4
            st.rerun()

    _chat_box(stap=3, kolom_voorbeeld="meta_description")


# ── Stap 4: Opslaan + Export ──────────────────────────────────────────────────

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
TEXT_COLS = {"Variant Barcode", "Product Metafield custom.ean"}


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
            "Variant SKU":    p.get("sku",""),
            "Product Handle": p.get("handle","") or p.get("sku",""),
            "Product Title":  p.get("product_title_nl","") or p.get("product_title",""),
            "Product Vendor": p.get("vendor","") or p.get("supplier",""),
            "Product Type":   p.get("hoofdcategorie","") or p.get("product_type",""),
            "Variant Barcode": str(p.get("ean_shopify","") or ""),
            "Variant Price":  _clean(p.get("verkoopprijs") or p.get("price")),
            "Variant Cost":   _clean(p.get("inkoopprijs")),
            "Product Description": p.get("meta_description","") or "",
            "Product Tags":   p.get("tags","") or "",
            "Variant Metafield custom.collectie": p.get("collectie","") or "",
            "Product Metafield custom.designer":  p.get("designer","") or "",
            "Product Metafield custom.materiaal": p.get("materiaal_nl","") or "",
            "Product Metafield custom.kleur":     p.get("kleur_nl","") or "",
            "Product Metafield custom.hoogte_filter": _clean(p.get("hoogte_cm")),
            "Product Metafield custom.lengte_filter": _clean(p.get("lengte_cm")),
            "Product Metafield custom.breedte_filter": _clean(p.get("breedte_cm")),
            "photo_packshot_1": p.get("photo_packshot_1","") or "",
            "photo_packshot_2": p.get("photo_packshot_2","") or "",
            "photo_packshot_3": p.get("photo_packshot_3","") or "",
            "photo_packshot_4": p.get("photo_packshot_4","") or "",
            "photo_packshot_5": p.get("photo_packshot_5","") or "",
            "photo_lifestyle_1": p.get("photo_lifestyle_1","") or "",
            "photo_lifestyle_2": p.get("photo_lifestyle_2","") or "",
            "photo_lifestyle_3": p.get("photo_lifestyle_3","") or "",
            "photo_lifestyle_4": p.get("photo_lifestyle_4","") or "",
            "photo_lifestyle_5": p.get("photo_lifestyle_5","") or "",
            "Product Metafield custom.ean": str(p.get("ean_piece","") or ""),
            "Product Metafield custom.artikelnummer": p.get("sku","") or "",
            "Product Metafield custom.meta_description": p.get("meta_description","") or "",
        }
        for ci, col in enumerate(HEXTOM_COLUMNS, 1):
            val = row_data.get(col,"") if col else ""
            cell = ws.cell(row=ri, column=ci, value=val)
            if col in TEXT_COLS and val:
                cell.value = str(val); cell.number_format = "@"
    for ci in range(1, len(HEXTOM_COLUMNS)+1):
        ws.column_dimensions[get_column_letter(ci)].width = {1:18,4:40,5:50,8:16,11:60,15:50}.get(ci,20)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def _stap_export() -> None:
    data: list[dict] = st.session_state["hvp_data"]
    n = len(data)

    st.markdown(f"### Opslaan & exporteren ({n} producten)")

    # Samenvatting
    df_sum = pd.DataFrame([{
        "SKU": r.get("sku",""),
        "Naam NL": r.get("product_title_nl",""),
        "Categorie": f"{r.get('hoofdcategorie','')} / {r.get('sub_subcategorie','')}",
        "Materiaal": r.get("materiaal_nl",""),
        "Kleur": r.get("kleur_nl",""),
        "Meta (tekens)": len(r.get("meta_description","") or ""),
    } for r in data])
    st.dataframe(df_sum, hide_index=True, use_container_width=True)

    c1, c2, c3 = st.columns([2, 2, 2])

    with c1:
        if st.button("← Terug naar meta", key="hvp_s4_back"):
            st.session_state["hvp_stap"] = 3
            st.rerun()

    with c2:
        if st.button(f"Opslaan in database ({n})", type="primary", key="hvp_s4_save"):
            sb = _get_sb()
            saved = errors = 0
            prog = st.progress(0.0)
            for idx, r in enumerate(data):
                prog.progress((idx+1)/n)
                sku = r.get("sku","")
                if not sku:
                    continue
                try:
                    from execution.transform_v2 import generate_handle, build_title
                    titel = r.get("product_title_nl","")
                    handle = r.get("handle","") or generate_handle(titel) if titel else ""
                    upd = {
                        "sku": sku,
                        "supplier": r.get("vendor","") or r.get("supplier",""),
                        "product_title_nl": titel,
                        "handle": handle,
                        "hoofdcategorie": r.get("hoofdcategorie",""),
                        "subcategorie": r.get("subcategorie",""),
                        "sub_subcategorie": r.get("sub_subcategorie",""),
                        "collectie": r.get("collectie","") or r.get("subcategorie",""),
                        "tags": r.get("tags",""),
                        "materiaal_nl": r.get("materiaal_nl",""),
                        "kleur_nl": r.get("kleur_nl",""),
                        "meta_description": r.get("meta_description",""),
                        "pipeline_status": "ready",
                        "review_reden": None,
                    }
                    for veld in ("verkoopprijs","inkoopprijs"):
                        val = r.get(veld)
                        if val is not None:
                            try: upd[veld] = float(val)
                            except (ValueError, TypeError): pass
                    existing = sb.table("products_curated").select("id").eq("sku",sku).execute().data
                    if existing:
                        sb.table("products_curated").update(upd).eq("sku",sku).execute()
                    else:
                        sb.table("products_curated").insert(upd).execute()
                    saved += 1
                except Exception as e:
                    errors += 1
            if errors:
                st.warning(f"✅ {saved} opgeslagen · ⚠️ {errors} fouten")
            else:
                st.success(f"✅ {saved} producten opgeslagen in products_curated.")

    with c3:
        if st.button("Download Hextom Excel", key="hvp_s4_excel"):
            with st.spinner("Excel bouwen..."):
                xlsx = _build_excel(data)
            vendor = (data[0].get("vendor","") or data[0].get("supplier","")).replace(" ","_") if data else "export"
            st.download_button(
                f"💾 Download ({n} producten)",
                data=xlsx,
                file_name=f"hextom_{vendor}_herverwerkt_{n}st.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="hvp_s4_dl",
            )

    st.divider()
    if st.button("Opnieuw beginnen (nieuwe selectie)", key="hvp_s4_nieuw"):
        for k in ["hvp_data","hvp_stap","hvp_s1_gerund","hvp_s2_gerund","hvp_s3_gerund","hvp_ai_klaar"]:
            st.session_state.pop(k, None)
        st.switch_page("pages/08_Herverwerk.py")


# ── Render ────────────────────────────────────────────────────────────────────

def render() -> None:
    st.subheader("Herverwerk — stap-voor-stap pipeline")

    rows: list[dict] = st.session_state.get("hv_pipeline_rows", [])
    if not rows:
        st.warning("Geen producten geladen. Ga terug naar **Archief herverwerken**.")
        if st.button("Terug"):
            st.switch_page("pages/08_Herverwerk.py")
        return

    # Initialiseer data + stap
    if "hvp_data" not in st.session_state:
        st.session_state["hvp_data"] = list(rows)
    if "hvp_stap" not in st.session_state:
        st.session_state["hvp_stap"] = 1

    n = len(st.session_state["hvp_data"])
    st.caption(f"{n} producten geladen — foto's, EAN en barcodes worden niet aangeraakt.")

    _voortgang(st.session_state["hvp_stap"])

    stap = st.session_state["hvp_stap"]
    if stap == 1:
        _stap_namen()
    elif stap == 2:
        _stap_categorie_kleur()
    elif stap == 3:
        _stap_meta()
    elif stap == 4:
        _stap_export()
