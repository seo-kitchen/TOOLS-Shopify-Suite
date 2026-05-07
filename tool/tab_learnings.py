"""Tab — Learnings systeem (feedback regels).

Drie tabs:
  - Nieuwe correctie: typ in NL, Claude parset naar rule
  - Pending: keur regels goed/af
  - Applied: actieve regels (worden gebruikt door transform_v2)

Regel-types:
  - name_rule: zoekwoord in productnaam → sub_subcategorie
  - translation: en→nl voor materiaal/kleur
  - category_mapping: leverancier → onze categorie
"""
from __future__ import annotations

import json
import os
from datetime import datetime

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
        return None
    return create_client(url, key)


SYSTEM_PROMPT = """Je bent een data-engineer die natuurlijk-taal correcties parset naar JSON-regels.

Beschikbare types:
1. name_rule — als productnaam een woord bevat, gebruik dan deze sub_subcategorie
   action: {"zoekwoord": "...", "sub_subcategorie": "...", "is_extra": false}
2. name_rule_bulk — meerdere zoekwoord-regels tegelijk
   action: {"regels": [{"zoekwoord": "...", "sub_subcategorie": "..."}, ...]}
3. translation — Engels naar Nederlands voor materiaal of kleur
   action: {"veld": "materiaal" of "kleur", "en": "...", "nl": "..."}
4. category_mapping — leverancier-categorie naar onze categorie
   action: {"leverancier_category": "...", "leverancier_item_cat": "...", "hoofdcategorie": "...", "subcategorie": "...", "sub_subcategorie": "..."}
5. unclear — als de input onduidelijk is

Output JSON met:
{
  "stap": "categorie" of "vertaling" of "titel",
  "rule_type": "name_rule" of "translation" of "category_mapping" of "name_rule_bulk" of "unclear",
  "action": {...},
  "scope": "Serax" of "Pottery Pots" of "alle",
  "example_before": "voorbeeld van wat er mis gaat",
  "example_after": "wat het zou moeten worden"
}

Geef ALLEEN valide JSON terug, geen uitleg."""


def _interpret(input_text: str) -> dict | None:
    """Stuur correctie naar Claude en parse als JSON."""
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=600,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": input_text}],
        )
        text = resp.content[0].text.strip()
        # Strip eventuele markdown-codeblokken
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text.strip())
    except Exception as e:
        st.error(f"Claude interpretatie mislukt: {e}")
        return None


def render() -> None:
    st.subheader("Learnings — feedback regels")
    st.caption(
        "Leer correcties aan het systeem die automatisch worden toegepast op de volgende "
        "Transform-run. Pending regels moeten goedgekeurd, applied regels zijn actief."
    )

    sb = _get_sb()
    if not sb:
        st.error("Supabase niet bereikbaar.")
        return

    tab1, tab2, tab3 = st.tabs(["Nieuwe correctie", "Pending", "Applied"])

    # ── Tab 1: nieuwe correctie ──
    with tab1:
        st.markdown("**Typ in normaal Nederlands wat er mis gaat:**")
        st.caption(
            "Voorbeelden:\n"
            "- _'Producten met Round in de naam moeten sub_subcategorie Bloempotten binnen krijgen'_\n"
            "- _'Vertaal ash wood altijd naar Essenhout'_\n"
            "- _'De vertaling van Pottery&UJ + flower pots is fout, moet zijn Vazen & Potten / Potten / Bloempotten binnen'_"
        )

        input_text = st.text_area("Correctie", height=100, key="lr_input",
                                   placeholder="bv. 'storage_pot in productnaam moet altijd Voorraadpotten worden'")
        col_int, col_clr = st.columns([1, 4])
        with col_int:
            interpret_btn = st.button("Interpret", type="primary",
                                       disabled=not input_text.strip(), key="lr_int")
        with col_clr:
            if st.button("Wis", key="lr_clr"):
                st.session_state["lr_input"] = ""
                st.rerun()

        if interpret_btn and input_text.strip():
            with st.spinner("Claude parset..."):
                parsed = _interpret(input_text.strip())
            if parsed:
                st.json(parsed)
                if st.button("Opslaan als pending", key="lr_save"):
                    try:
                        sb.table("seo_learnings").insert({
                            "stap": parsed.get("stap"),
                            "rule_type": parsed.get("rule_type"),
                            "scope": parsed.get("scope", "alle"),
                            "input_text": input_text.strip(),
                            "action": parsed.get("action", {}),
                            "raw_response": json.dumps(parsed),
                            "status": "pending",
                            "example_before": parsed.get("example_before"),
                            "example_after": parsed.get("example_after"),
                        }).execute()
                        st.success("✅ Opgeslagen als pending — keur goed in tab Pending.")
                    except Exception as e:
                        st.error(f"Opslaan mislukt: {e}")

    # ── Tab 2: pending ──
    with tab2:
        try:
            pending = sb.table("seo_learnings").select("*") \
                .eq("status", "pending").order("created_at", desc=True) \
                .limit(50).execute().data or []
        except Exception as e:
            st.error(f"Fout: {e}")
            pending = []

        if not pending:
            st.info("Geen pending regels.")
        else:
            st.caption(f"{len(pending)} regels wachten op goedkeuring.")
            for L in pending:
                with st.container(border=True):
                    c1, c2 = st.columns([5, 2])
                    with c1:
                        st.markdown(f"**{L.get('rule_type')}** — {L.get('stap', '?')}")
                        st.caption(f"Input: {L.get('input_text', '')[:100]}")
                        if L.get("example_before"):
                            st.caption(f"Voor: {L['example_before']}  →  Na: {L.get('example_after', '?')}")
                        with st.expander("Action JSON"):
                            st.json(L.get("action", {}))
                    with c2:
                        if st.button("✅ Goedkeuren", key=f"lr_apr_{L['id']}", type="primary"):
                            try:
                                sb.table("seo_learnings").update({
                                    "status": "applied",
                                    "applied_at": datetime.utcnow().isoformat(),
                                    "applied_by": "chef@seokitchen.nl",
                                }).eq("id", L["id"]).execute()
                                st.rerun()
                            except Exception as e:
                                st.error(f"Fout: {e}")
                        if st.button("❌ Afkeuren", key=f"lr_rej_{L['id']}"):
                            try:
                                sb.table("seo_learnings").update({"status": "rejected"}) \
                                  .eq("id", L["id"]).execute()
                                st.rerun()
                            except Exception as e:
                                st.error(f"Fout: {e}")

    # ── Tab 3: applied ──
    with tab3:
        try:
            applied = sb.table("seo_learnings").select("*") \
                .eq("status", "applied").order("applied_at", desc=True) \
                .limit(100).execute().data or []
        except Exception as e:
            st.error(f"Fout: {e}")
            applied = []

        if not applied:
            st.info("Nog geen applied regels.")
        else:
            st.caption(f"{len(applied)} actieve regels — worden toegepast tijdens Transform.")
            df = pd.DataFrame([{
                "id": L["id"],
                "stap": L.get("stap"),
                "rule_type": L.get("rule_type"),
                "input": (L.get("input_text") or "")[:60],
                "applied_at": (L.get("applied_at") or "")[:16],
                "applied_rows": L.get("applied_rows", 0),
            } for L in applied])
            st.dataframe(df, hide_index=True, use_container_width=True)

            # Wis-knop voor één regel
            ids_to_disable = st.multiselect("Selecteer ID's om te deactiveren",
                                             options=[L["id"] for L in applied],
                                             key="lr_disable")
            if ids_to_disable and st.button("Deactiveer geselecteerde",
                                              key="lr_disable_btn"):
                for lid in ids_to_disable:
                    try:
                        sb.table("seo_learnings").update({"status": "superseded"}) \
                          .eq("id", lid).execute()
                    except Exception:
                        pass
                st.success(f"{len(ids_to_disable)} regels gedeactiveerd.")
                st.rerun()
