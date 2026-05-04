"""Learnings — approval queue + applied + alles. Werkt op seo_learnings."""
from __future__ import annotations

import json

import streamlit as st

from ui.layout import page_header
from ui.learnings import apply_learning, list_learnings, reject_learning, save_pending
from ui.supabase_client import get_claude_client, get_supabase


page_header(
    "🧠 Learnings",
    subtitle="Chef-correcties: pending → approve → applied. Applied rules worden automatisch gebruikt bij elke transform-run.",
)


def _pretty_action(action: dict | None) -> str:
    if not action:
        return "_(geen action)_"
    t = action.get("type") or action.get("rule_type") or "?"
    if "zoekwoord" in action and "sub_subcategorie" in action:
        extra = " (extra)" if action.get("is_extra") else ""
        return f'🔧 **name_rule**: "{action["zoekwoord"]}" → **{action["sub_subcategorie"]}**{extra}'
    if "leverancier_category" in action:
        return (
            f'🗂️ **mapping**: ({action.get("leverancier_category")}, {action.get("leverancier_item_cat")}) '
            f'→ {action.get("hoofdcategorie")} > {action.get("subcategorie")} > {action.get("sub_subcategorie")}'
        )
    if "regels" in action:
        return f'📋 **bulk**: {len(action["regels"])} name-rules'
    if "regel" in action:
        return f'✏️ **title_rule**: {action["regel"]}'
    if "en" in action and "nl" in action:
        return f'🌐 **translation**: {action["en"]} → {action["nl"]}'
    return action.get("beschrijving") or json.dumps(action)[:200]


tab_new, tab_pending, tab_applied, tab_all = st.tabs(
    ["➕ Nieuwe correctie", "⏳ Pending", "✅ Applied", "📚 Alles"]
)


# ── Tab: Nieuwe correctie ────────────────────────────────────────────────────

with tab_new:
    st.markdown(
        """
        Typ hieronder een correctie in gewone taal. Claude parset 'm naar een rule
        en slaat 'm op als **pending** — je keurt 'm daarna handmatig goed
        onder de tab **Pending**.
        """
    )
    stap = st.selectbox("Welke stap?", ["categorie", "titel", "vertaling", "meta"], key="new_stap")
    voorbeelden = {
        "categorie": '- "deep plate moet bij Diepe borden, niet Dinerborden"\n- "alle Pottery pots met Round in de item cat zijn Bloempotten binnen"',
        "titel": '- "voor Pottery pots gebruik \'Pottery Pots\' als merk ipv Serax"\n- "als een woord in categorie én naam voorkomt, toon het maar 1x"',
        "vertaling": '- "ash wood = Essenhout"\n- "rust kleur = Roestbruin"',
        "meta": '- "meta description moet altijd eindigen met — Shop nu online."',
    }
    st.caption("Voorbeelden:\n" + voorbeelden[stap])

    user_input = st.text_area(
        "Jouw correctie",
        placeholder="Typ hier wat er anders moet…",
        height=100,
        key="new_input",
    )

    if st.button("🧠 Interpreteer & sla op als pending", type="primary"):
        if not user_input or not user_input.strip():
            st.warning("Typ eerst een correctie.")
            st.stop()

        try:
            claude = get_claude_client()
        except Exception as e:
            st.error(f"Geen Claude-client: {e}. Check ANTHROPIC_API_KEY in .env.")
            st.stop()

        system = _make_system_prompt(stap)
        with st.spinner("Claude interpreteert…"):
            try:
                resp = claude.messages.create(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=1000,
                    system=system,
                    messages=[{"role": "user", "content": user_input}],
                )
                raw = resp.content[0].text.strip()
            except Exception as e:
                st.error(f"Claude-fout: {e}")
                st.stop()

        # Parse JSON block
        action = _parse_json_block(raw)
        if action is None:
            st.error("Kon Claude's antwoord niet parsen.")
            with st.expander("Raw output"):
                st.code(raw)
            # Save as unclear to not lose it
            save_pending(
                stap=stap,
                rule_type="unclear",
                action={"raw": raw, "beschrijving": "parse_error"},
                input_text=user_input,
                raw_response=raw,
            )
            st.caption("Opgeslagen als 'unclear' pending — kun je later handmatig bewerken.")
            st.stop()

        rule_type = action.pop("type", "unclear") if "type" in action else action.get("rule_type", "unclear")
        row = save_pending(
            stap=stap,
            rule_type=rule_type,
            action=action,
            input_text=user_input,
            raw_response=raw,
        )
        st.success(f"✅ Opgeslagen als pending #{row.get('id')}. Ga naar tab **Pending** om te approven.")


def _make_system_prompt(stap: str) -> str:
    base = (
        "Je bent een JSON-generator voor het categorie- en titel-systeem van een Belgische "
        "interieur-webshop (Serax, Pottery Pots, S&P, Printworks). "
        "Geef ALLEEN een JSON object terug — geen uitleg. Gebruik geen markdown fences.\n\n"
    )
    if stap == "categorie":
        return base + (
            "Kies één van:\n"
            '  {"type":"category_mapping", "leverancier_category":..., "leverancier_item_cat":..., '
            '   "hoofdcategorie":..., "subcategorie":..., "sub_subcategorie":..., "beschrijving":...}\n'
            '  {"type":"name_rule", "zoekwoord":..., "sub_subcategorie":..., "is_extra":bool, "beschrijving":...}\n'
            '  {"type":"name_rule_bulk", "regels":[...], "beschrijving":...}\n'
            '  {"type":"unclear", "beschrijving":...}'
        )
    if stap == "titel":
        return base + (
            'Formaat: {"type":"title_rule", "regel":...naam..., "beschrijving":..., "voorbeeld_voor":..., "voorbeeld_na":...}\n'
            "OF {\"type\":\"unclear\", \"beschrijving\":...} als de regel niet duidelijk is."
        )
    if stap == "vertaling":
        return base + (
            'Formaat: {"type":"translation", "veld":"kleur|materiaal|overig", "en":..., "nl":..., "beschrijving":...}'
        )
    return base + '{"type":"unclear", "beschrijving":...}'


def _parse_json_block(text: str) -> dict | None:
    start = text.find("{")
    if start == -1:
        return None
    depth = 0
    for i in range(start, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[start:i + 1])
                except json.JSONDecodeError:
                    return None
    return None


# ── Tab: Pending ─────────────────────────────────────────────────────────────

with tab_pending:
    try:
        pending = list_learnings(status="pending", limit=500)
    except Exception as e:
        st.error(f"❌ Kon seo_learnings niet lezen: {e}")
        st.caption("Draai eerst `execution/schema_v2_dashboard.sql` in Supabase.")
        st.stop()

    stap_filter = st.multiselect(
        "Filter op stap",
        sorted({r["stap"] for r in pending}) or ["categorie"],
        key="p_stap_f",
    )
    rule_filter = st.multiselect(
        "Filter op rule type",
        sorted({r["rule_type"] for r in pending}) or [],
        key="p_rt_f",
    )

    visible = pending
    if stap_filter:
        visible = [r for r in visible if r["stap"] in stap_filter]
    if rule_filter:
        visible = [r for r in visible if r["rule_type"] in rule_filter]

    st.caption(f"{len(visible)} van {len(pending)} pending learnings zichtbaar")

    for row in visible:
        with st.container(border=True):
            cols = st.columns([3, 1])
            with cols[0]:
                st.markdown(
                    f"**#{row['id']} · {row['stap']} · `{row['rule_type']}`**  \n"
                    f"🗣️ _{(row.get('input_text') or '').strip() or '(geen input)'}_"
                )
                st.markdown(_pretty_action(row.get("action") or {}))
                with st.expander("Volledige action JSON"):
                    st.json(row.get("action") or {})
            with cols[1]:
                if row["rule_type"] == "unclear":
                    st.caption("🚫 kan niet applied worden — 'unclear'")
                    if st.button("❌ Reject", key=f"rej_{row['id']}", width="stretch"):
                        try:
                            reject_learning(row["id"], reason="rejected via UI")
                            st.rerun()
                        except Exception as e:
                            st.error(str(e))
                else:
                    if st.button("✅ Apply", key=f"app_{row['id']}", type="primary", width="stretch"):
                        try:
                            applied = apply_learning(row["id"])
                            st.success(
                                f"✅ Applied! {applied.get('applied_rows') or 0} rijen bijgewerkt."
                            )
                            st.rerun()
                        except Exception as e:
                            st.error(f"Fout: {e}")
                    if st.button("❌ Reject", key=f"rej_{row['id']}", width="stretch"):
                        try:
                            reject_learning(row["id"])
                            st.rerun()
                        except Exception as e:
                            st.error(str(e))


# ── Tab: Applied ─────────────────────────────────────────────────────────────

with tab_applied:
    try:
        applied = list_learnings(status="applied", limit=500)
    except Exception as e:
        st.error(str(e))
        st.stop()

    st.caption(f"{len(applied)} applied learnings — actief in transform")
    for row in applied:
        with st.container(border=True):
            st.markdown(
                f"**#{row['id']} · {row['stap']} · `{row['rule_type']}`**  "
                f"applied door _{row.get('applied_by') or '?'}_ "
                f"op `{(row.get('applied_at') or '')[:16]}` · "
                f"{row.get('applied_rows') or 0} rijen"
            )
            st.markdown(_pretty_action(row.get("action") or {}))


# ── Tab: Alles ───────────────────────────────────────────────────────────────

with tab_all:
    try:
        rows = list_learnings(limit=500)
    except Exception as e:
        st.error(str(e))
        st.stop()

    import pandas as pd
    if not rows:
        st.info("Geen learnings.")
    else:
        df = pd.DataFrame(rows)
        keep = [c for c in ["id", "created_at", "stap", "rule_type", "status",
                            "applied_rows", "applied_by", "input_text"] if c in df.columns]
        st.dataframe(df[keep], hide_index=True, width="stretch")
