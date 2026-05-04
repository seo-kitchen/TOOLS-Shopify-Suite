"""Supabase-backed learning system.

Replaces the file-based ``config/learnings.json`` + ``_add_translation_to_transform``
flow. Chef corrections now land in the ``seo_learnings`` table as ``pending`` rows
and move to ``applied`` only after explicit approval on the Learnings page.

Public API:
  - ``save_pending(stap, rule_type, action, ...)``
  - ``list_learnings(stap=..., status=..., limit=...)``
  - ``apply_learning(row_id)`` -> commits the rule (category_mapping row,
     bulk update on seo_products, etc.)
  - ``reject_learning(row_id)``
  - ``load_active_learnings(stap)`` -> used by the transform page/script to read
     all rules with status='applied' for a given pipeline step.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

import streamlit as st

from .supabase_client import current_user_email, get_supabase


TABLE = "seo_learnings"


def save_pending(
    stap: str,
    rule_type: str,
    action: dict[str, Any],
    input_text: str | None = None,
    raw_response: str | None = None,
    scope: str = "global",
    example_before: str | None = None,
    example_after: str | None = None,
) -> dict:
    """Insert a new 'pending' learning. Returns the inserted row."""
    sb = get_supabase()
    row = {
        "stap": stap,
        "scope": scope,
        "rule_type": rule_type,
        "input_text": input_text,
        "action": action,
        "raw_response": raw_response,
        "status": "pending",
        "example_before": example_before or action.get("voorbeeld_voor"),
        "example_after": example_after or action.get("voorbeeld_na"),
    }
    res = sb.table(TABLE).insert(row).execute()
    return (res.data or [{}])[0]


def list_learnings(
    stap: str | None = None,
    status: str | None = None,
    rule_type: str | None = None,
    limit: int = 100,
) -> list[dict]:
    sb = get_supabase()
    q = sb.table(TABLE).select("*").order("created_at", desc=True).limit(limit)
    if stap:
        q = q.eq("stap", stap)
    if status:
        q = q.eq("status", status)
    if rule_type:
        q = q.eq("rule_type", rule_type)
    return (q.execute().data or [])


@st.cache_data(ttl=60, show_spinner=False)
def load_active_learnings(stap: str) -> list[dict]:
    """Used by transform page: return all applied rules for a pipeline step.

    Cached 60s so a transform run doesn't re-query on every row.
    """
    sb = get_supabase()
    res = (
        sb.table(TABLE)
        .select("id, rule_type, action, scope")
        .eq("stap", stap)
        .eq("status", "applied")
        .order("created_at", desc=False)
        .execute()
    )
    return res.data or []


def apply_learning(row_id: int) -> dict:
    """Promote a learning from 'pending'/'approved' to 'applied'.

    Side effects depend on rule_type:
      - category_mapping : upsert a row in seo_category_mapping
      - name_rule        : bulk update seo_products.sub_subcategorie where naam ILIKE
      - name_rule_bulk   : same, but multiple regels
      - title_rule       : no DB write; transform reads the rule on next run
      - translation      : no DB write; transform reads the rule on next run
      - unclear          : reject instead

    Returns the updated learning row.
    """
    sb = get_supabase()
    row = sb.table(TABLE).select("*").eq("id", row_id).single().execute().data
    if not row:
        raise ValueError(f"Learning id={row_id} not found")

    rt = row["rule_type"]
    action = row["action"] or {}
    applied_rows = 0

    if rt == "unclear":
        raise ValueError(
            "Learning is marked 'unclear' — reject or clarify, don't apply."
        )

    elif rt == "category_mapping":
        upsert = {
            "leverancier_category": action.get("leverancier_category"),
            "leverancier_item_cat": action.get("leverancier_item_cat"),
            "hoofdcategorie": action.get("hoofdcategorie"),
            "subcategorie": action.get("subcategorie"),
            "sub_subcategorie": action.get("sub_subcategorie"),
        }
        sb.table("seo_category_mapping").upsert(
            upsert,
            on_conflict="leverancier_category,leverancier_item_cat",
        ).execute()

    elif rt == "name_rule":
        zoek = (action.get("zoekwoord") or "").strip()
        sub_sub = action.get("sub_subcategorie") or ""
        is_extra = bool(action.get("is_extra"))
        if zoek and sub_sub:
            if is_extra:
                pass
            else:
                res = (
                    sb.table("seo_products")
                    .update({"sub_subcategorie": sub_sub})
                    .ilike("product_name_raw", f"%{zoek}%")
                    .execute()
                )
                applied_rows = len(res.data or [])

    elif rt == "name_rule_bulk":
        for regel in action.get("regels", []) or []:
            zoek = (regel.get("zoekwoord") or "").strip()
            sub_sub = regel.get("sub_subcategorie") or ""
            if not zoek or not sub_sub:
                continue
            if not bool(regel.get("is_extra")):
                res = (
                    sb.table("seo_products")
                    .update({"sub_subcategorie": sub_sub})
                    .ilike("product_name_raw", f"%{zoek}%")
                    .execute()
                )
                applied_rows += len(res.data or [])

    update = {
        "status": "applied",
        "applied_at": datetime.utcnow().isoformat(),
        "applied_by": current_user_email(),
        "applied_rows": applied_rows,
    }
    res = sb.table(TABLE).update(update).eq("id", row_id).execute()
    load_active_learnings.clear()
    return (res.data or [{}])[0]


def reject_learning(row_id: int, reason: str | None = None) -> dict:
    sb = get_supabase()
    update = {
        "status": "rejected",
        "notes": reason,
    }
    res = sb.table(TABLE).update(update).eq("id", row_id).execute()
    load_active_learnings.clear()
    return (res.data or [{}])[0]
