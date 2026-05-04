#!/usr/bin/env python3
"""
Migrate config/learnings.json into the new seo_learnings table.

One-shot. Run after seo_learnings has been created (see schema_v2_dashboard.sql).

Mapping from old JSON to new schema:
    stap "Categorie-toewijzing"  -> stap="categorie"
    stap "Producttitel & meta"    -> stap="titel"
    stap "Vertaling"               -> stap="vertaling"  (none in current file)
    actie.type                     -> rule_type
    actie (minus "type")           -> action (jsonb)
    input                          -> input_text
    raw_response                   -> raw_response
    timestamp                      -> created_at

Status logic: 'unclear' entries land as 'pending' (chef still needs to resolve).
Everything else lands as 'applied' — these are the rules that were actively
being patched into transform.py via _add_translation_to_transform.

Usage:
    python -m execution.migrate_learnings_to_supabase           # dry-run preview
    python -m execution.migrate_learnings_to_supabase --commit  # write to DB
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from supabase import Client, create_client


STAP_MAPPING = {
    "Categorie-toewijzing": "categorie",
    "Producttitel & meta": "titel",
    "Vertaling": "vertaling",
    "Meta-description": "meta",
}


def _load_learnings_json(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _build_row(entry: dict) -> dict:
    """Map one old entry -> new seo_learnings row (dict, ready to insert)."""
    actie = dict(entry.get("actie") or {})
    rule_type = actie.pop("type", "unclear") or "unclear"
    stap_raw = entry.get("stap", "")
    stap = STAP_MAPPING.get(stap_raw, stap_raw.lower() or "onbekend")

    status = "pending" if rule_type == "unclear" else "applied"

    return {
        "stap": stap,
        "scope": "global",
        "rule_type": rule_type,
        "input_text": entry.get("input") or None,
        "action": actie,
        "raw_response": entry.get("raw_response") or None,
        "status": status,
        "applied_at": entry.get("timestamp") if status == "applied" else None,
        "applied_by": os.getenv("USER_EMAIL", "chef@seokitchen.nl") if status == "applied" else None,
        "example_before": actie.get("voorbeeld_voor"),
        "example_after": actie.get("voorbeeld_na"),
        "notes": f"Migrated from config/learnings.json on 2026-04-20",
    }


def _preview(rows: list[dict]) -> None:
    from collections import Counter

    by_stap = Counter(r["stap"] for r in rows)
    by_type = Counter(r["rule_type"] for r in rows)
    by_status = Counter(r["status"] for r in rows)

    print(f"Preview: {len(rows)} rows to insert\n")
    print("Per stap:")
    for k, v in by_stap.most_common():
        print(f"  {k:15} {v:4d}")
    print("\nPer rule_type:")
    for k, v in by_type.most_common():
        print(f"  {k:20} {v:4d}")
    print("\nPer status:")
    for k, v in by_status.most_common():
        print(f"  {k:10} {v:4d}")

    print("\nFirst 3 rows (truncated):")
    for r in rows[:3]:
        preview = {
            "stap": r["stap"],
            "rule_type": r["rule_type"],
            "status": r["status"],
            "input_text": (r["input_text"] or "")[:60],
            "action_keys": list(r["action"].keys()),
        }
        print("  ", preview)


def _get_supabase() -> Client:
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_SERVICE_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL / SUPABASE_KEY missing in .env")
    return create_client(url, key)


def migrate_learnings(
    source: Path | str = "config/learnings.json",
    commit: bool = False,
) -> list[dict]:
    """Read old json, build rows, optionally insert into Supabase.

    Returns the list of row dicts that were (or would be) inserted.
    """
    rows = [_build_row(e) for e in _load_learnings_json(Path(source))]
    _preview(rows)

    if not commit:
        print("\n[dry-run] Pass --commit to actually insert into seo_learnings.")
        return rows

    sb = _get_supabase()

    existing = sb.table("seo_learnings").select("id", count="exact").execute()
    existing_count = existing.count or 0
    if existing_count > 0:
        print(
            f"\n[warn] seo_learnings already has {existing_count} row(s). "
            "Migration aborted to prevent duplicates. "
            "Delete existing rows first or migrate is already done."
        )
        sys.exit(2)

    res = sb.table("seo_learnings").insert(rows).execute()
    inserted = len(res.data or [])
    print(f"\n[ok] Inserted {inserted} row(s) into seo_learnings.")
    return rows


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--source",
        default="config/learnings.json",
        help="Path to the legacy learnings json (default: config/learnings.json)",
    )
    p.add_argument(
        "--commit",
        action="store_true",
        help="Actually insert rows. Without this flag: dry-run preview only.",
    )
    args = p.parse_args()

    migrate_learnings(source=args.source, commit=args.commit)


if __name__ == "__main__":
    main()
