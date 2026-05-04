"""Column allow-lists per export / update task.

Implements the hard rule that "nieuwe exports mogen gecureerde data niet
overschrijven". Every place in the dashboard that writes to ``seo_products``
declares WHICH columns it is allowed to touch for the task at hand. Any column
outside the list makes ``check_allowed`` raise.
"""
from __future__ import annotations


# Columns each task is permitted to touch on seo_products.
# Keep this list tight — narrower is safer.
ALLOWED_COLUMNS: dict[str, set[str]] = {

    "ingest_new_row": {
        "sku", "ean", "ean_shopify", "leverancier", "leverancier_category",
        "leverancier_item_cat", "product_name_raw", "product_name_en",
        "fase", "batch_tag", "merk",
        "verkoopprijs", "inkoopprijs", "rrp_gb_eur",
        "kleur_en", "materiaal_en",
        "lengte_cm", "breedte_cm", "hoogte_cm", "gewicht_kg",
        "status", "created_at", "updated_at",
    },

    "match_shopify": {
        "status_shopify", "match_methode", "match_zekerheid",
        "shopify_product_id", "shopify_variant_id",
        "review_reden", "status", "updated_at",
    },

    "transform_enrich": {
        "hoofdcategorie", "subcategorie", "sub_subcategorie",
        "product_title_nl", "handle", "tags", "meta_description",
        "materiaal_nl", "kleur_nl",
        "status", "updated_at",
    },

    "validate_autofix": {
        "verkoopprijs", "inkoopprijs", "meta_description",
        "review_reden", "status", "updated_at",
    },

    "update_prices": {
        "verkoopprijs", "inkoopprijs", "rrp_gb_eur", "updated_at",
    },

    "resize_photos": {
        "photo_packshot_1", "photo_packshot_2", "photo_packshot_3",
        "photo_packshot_4", "photo_packshot_5",
        "photo_lifestyle_1", "photo_lifestyle_2", "photo_lifestyle_3",
        "photo_lifestyle_4", "photo_lifestyle_5",
        "updated_at",
    },

    "fix_dimensions": {
        "lengte_cm", "breedte_cm", "hoogte_cm", "updated_at",
    },
}


class DisallowedColumnsError(ValueError):
    """Raised when a task tries to update a column it is not allowed to touch."""


def check_allowed(task: str, columns: set[str] | list[str]) -> None:
    """Raise if any column is outside the allow-list for ``task``."""
    if task not in ALLOWED_COLUMNS:
        raise DisallowedColumnsError(
            f"Task '{task}' has no declared column allow-list. "
            f"Add it to ui/export_schemas.py before doing writes."
        )
    cols = set(columns)
    allowed = ALLOWED_COLUMNS[task]
    offending = cols - allowed
    if offending:
        raise DisallowedColumnsError(
            f"Task '{task}' is not allowed to write to columns: "
            f"{sorted(offending)}. Allowed columns for this task: {sorted(allowed)}."
        )


def filter_to_allowed(task: str, row: dict) -> dict:
    """Return a copy of ``row`` containing only keys allowed by the task.

    Useful when you have a wider dict and want to defensively strip it down
    before an update call. Prefer ``check_allowed`` at call-sites where
    silently dropping data would be a bug.
    """
    allowed = ALLOWED_COLUMNS.get(task, set())
    return {k: v for k, v in row.items() if k in allowed}
