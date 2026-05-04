"""
Modal app voor de Shopify content pipeline.
Elke pipeline stap is een web_endpoint zodat het dashboard hem kan aanroepen.

Deploy: python -m modal deploy execution/modal_shopify_pipeline.py
Test:   python -m modal run execution/modal_shopify_pipeline.py::status
"""

import modal
import os

app = modal.App("seokitchen-shopify-pipeline")

image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install(
        "fastapi[standard]",
        "supabase==2.15.1",
        "pandas==2.2.3",
        "openpyxl==3.1.5",
        "python-dotenv==1.1.0",
    )
)

secret = modal.Secret.from_name("seokitchen-shopify")


# ── Helpers ────────────────────────────────────────────────────────────────────

def get_supabase():
    from supabase import create_client
    url = os.environ["SUPABASE_NEW_URL"]
    key = os.environ["SUPABASE_NEW_SERVICE_KEY"]
    return create_client(url, key)


def log_to_dashboard(sb, project_id: str, step: str, status: str, summary: str):
    """Schrijf een regel naar tool_run_logs in het dashboard (optioneel)."""
    try:
        # Dashboard Supabase is een andere DB — skip als geen credentials
        dashboard_url = os.environ.get("DASHBOARD_SUPABASE_URL")
        dashboard_key = os.environ.get("DASHBOARD_SUPABASE_SERVICE_KEY")
        if not dashboard_url or not project_id:
            return
        from supabase import create_client
        dash = create_client(dashboard_url, dashboard_key)
        dash.table("tool_run_logs").insert({
            "project_id": project_id,
            "tool_slug": "shopify-pipeline",
            "step": step,
            "status": status,
            "output_summary": summary[:500],
        }).execute()
    except Exception:
        pass  # logging is best-effort, nooit blokkeren


# ── Stap 0: Status ─────────────────────────────────────────────────────────────

@app.function(image=image, secrets=[secret])
@modal.fastapi_endpoint(method="POST")
def status(body: dict) -> dict:
    """
    Geeft een overzicht van producten per fase en status.
    Body: { "project_id": "..." }  (optioneel, voor logging)
    """
    sb = get_supabase()

    result = sb.table("seo_products").select("fase, status").execute()
    products = result.data or []

    # Tel per fase en status
    counts: dict = {}
    for p in products:
        fase = str(p.get("fase", "?"))
        stat = p.get("status", "?")
        counts.setdefault(fase, {})
        counts[fase][stat] = counts[fase].get(stat, 0) + 1

    total = len(products)

    log_to_dashboard(sb, body.get("project_id", ""), "status", "success",
                     f"{total} producten gevonden")

    return {
        "success": True,
        "total_products": total,
        "per_fase": counts,
    }


# ── Stap 4: Validate ───────────────────────────────────────────────────────────

@app.function(image=image, secrets=[secret], timeout=300)
@modal.fastapi_endpoint(method="POST")
def validate(body: dict) -> dict:
    """
    Valideert producten voor een gegeven fase.
    Body: { "fase": "3", "project_id": "...", "autofix": true }
    """
    fase = str(body.get("fase", "3"))
    autofix = body.get("autofix", True)
    project_id = body.get("project_id", "")

    sb = get_supabase()

    # Producten ophalen
    result = sb.table("seo_products").select("*").eq("fase", fase).in_(
        "status", ["ready", "review"]
    ).execute()
    products = result.data or []

    if not products:
        return {"success": True, "total": 0, "ok": 0, "review": 0,
                "autofixed": 0, "issues": [], "message": f"Geen producten voor fase {fase}"}

    total = len(products)
    required_fields = ["product_title_nl", "ean_shopify", "hoofdcategorie", "verkoopprijs"]

    ok = 0
    review_count = 0
    autofixed = 0
    all_issues = []

    for product in products:
        pid = product["id"]
        sku = product.get("sku", pid)
        issues = []
        updates = {}
        set_review = False

        # Verplichte velden
        for field_name in required_fields:
            val = product.get(field_name)
            if val is None or str(val).strip() in ("", "None", "nan"):
                issues.append(f"leeg verplicht veld: {field_name}")
                set_review = True

        # Meta description te lang (auto-truncate)
        meta = product.get("meta_description") or ""
        if len(meta) > 160:
            if autofix:
                updates["meta_description"] = meta[:160]
            autofixed += 1

        # Prijs <= 0
        prijs = product.get("verkoopprijs")
        if prijs is not None and float(prijs or 0) <= 0:
            issues.append("verkoopprijs is 0 of negatief")
            set_review = True

        # Status bijwerken
        if set_review:
            updates["status"] = "review"
            review_count += 1
        else:
            ok += 1
            if product["status"] == "review":
                updates["status"] = "ready"

        if updates:
            sb.table("seo_products").update(updates).eq("id", pid).execute()

        if issues:
            all_issues.append({"sku": sku, "issues": issues})

    summary = f"fase {fase}: {total} producten, {ok} ok, {review_count} review, {autofixed} autofixed"
    log_to_dashboard(sb, project_id, "validate", "success", summary)

    return {
        "success": True,
        "fase": fase,
        "total": total,
        "ok": ok,
        "review": review_count,
        "autofixed": autofixed,
        "issues": all_issues[:50],  # max 50 teruggeven
        "message": summary,
    }
