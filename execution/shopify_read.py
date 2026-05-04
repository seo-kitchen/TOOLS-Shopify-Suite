"""
Haalt alle collecties en producten op uit Shopify (read-only).
Gebruikt client credentials grant voor authenticatie.

Gebruik:
    python execution/shopify_read.py --mode collections
    python execution/shopify_read.py --mode products
    python execution/shopify_read.py --mode vergelijk
"""

import argparse
import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("SHOPIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SHOPIFY_CLIENT_SECRET")
STORE = os.getenv("SHOPIFY_STORE")
API_VERSION = "2026-04"


def get_token():
    token = os.getenv("SHOPIFY_ACCESS_TOKEN")
    if not token:
        raise Exception("SHOPIFY_ACCESS_TOKEN ontbreekt in .env")
    return token


def shopify_get(token, endpoint, params=None):
    url = f"https://{STORE}/admin/api/{API_VERSION}/{endpoint}"
    headers = {"X-Shopify-Access-Token": token}
    results = []
    while url:
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            raise Exception(f"API fout: {resp.status_code} - {resp.text}")
        data = resp.json()
        key = list(data.keys())[0]
        results.extend(data[key])
        # Paginering via Link header
        link = resp.headers.get("Link", "")
        url = None
        params = None
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip().strip("<>")
    return results


def haal_collecties(token):
    custom = shopify_get(token, "custom_collections.json", {"limit": 250})
    smart = shopify_get(token, "smart_collections.json", {"limit": 250})

    rows = []
    for c in custom:
        rows.append({
            "id": c["id"],
            "type": "custom",
            "titel": c["title"],
            "handle": c["handle"],
            "gepubliceerd": "ja" if c.get("published_at") else "nee",
            "published_at": c.get("published_at", ""),
        })
    for c in smart:
        rows.append({
            "id": c["id"],
            "type": "smart",
            "titel": c["title"],
            "handle": c["handle"],
            "gepubliceerd": "ja" if c.get("published_at") else "nee",
            "published_at": c.get("published_at", ""),
        })

    df = pd.DataFrame(rows).sort_values(["gepubliceerd", "titel"])
    return df


def haal_producten(token):
    products = shopify_get(token, "products.json", {"limit": 250, "status": "any"})
    rows = []
    for p in products:
        collections = []
        rows.append({
            "id": p["id"],
            "titel": p["title"],
            "handle": p["handle"],
            "status": p["status"],
            "vendor": p.get("vendor", ""),
            "product_type": p.get("product_type", ""),
            "tags": p.get("tags", ""),
            "aangemaakt": p.get("created_at", ""),
            "gepubliceerd": p.get("published_at", ""),
        })
    df = pd.DataFrame(rows)
    return df


def vergelijk_met_database(token):
    from supabase import create_client
    sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

    print("Shopify collecties ophalen...")
    df_cols = haal_collecties(token)
    shopify_titels = set(df_cols["titel"].str.lower())

    print("Supabase categorieën ophalen...")
    res = sb.table("seo_products").select("hoofdcategorie,subcategorie,sub_subcategorie").execute()
    data = res.data

    # Unieke categorieën uit Supabase
    cats = set()
    for r in data:
        for val in [r.get("sub_subcategorie"), r.get("subcategorie"), r.get("hoofdcategorie")]:
            if val:
                cats.add(val.strip())

    print(f"\n{'='*60}")
    print(f"Shopify collecties: {len(df_cols)}")
    print(f"  Online:  {len(df_cols[df_cols['gepubliceerd']=='ja'])}")
    print(f"  Offline: {len(df_cols[df_cols['gepubliceerd']=='nee'])}")
    print(f"\nSupabase categorieën: {len(cats)}")

    print(f"\n--- SUPABASE categorieën NIET in Shopify ---")
    missing = [c for c in sorted(cats) if c.lower() not in shopify_titels]
    for c in missing:
        print(f"  ONTBREEKT: {c}")

    print(f"\n--- Shopify collecties OFFLINE (hebben producten nodig) ---")
    offline = df_cols[df_cols["gepubliceerd"] == "nee"][["titel", "handle", "type"]]
    print(offline.to_string(index=False))

    return df_cols


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="collections",
                        choices=["collections", "products", "vergelijk"])
    args = parser.parse_args()

    print("Token ophalen...")
    token = get_token()
    print("Token OK\n")

    if args.mode == "collections":
        df = haal_collecties(token)
        print(f"Totaal: {len(df)} collecties\n")
        print(df.to_string(index=False))
        df.to_csv(".tmp/shopify_collecties.csv", index=False)
        print("\nOpgeslagen in .tmp/shopify_collecties.csv")

    elif args.mode == "products":
        df = haal_producten(token)
        print(f"Totaal: {len(df)} producten\n")
        status_counts = df["status"].value_counts()
        print(status_counts.to_string())
        df.to_csv(".tmp/shopify_producten.csv", index=False)
        print("\nOpgeslagen in .tmp/shopify_producten.csv")

    elif args.mode == "vergelijk":
        vergelijk_met_database(token)
