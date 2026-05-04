"""
Synct Shopify producten naar de shopify_sync tabel.
Matcht op SKU tegen products_curated om te zien wat live staat.

Gebruik:
    python execution/shopify_sync.py              # sync alle curated SKUs
    python execution/shopify_sync.py --fase fase4  # alleen fase4
"""

import os, sys, argparse, requests, time
sys.stdout.reconfigure(encoding='utf-8')
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

STORE = os.getenv('SHOPIFY_STORE')
TOKEN = os.getenv('SHOPIFY_ACCESS_TOKEN')
API   = '2026-04'

sb = create_client(os.getenv('SUPABASE_NEW_URL'), os.getenv('SUPABASE_NEW_SERVICE_KEY'))


def shopify_get_all(endpoint, params=None):
    url = f'https://{STORE}/admin/api/{API}/{endpoint}'
    headers = {'X-Shopify-Access-Token': TOKEN}
    results = []
    while url:
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        key = list(data.keys())[0]
        results.extend(data[key])
        link = resp.headers.get('Link', '')
        url, params = None, None
        if 'rel="next"' in link:
            for part in link.split(','):
                if 'rel="next"' in part:
                    url = part.split(';')[0].strip().strip('<>')
    return results


def sync(fase=None):
    # Haal curated SKUs op die we willen checken
    query = sb.table('products_curated').select('sku, fase, pipeline_status')
    if fase:
        query = query.eq('fase', fase)
    curated = query.execute().data
    curated_skus = {r['sku']: r for r in curated}
    print(f'Curated SKUs te checken: {len(curated_skus)} (fase={fase or "alle"})')

    # Fetch alle Shopify producten in pages, match on the fly
    print('Shopify producten ophalen in pages...')
    headers_shopify = {'X-Shopify-Access-Token': TOKEN}
    remaining_skus = set(curated_skus.keys())
    matched, page = [], 0
    url = f'https://{STORE}/admin/api/{API}/products.json'
    # published_status=published pakt active producten (draft staat ook live in Shopify)
    # Gearchiveerde producten worden standaard overgeslagen (18k+ rijen, niet relevant)
    params = {'limit': 250, 'published_status': 'published', 'fields': 'id,title,handle,vendor,status,published_at,tags,variants'}

    while url and remaining_skus:
        time.sleep(0.6)  # max ~1.6 req/sec, binnen Shopify limiet van 2/sec
        resp = requests.get(url, headers=headers_shopify, params=params)
        if resp.status_code == 429:
            print('\n  Rate limit, 10 seconden wachten...')
            time.sleep(10)
            resp = requests.get(url, headers=headers_shopify, params=params)
        resp.raise_for_status()
        products = resp.json().get('products', [])
        page += 1
        print(f'  Page {page}: {len(products)} producten | nog {len(remaining_skus)} SKUs te vinden', end='\r')

        for p in products:
            for v in p.get('variants', []):
                sku = str(v.get('sku', '') or '').strip()
                if sku in remaining_skus:
                    matched.append({
                        'shopify_product_id': str(p['id']),
                        'shopify_variant_id': str(v['id']),
                        'sku':                sku,
                        'ean':                str(v.get('barcode', '') or ''),
                        'title':              p.get('title', ''),
                        'handle':             p.get('handle', ''),
                        'vendor':             p.get('vendor', ''),
                        'shopify_status':     p.get('status', ''),
                        'published_at':       p.get('published_at'),
                        'price':              float(v.get('price', 0) or 0),
                        'tags':               p.get('tags', ''),
                        'collection_handles': [],
                        'fase':               curated_skus[sku].get('fase', fase or 'onbekend'),
                    })
                    remaining_skus.discard(sku)

        # Volgende page via Link header
        link = resp.headers.get('Link', '')
        url, params = None, None
        if 'rel="next"' in link:
            for part in link.split(','):
                if 'rel="next"' in part:
                    url = part.split(';')[0].strip().strip('<>')

    not_found = list(remaining_skus)
    print(f'\nKlaar: {page} pages doorlopen.')

    print(f'\nResultaat:')
    print(f'  Gevonden in Shopify:    {len(matched)}')
    print(f'  Niet gevonden:          {len(not_found)}')

    # Upsert naar shopify_sync
    if matched:
        for i in range(0, len(matched), 100):
            batch = matched[i:i+100]
            sb.table('shopify_sync').upsert(batch, on_conflict='shopify_product_id').execute()
        print(f'  shopify_sync bijgewerkt: {len(matched)} rijen')

    # Update pipeline_status in products_curated
    for row in matched:
        status = 'live' if row['shopify_status'] == 'active' else 'in_shopify'
        sb.table('products_curated').update({'pipeline_status': status}).eq('sku', row['sku']).execute()

    if not_found:
        print(f'\nNiet gevonden in Shopify ({len(not_found)} SKUs):')
        for sku in not_found[:20]:
            print(f'  {sku}')
        if len(not_found) > 20:
            print(f'  ... en {len(not_found)-20} meer')

    # Statusoverzicht
    print('\n=== STATUSOVERZICHT ===')
    res = sb.table('products_curated').select('fase, pipeline_status').execute()
    from collections import Counter
    counts = Counter((r['fase'], r['pipeline_status']) for r in res.data)
    for (f, s), n in sorted(counts.items()):
        print(f'  {f:12s} | {s:15s} | {n} producten')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--fase', default=None)
    args = parser.parse_args()
    sync(args.fase)
