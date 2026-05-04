"""
Migreert data van oude Supabase naar nieuwe Supabase.
Splitst seo_products in products_raw + products_curated.

Gebruik:
    python execution/migrate_to_new_supabase.py
"""

import os, sys
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

sb_nieuw = create_client(
    os.getenv('SUPABASE_NEW_URL'),
    os.getenv('SUPABASE_NEW_SERVICE_KEY')
)

# Supplier afleiden uit leverancier_category
SUPPLIER_MAP = {
    'Pottery&Urban Jungle': 'pottery_pots',
    'Use and Care': 'pottery_pots',
    'Natural': 'pottery_pots',
    'Refined': 'pottery_pots',
    'Essential': 'pottery_pots',
    'jigger': 'printworks',
    'Dinnerware': 'serax',
    'Lighting': 'serax',
    'Furniture Indoor': 'serax',
    'drinking_glass': 'serax',
    'Interior Acc.': 'serax',
    'Glassware': 'serax',
    'Pottery&UJ': 'serax',
}

def afleiden_supplier(cat):
    if pd.isna(cat):
        return 'onbekend'
    return SUPPLIER_MAP.get(str(cat).strip(), 'serax')

def nan_to_none(val, as_int=False):
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if val is None or str(val) == 'nan':
        return None
    if as_int:
        try:
            return int(float(val))
        except (ValueError, TypeError):
            return None
    return val

def migreer():
    df = pd.read_csv('.tmp/export_seo_products.csv')
    print(f'Geladen: {len(df)} producten uit oude Supabase')

    raw_rows = []
    curated_rows = []

    for _, r in df.iterrows():
        supplier = afleiden_supplier(r.get('leverancier_category'))

        # --- PRODUCTS RAW ---
        raw = {
            'sku':                  nan_to_none(r.get('sku')),
            'supplier':             supplier,
            'fase':                 nan_to_none(r.get('fase')) or '3april',
            'import_batch':         'migratie_oud_naar_nieuw',
            'product_name_raw':     nan_to_none(r.get('product_name_raw')),
            'ean_piece':            nan_to_none(r.get('ean_piece')),
            'ean_shopify':          nan_to_none(r.get('ean_shopify')),
            'designer':             nan_to_none(r.get('designer')),
            'kleur_en':             nan_to_none(r.get('kleur_en')),
            'materiaal_raw':        nan_to_none(r.get('materiaal_nl')),
            'hoogte_cm':            nan_to_none(r.get('hoogte_cm')),
            'lengte_cm':            nan_to_none(r.get('lengte_cm')),
            'breedte_cm':           nan_to_none(r.get('breedte_cm')),
            'giftbox':              nan_to_none(r.get('giftbox')),
            'giftbox_qty':          nan_to_none(r.get('giftbox_qty'), as_int=True),
            'rrp_stuk_eur':         nan_to_none(r.get('rrp_stuk_eur')),
            'rrp_gb_eur':           nan_to_none(r.get('rrp_gb_eur')),
            'inkoopprijs_stuk_eur': nan_to_none(r.get('inkoopprijs_stuk_eur')),
            'inkoopprijs_gb_eur':   nan_to_none(r.get('inkoopprijs_gb_eur')),
            'leverancier_category': nan_to_none(r.get('leverancier_category')),
            'leverancier_item_cat': nan_to_none(r.get('leverancier_item_cat')),
            'photo_packshot_1':     nan_to_none(r.get('photo_packshot_1')),
            'photo_packshot_2':     nan_to_none(r.get('photo_packshot_2')),
            'photo_packshot_3':     nan_to_none(r.get('photo_packshot_3')),
            'photo_packshot_4':     nan_to_none(r.get('photo_packshot_4')),
            'photo_packshot_5':     nan_to_none(r.get('photo_packshot_5')),
            'photo_lifestyle_1':    nan_to_none(r.get('photo_lifestyle_1')),
            'photo_lifestyle_2':    nan_to_none(r.get('photo_lifestyle_2')),
            'photo_lifestyle_3':    nan_to_none(r.get('photo_lifestyle_3')),
            'photo_lifestyle_4':    nan_to_none(r.get('photo_lifestyle_4')),
            'photo_lifestyle_5':    nan_to_none(r.get('photo_lifestyle_5')),
        }
        raw_rows.append(raw)

        # Pipeline status mappen
        status_oud = str(r.get('status', 'raw'))
        pipeline_status = 'transformed' if status_oud == 'ready' else 'ingested'

        # --- PRODUCTS CURATED ---
        curated = {
            'sku':               nan_to_none(r.get('sku')),
            'supplier':          supplier,
            'fase':              nan_to_none(r.get('fase')) or '3april',
            'product_title_nl':  nan_to_none(r.get('product_title_nl')),
            'handle':            nan_to_none(r.get('handle')),
            'hoofdcategorie':    nan_to_none(r.get('hoofdcategorie')),
            'subcategorie':      nan_to_none(r.get('subcategorie')),
            'sub_subcategorie':  nan_to_none(r.get('sub_subcategorie')),
            'collectie':         nan_to_none(r.get('collectie')),
            'tags':              nan_to_none(r.get('tags')),
            'materiaal_nl':      nan_to_none(r.get('materiaal_nl')),
            'kleur_nl':          nan_to_none(r.get('kleur_nl')),
            'meta_description':  nan_to_none(r.get('meta_description')),
            'verkoopprijs':      nan_to_none(r.get('verkoopprijs')),
            'inkoopprijs':       nan_to_none(r.get('inkoopprijs')),
            'pipeline_status':   pipeline_status,
            'review_reden':      nan_to_none(r.get('review_reden')),
        }
        curated_rows.append(curated)

    # Batch insert products_raw
    print('products_raw inserteren...')
    batch_size = 100
    for i in range(0, len(raw_rows), batch_size):
        batch = raw_rows[i:i+batch_size]
        sb_nieuw.table('products_raw').insert(batch).execute()
        print(f'  {min(i+batch_size, len(raw_rows))}/{len(raw_rows)}', end='\r')
    print(f'\nproducts_raw: {len(raw_rows)} rijen ingevoegd')

    # Haal raw IDs op voor de koppeling
    print('raw_id koppeling ophalen...')
    res = sb_nieuw.table('products_raw').select('id,sku').eq('import_batch', 'migratie_oud_naar_nieuw').execute()
    sku_to_raw_id = {r['sku']: r['id'] for r in res.data}

    # Voeg raw_id toe aan curated
    for c in curated_rows:
        c['raw_id'] = sku_to_raw_id.get(c['sku'])

    # Batch insert products_curated
    print('products_curated inserteren...')
    for i in range(0, len(curated_rows), batch_size):
        batch = curated_rows[i:i+batch_size]
        sb_nieuw.table('products_curated').insert(batch).execute()
        print(f'  {min(i+batch_size, len(curated_rows))}/{len(curated_rows)}', end='\r')
    print(f'\nproducts_curated: {len(curated_rows)} rijen ingevoegd')

    print('\nMigratie voltooid.')
    print(f'  products_raw:     {len(raw_rows)} rijen')
    print(f'  products_curated: {len(curated_rows)} rijen')

if __name__ == '__main__':
    migreer()
