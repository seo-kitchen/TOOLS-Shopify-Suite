"""
Importeert alle masterbestanden naar products_raw in de nieuwe Supabase.

Leveranciers:
    - Serax Basic (3157 rijen)
    - Serax New Items (421 rijen)
    - Pottery Pots (1541 rijen)
    - Printworks (270 rijen)
    - S&P (2619 rijen)

Gebruik:
    python execution/import_masterfiles.py
    python execution/import_masterfiles.py --supplier serax_basic
"""

import os, sys, argparse
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
sys.stdout.reconfigure(encoding='utf-8')

sb = create_client(
    os.getenv('SUPABASE_NEW_URL'),
    os.getenv('SUPABASE_NEW_SERVICE_KEY')
)

BATCH_SIZE = 100

def nan_none(val, as_int=False, as_float=False):
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if val is None:
        return None
    s = str(val).strip()
    if s in ('nan', 'NaN', '', '-', 'N/A', 'n/a', '#N/A', 'None'):
        return None
    if as_int:
        try:
            return int(float(s.replace(',', '.')))
        except (ValueError, TypeError):
            return None
    if as_float:
        try:
            return float(s.replace(',', '.'))
        except (ValueError, TypeError):
            return None
    # Auto-detect numeric strings met komma als decimaalscheider
    if ',' in s and '.' not in s:
        try:
            return float(s.replace(',', '.'))
        except (ValueError, TypeError):
            pass
    return val

def insert_batch(rows, naam):
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i+BATCH_SIZE]
        sb.table('products_raw').insert(batch).execute()
        print(f'  {min(i+BATCH_SIZE, len(rows))}/{len(rows)} ingevoegd', end='\r')
    print(f'\n{naam}: {len(rows)} rijen klaar')


# ──────────────────────────────────────────────
# SERAX BASIC
# ──────────────────────────────────────────────
def import_serax_basic():
    df = pd.read_excel(
        'Master Files/Masterdata Serax basic items_Interieur-Shop (1).xlsx',
        header=1
    )
    df = df[df['Brand_id'].notna()]
    print(f'Serax Basic geladen: {len(df)} rijen')

    rows = []
    for _, r in df.iterrows():
        rows.append({
            'sku':                  nan_none(r.get('Brand_id')),
            'supplier':             'serax',
            'fase':                 'masterdata',
            'import_batch':         'serax_basic_2026',
            'product_name_raw':     nan_none(r.get('Product Name Piece (English)')),
            'ean_piece':            nan_none(r.get('EAN Piece')),
            'ean_shopify':          nan_none(r.get('EAN Code Packaging/Giftbox')),
            'designer':             nan_none(r.get('Designer')),
            'kleur_en':             nan_none(r.get('Color')),
            'materiaal_raw':        nan_none(r.get('Product Material')),
            'leverancier_category': nan_none(r.get('Product Category')),
            'leverancier_item_cat': nan_none(r.get('Item Category')),
            'giftbox':              nan_none(r.get('Giftbox availble?')),
            'giftbox_qty':          nan_none(r.get('Giftbox quantity'), as_int=True),
        })
    insert_batch(rows, 'Serax Basic')


# ──────────────────────────────────────────────
# SERAX NEW ITEMS 2026
# ──────────────────────────────────────────────
def import_serax_new_items():
    df = pd.read_excel(
        'Master Files/Masterdata serax new items_2026_Interieur-Shop (2).xlsx',
        header=1
    )
    df = df[df['Brand_id'].notna()] if 'Brand_id' in df.columns else df[df[df.columns[0]].notna()]
    sku_col = 'Brand_id' if 'Brand_id' in df.columns else df.columns[0]
    print(f'Serax New Items geladen: {len(df)} rijen')

    rows = []
    for _, r in df.iterrows():
        rows.append({
            'sku':                  nan_none(r.get(sku_col)),
            'supplier':             'serax',
            'fase':                 'masterdata',
            'import_batch':         'serax_new_items_2026',
            'product_name_raw':     nan_none(r.get('Product Name Piece (English)')),
            'ean_piece':            nan_none(r.get('EAN Piece')),
            'ean_shopify':          nan_none(r.get('EAN Code Packaging/Giftbox')),
            'designer':             nan_none(r.get('Designer')),
            'kleur_en':             nan_none(r.get('Color')),
            'materiaal_raw':        nan_none(r.get('Product Material')),
            'leverancier_category': nan_none(r.get('Product Category')),
            'leverancier_item_cat': nan_none(r.get('Item Category')),
            'giftbox':              nan_none(r.get('Giftbox availble?')),
            'giftbox_qty':          nan_none(r.get('Giftbox quantity'), as_int=True),
        })
    insert_batch(rows, 'Serax New Items')


# ──────────────────────────────────────────────
# POTTERY POTS
# ──────────────────────────────────────────────
def import_pottery_pots():
    df = pd.read_excel('Master Files/Pricelist 2026 Pottery pots.xlsx')
    df = df[df['Articlecode'].notna()]
    print(f'Pottery Pots geladen: {len(df)} rijen')

    # Prijs kolom zoeken
    prijs_col = next((c for c in df.columns if 'Pricelist' in str(c) or 'price' in str(c).lower()), None)
    rrp_col = next((c for c in df.columns if 'RRP' in str(c)), None)

    rows = []
    for _, r in df.iterrows():
        rows.append({
            'sku':                  nan_none(r.get('Articlecode')),
            'supplier':             'pottery_pots',
            'fase':                 'masterdata',
            'import_batch':         'pottery_pots_2026',
            'product_name_raw':     nan_none(r.get('Description')),
            'ean_piece':            nan_none(r.get('EAN-UCC _Code')),
            'ean_shopify':          nan_none(r.get('EAN-UCC _Code')),
            'kleur_en':             nan_none(r.get('Color name')),
            'materiaal_raw':        nan_none(r.get('Material')),
            'leverancier_category': nan_none(r.get('Collection')),
            'leverancier_item_cat': nan_none(r.get('Form')),
            'rrp_stuk_eur':         nan_none(r.get(rrp_col)) if rrp_col else None,
            'inkoopprijs_stuk_eur': nan_none(r.get(prijs_col)) if prijs_col else None,
            'hoogte_cm':            nan_none(r.get('Height\nsingle item\n(cm)')),
            'lengte_cm':            nan_none(r.get('Length\nsingle item\n (cm)')),
            'breedte_cm':           nan_none(r.get('Width\nsingle item \n(cm) ')),
        })
    insert_batch(rows, 'Pottery Pots')


# ──────────────────────────────────────────────
# PRINTWORKS
# ──────────────────────────────────────────────
def import_printworks():
    df = pd.read_excel('Master Files/Prijslijst Printworks (1).xlsx')
    df = df[df['Artikelcode'].notna()]
    print(f'Printworks geladen: {len(df)} rijen')

    rrp_col = next((c for c in df.columns if 'advies' in str(c).lower() or 'retail' in str(c).lower()), None)
    ink_col = next((c for c in df.columns if 'netto' in str(c).lower() or 'nettoprijsprijs' in str(c).lower() or '2026' in str(c)), None)

    rows = []
    for _, r in df.iterrows():
        rows.append({
            'sku':                  nan_none(r.get('Artikelcode')),
            'supplier':             'printworks',
            'fase':                 'masterdata',
            'import_batch':         'printworks_2026',
            'product_name_raw':     nan_none(r.get('Productnaam')),
            'ean_piece':            nan_none(r.get('EAN code per stuk')),
            'ean_shopify':          nan_none(r.get('EAN code per stuk')),
            'kleur_en':             nan_none(r.get('Kleur (1)')),
            'materiaal_raw':        nan_none(r.get('Materiaal (1)')),
            'leverancier_category': nan_none(r.get('Collectie')),
            'rrp_stuk_eur':         nan_none(r.get(rrp_col)) if rrp_col else None,
            'inkoopprijs_stuk_eur': nan_none(r.get(ink_col)) if ink_col else None,
        })
    insert_batch(rows, 'Printworks')


# ──────────────────────────────────────────────
# SALT & PEPPER
# ──────────────────────────────────────────────
def import_sp():
    df = pd.read_excel('Master Files/Volledige lijst S&P.xlsx')
    df = df[df['Item Nr.'].notna()]
    print(f'S&P geladen: {len(df)} rijen')

    rows = []
    for _, r in df.iterrows():
        rows.append({
            'sku':                  nan_none(r.get('Item Nr.')),
            'supplier':             'sp',
            'fase':                 'masterdata',
            'import_batch':         'sp_2026',
            'product_name_raw':     nan_none(r.get('Omschrijving ')),
            'ean_piece':            nan_none(r.get('Barcode')),
            'ean_shopify':          nan_none(r.get('Barcode')),
            'kleur_en':             nan_none(r.get('Kleur')),
            'materiaal_raw':        nan_none(r.get('Materiaal')),
            'leverancier_category': nan_none(r.get('Range/Reeks / Série/Serie')),
            'rrp_stuk_eur':         nan_none(r.get(' SRP / Publieke Prijs / Prix de vente')),
            'inkoopprijs_stuk_eur': nan_none(r.get('Buying price / Aankoopprijs / Prix achat ')),
            'giftbox':              nan_none(r.get('Giftbox X = Yes / Geschenkverpakking X = ja / Boîte Cadeau X = oui')),
            'hoogte_cm':            nan_none(r.get('Hoogte CM')),
            'lengte_cm':            nan_none(r.get('Lengte CM')),
            'breedte_cm':           nan_none(r.get('Breedte CM')),
        })
    insert_batch(rows, 'S&P')


# ──────────────────────────────────────────────
# SERAX FASE 4 BATCH → products_curated
# ──────────────────────────────────────────────
def import_serax_fase4_curated():
    df = pd.read_excel('Eigen Bestande/Serax_Batch_20260414_0139 (1).xlsx')
    df = df[df['Variant SKU'].notna()]
    print(f'Serax Fase4 batch geladen: {len(df)} rijen')

    rows = []
    for _, r in df.iterrows():
        rows.append({
            'sku':               nan_none(r.get('Variant SKU')),
            'supplier':          'serax',
            'fase':              'fase4',
            'product_title_nl':  nan_none(r.get('Product title')),
            'handle':            nan_none(r.get('Product handle')),
            'hoofdcategorie':    nan_none(r.get('Nieuwe hoofdcategorie')),
            'subcategorie':      nan_none(r.get('Nieuwe subcategorie')),
            'sub_subcategorie':  nan_none(r.get('Nieuwe sub-subcategorie')),
            'tags':              nan_none(r.get('Nieuwe tag')),
            'collectie':         nan_none(r.get('collectie')),
            'materiaal_nl':      nan_none(r.get('materiaal')),
            'kleur_nl':          nan_none(r.get('kleur')),
            'meta_description':  nan_none(r.get('meta_description')),
            'verkoopprijs':      nan_none(r.get('Verkoopprijs Shopify')),
            'inkoopprijs':       nan_none(r.get('Inkoopprijs Shopify')),
            'pipeline_status':   'transformed',
        })

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i+BATCH_SIZE]
        sb.table('products_curated').upsert(batch, on_conflict='sku').execute()
        print(f'  {min(i+BATCH_SIZE, len(rows))}/{len(rows)} ingevoegd', end='\r')
    print(f'\nSerax Fase4 curated: {len(rows)} rijen klaar')


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
SUPPLIERS = {
    'serax_basic':      import_serax_basic,
    'serax_new_items':  import_serax_new_items,
    'pottery_pots':     import_pottery_pots,
    'printworks':       import_printworks,
    'sp':               import_sp,
    'fase4_curated':    import_serax_fase4_curated,
}

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--supplier', default='all', choices=list(SUPPLIERS.keys()) + ['all'])
    args = parser.parse_args()

    if args.supplier == 'all':
        for naam, fn in SUPPLIERS.items():
            print(f'\n=== {naam.upper()} ===')
            fn()
    else:
        SUPPLIERS[args.supplier]()

    print('\nImport voltooid.')
