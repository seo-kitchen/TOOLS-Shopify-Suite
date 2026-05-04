# Directive: load_website_structure.py — Website-structuur inladen

## Doel
Laad de bestaande Shopify-websitestructuur in Supabase zodat de pipeline:
1. Kan matchen op bestaande SKUs en EANs (`seo_shopify_index`)
2. Kan valideren of categorieën al bestaan (`seo_website_collections`)
3. Kan waarschuwen bij nieuwe filterwaarden (`seo_filter_values`)

## Gebruik
```bash
python execution/load_website_structure.py \
    --webshop active_products.csv \
    --archive archive_products.csv
```

## Wanneer uitvoeren
- Eenmalig bij de eerste setup
- Na een grote productbatch in Shopify (nieuw actief of gearchiveerd)
- NIET nodig voor elke verwerking — alleen bij structuurwijzigingen

## Input
- `active_products.csv` — volledige Shopify webshop export (Shopify Admin > Producten > Export > Alle producten)
- `archive_products.csv` — Shopify archief export (optioneel maar aanbevolen)

## Wat er geladen wordt

| Tabel | Inhoud | Hoe |
|-------|--------|-----|
| `seo_shopify_index` | SKU, EAN, Product ID, Variant ID, status | Upsert op SKU |
| `seo_website_collections` | Unieke Product Types uit webshop | Upsert op naam |
| `seo_filter_values` | Unieke kleur-, materiaal- en designer-waarden | Upsert op (type, waarde) |

## Shopify-kolomnamen in de export
- `Variant SKU` → sku
- `Variant Barcode` → ean
- `ID` → shopify_product_id
- `Variant ID` → shopify_variant_id
- `Product Type` → collectie/categorie
- Metafields: `custom.kleur`, `custom.materiaal`, `custom.designer`

## Edge cases
- Actieve export overschrijft archief bij dezelfde SKU (actief is leidend)
- Lege SKU-rijen worden overgeslagen
- Dubbele SKUs in de CSV: eerste rij wint

## Na uitvoering
Check de aantallen in de output. Kloppen deze met wat je op de website ziet?
Zo niet: exporteer opnieuw vanuit Shopify en draai het script nogmaals.
