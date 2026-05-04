# Directive: Product Onboarding Pipeline — Overzicht

## Doel
Verwerk een leveranciers-Excel automatisch naar kant-en-klare Shopify-importbestanden via Supabase + Claude agent.

## Uitgangspunten
1. **Elke fout is rampzalig** — het systeem raadt nooit. Alleen automatisch als 100% zeker, anders stoppen en vragen.
2. **Interactieve controle per stap** — Claude voert elke stap uit, rapporteert terug, en wacht op bevestiging.
3. **Supabase is de enige bron van waarheid** — alle masterdata staat in de database.

## Database-tabellen

| Tabel | Inhoud | Gevuld door |
|-------|--------|-------------|
| `seo_products` | Alle masterdata van de leverancier | `ingest.py` |
| `seo_category_mapping` | SOP-categorietabel | `seed_categories.py` (eenmalig) |
| `seo_shopify_index` | Shopify matching-index (SKU + EAN) | `load_website_structure.py` |
| `seo_website_collections` | Bestaande Shopify-collecties | `load_website_structure.py` |
| `seo_filter_values` | Bestaande filterwaarden (kleur, materiaal) | `load_website_structure.py` |
| `seo_import_runs` | Logboek per batch | alle scripts |

## Pipeline-volgorde

### Eenmalige setup (bij start of na grote structuurwijziging)
```bash
python execution/seed_categories.py
python execution/load_website_structure.py --webshop active.csv --archive archive.csv
```

### Per batch (interactief)
```bash
python execution/ingest.py --file masterdata.xlsx --fase 3 [--fotos foto_export.xlsx]
python execution/match.py --fase 3
python execution/transform.py --fase 3
python execution/validate.py --fase 3
python execution/export.py --fase 3 --output ./exports/
```

## Interactief model
- Na elke stap rapporteert Claude: aantallen, waarschuwingen, twijfelgevallen
- Twijfelgevallen (matching, categorisering, nieuwe filterwaarden) worden één voor één voorgelegd
- Je bevestigt of corrigeert voor de volgende stap begint

## Input per batch
- `masterdata.xlsx` — Serax masterdata Excel
- `foto_export.xlsx` — foto-export Excel van Serax (optioneel)
- `active_products.csv` — Shopify webshop export (eenmalig / bij update)
- `archive_products.csv` — Shopify archief export (eenmalig / bij update)

## Output
- `exports/Shopify_Nieuw_fase{n}.xlsx` — nieuwe producten
- `exports/Shopify_Archief_fase{n}.xlsx` — te reactiveren producten

## Referentie
- SOP: `.tmp/SOP_Serax_Product_Onboarding_v2.7.docx`
- Technische architectuur: `.tmp/Technische_Architectuur_Automation.docx`
