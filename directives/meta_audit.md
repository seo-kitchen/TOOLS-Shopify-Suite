# Directive: Shopify meta title & description audit

## Doel
Alle actieve Shopify producten auditen op meta title + meta description, en
voor producten die niet voldoen een voorstel genereren via Claude. Goede meta's
blijven ongemoeid maar worden wel in Supabase opgenomen voor overzicht.

## Harde regels (project memory)
- **Meta title**: format `{Productnaam} | {Merk} – Interieur Shop`, max 60 tekens.
  Als format >60 → fallback `{Productnaam}` alleen.
- **Set-info ALTIJD behouden**: Als de originele producttitel "Set van X" of "X Stuks"
  bevat, MOET dit zichtbaar blijven in de meta title (bijv. "Set/2", "Set/4", "12 Stuks").
  NOOIT weglaten om binnen tekenlimiet te passen — liever de productnaam inkorten.
  Reden: zonder set-info lijkt het alsof je 1 stuk koopt voor de set-prijs.
- **Meta description**: max 155 tekens. Natuurlijk geschreven via Claude,
  GEEN template/standaardzin.
- **Alleen meta-velden uit exports.** Nooit andere kolommen importeren of
  overschrijven — gecureerde data (omschrijvingen, prijzen, tags) blijft intact.

## Tabel
`shopify_meta_audit` (zie `execution/meta_audit_schema.sql`). Losstaande tabel,
niet gekoppeld aan `products`. 1 rij per Shopify product (geen variants).

## Input
- `master files/Alle Active Producten.xlsx`
  Whitelist kolommen: Product ID, Product handle, Product title, Product vendor,
  Product meta title, Product meta description. Andere kolommen worden genegeerd.

## Workflow

### Stap 1 — Schema aanmaken (eenmalig)
Run `execution/meta_audit_schema.sql` in Supabase SQL editor.

### Stap 2 — Loader
```
python execution/meta_audit_loader.py --file "master files/Alle Active Producten.xlsx"
```
- Leest alleen whitelist-kolommen
- Dedupe op Product ID
- Flagt status per veld: `ok` | `missing` | `too_long` | `too_short` | `duplicate`
- Upsert op `shopify_product_id`

Dry-run:
```
python execution/meta_audit_loader.py --file "..." --dry-run
```

### Stap 3 — Testrun 1 (format validation, 5 producten)
Selecteer 5 producten uit verschillende vendors/categorieën.
Genereer voorstel title + desc, toon side-by-side met huidige.
**Nog geen Shopify push, geen bulk generate.**

### Stap 4 — Testrun 2 (batch ~20 via dashboard)
Pas na akkoord op format. Goedkeuring per product. Push 1-2 live als check.

### Stap 5 — Opschalen
Batches van 50-100 via dashboard. Nooit alles in één keer via Claude
(conform feedback: geen massa-Claude calls).

### Stap 6 — Push naar Shopify
Alleen `review_status = 'approved'`. Schrijf `approved_title` en
`approved_desc` weg naar Shopify via Admin API (metafields / SEO veld).
Zet `pushed_at = now()`.

## Audit-regels (deterministisch)
| Status        | Title criterium   | Desc criterium    |
|---------------|-------------------|-------------------|
| missing       | leeg              | leeg              |
| too_long      | > 60 tekens       | > 155 tekens      |
| too_short     | < 30 tekens       | < 120 tekens      |
| duplicate     | zelfde op ≥2 prod.| zelfde op ≥2 prod.|
| ok            | anders            | anders            |

## Edge cases
- Product ID als float in Excel → cast naar int-string
- Dubbele rijen per variant → `drop_duplicates(subset=['Product ID'])`
- Lege vendor → title fallback naar alleen productnaam

## Output
- Supabase `shopify_meta_audit` gevuld
- Terminal summary: counts per status
- Rewrite-queue = alle rijen met `title_status != 'ok' OR desc_status != 'ok'`
