# Directive: match.py — Matching tegen Shopify-index

## Doel
Match producten (status="raw") uit `seo_products` tegen de Shopify-index in `seo_shopify_index`.
100% zeker = automatisch verwerken. Twijfel = stoppen en vragen.

## Vereiste: website-structuur geladen
Draai eerst `load_website_structure.py` als `seo_shopify_index` leeg is.

## Gebruik
```bash
python execution/match.py --fase 3
```

## Matchlogica (strikt)

| Situatie | Actie |
|----------|-------|
| Exacte SKU-match (actief of archief) | auto: `status_shopify = actief/archief`, 100% |
| Geen SKU-match, geen EAN-match | auto: `status_shopify = nieuw`, 100% |
| EAN-match maar geen SKU-match | STOPPEN — voorleggen aan gebruiker |
| SKU-match én EAN behoort aan ander SKU | STOPPEN — data-conflict voorleggen |
| SKU staat zowel actief als archief | STOPPEN — welke is leidend? |

## Interactieve beslissing bij twijfel
Script toont de situatie en wacht op: `actief / archief / nieuw / overslaan`

Bij "overslaan": product blijft op status="raw" met `review_reden`.
Bij expliciete keuze: `match_zekerheid = "100%"` (handmatig bevestigd).

## Output (in Supabase)
- `status_shopify`: actief / archief / nieuw
- `match_methode`: sku / ean / geen
- `match_zekerheid`: 100% / twijfel
- `shopify_product_id` + `shopify_variant_id` bij match
- `review_reden`: beschrijving bij twijfelgeval

## Wanneer opnieuw draaien
- Na het updaten van `seo_shopify_index` (nieuwe webshop-export)
- Na handmatige correctie van een twijfelgeval
