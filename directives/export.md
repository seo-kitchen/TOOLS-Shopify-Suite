# Directive: export.py — Shopify bestanden genereren

## Doel
Genereer de Shopify importbestanden voor Hextom Bulk Product Edit op basis van alle producten met `status = "ready"`.

## Input
- `--fase` — fasecode (bijv. `3`)
- `--output` — outputmap (standaard: `./exports/`)

## Stappen
1. Haal alle producten op met `status = "ready"` en `fase = <fasecode>`
2. Splits op `status_shopify`:
   - `nieuw` → `Shopify_Nieuw.xlsx`
   - `archief` → `Shopify_Archief.xlsx`
3. Genereer beide Excel-bestanden met de 23+ kolommen hieronder
4. EAN opslaan als **tekst** (niet als getal — anders verliest Excel de voorloopnullen)

## Shopify kolommen (Hextom formaat)

| Kolom | Bron |
|---|---|
| Handle | slug van producttitel |
| Title | `product_title_nl` |
| Body (HTML) | meta description als `<p>` |
| Vendor | `Serax` (altijd) |
| Type | `hoofdcategorie` |
| Tags | `tags` |
| Published | `TRUE` |
| Variant SKU | `sku` |
| Variant Barcode | `ean_shopify` (als tekst) |
| Variant Price | `verkoopprijs` |
| Variant Compare At Price | (leeg of adviesprijs) |
| Metafield: meta_description | `meta_description` |
| Image Src | foto-URLs (één rij per foto) |
| Image Position | 1, 2, 3... |
| Metafield: materiaal | `materiaal_nl` |
| Metafield: kleur | `kleur_nl` |
| Metafield: hoogte | `hoogte_cm` |
| Metafield: lengte | `lengte_cm` |
| Metafield: breedte | `breedte_cm` |
| Metafield: designer | `designer` |
| Metafield: ean_piece | `ean_piece` |

## Output
- `exports/Shopify_Nieuw.xlsx`
- `exports/Shopify_Archief.xlsx`
- Terminal: X nieuwe producten, Y archiefproducten geëxporteerd

## Edge cases
- Meerdere foto's per product → meerdere rijen met zelfde Handle, alleen Image Src + Position verschilt
- EAN als getal opgeslagen → forceer tekstformaat via `openpyxl` cell format
- Lege exports (geen producten) → waarschuwing, geen leeg bestand aanmaken
