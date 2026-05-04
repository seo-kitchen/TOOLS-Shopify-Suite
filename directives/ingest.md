# Directive: ingest.py — Excel inladen

## Doel
Valideer de leveranciers-Excel en laad de data in de Supabase `products` tabel met `status = "raw"`.

## Input
- `--file` — pad naar de leveranciers Excel (masterdata)
- `--fase` — fasecode (bijv. `3`)

## Verplichte kolommen in de Excel
- SKU (brand_id)
- EAN Piece
- EAN Packaging/Giftbox
- Product Material
- Hoogte, Lengte, Breedte (in cm)
- Inkoopprijs
- Designer
- Foto-URLs (packshot + lifestyle)

## Stappen
1. Lees het Excel-bestand met `pandas`
2. Valideer aanwezigheid van alle verplichte kolommen — stop met foutmelding als iets ontbreekt
3. Normaliseer EAN: gebruik altijd `EAN Packaging/Giftbox` als `ean_shopify`
4. Sla `EAN Piece` op als `ean_piece` (referentie, niet voor Shopify)
5. Laad alle rijen in Supabase `products` met:
   - `status = "raw"`
   - `fase = <fasecode>`
   - `created_at = now()`
6. Sla een regel op in `import_runs` met datum, aantal producten, en bestandsnaam

## Edge cases
- Dubbele EANs in het invoerbestand → flaggen in `import_runs`, wel doorladen
- Lege EAN Packaging kolom → gebruik EAN Piece als fallback, log als waarschuwing
- Afmetingen met komma ipv punt → automatisch omzetten
- Bestand niet gevonden → stop met foutmelding

## Output
- Supabase `products` gevuld met `status = "raw"`
- Log in `import_runs`
- Samenvatting in terminal: X producten geladen, Y waarschuwingen
