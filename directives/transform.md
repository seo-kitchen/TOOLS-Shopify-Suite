# Directive: transform.py — SOP-stappen uitvoeren

## Doel
Verwerk alle producten met `status = "raw"` via de SOP-regels. Claude agent wordt ingeschakeld voor contextgevoelige stappen.

## Input
- `--fase` — fasecode (bijv. `3`)

## SOP-stappen per product (volgorde)

1. **EAN** — altijd `ean_shopify = EAN Packaging/Giftbox`

2. **Categorie** — opzoeken in `category_mapping` tabel:
   - Match op `leverancier_category` + `leverancier_item_cat` + optioneel `keyword`
   - Vul `hoofdcategorie`, `subcategorie`, `sub_subcategorie`
   - Geen match → `status = "review"`, log voor handmatige controle

3. **Tags genereren** — formaat:
   ```
   cat_{hoofdcategorie}, cat_{subcategorie}, cat_{sub_subcategorie}, structuur_fase{n}
   ```
   Alles lowercase, spaties vervangen door underscore

4. **Materiaal vertalen** → Nederlands (hardcoded vertaallijst + Claude voor onbekende materialen)

5. **Kleur vertalen** → Nederlands (hardcoded vertaallijst + Claude voor onbekende kleuren)

6. **Producttitel opbouwen**:
   ```
   Serax - {Designer} - {PRODUCTTYPE} {KLEUR}
   ```
   Uitzonderingen (lampen, sets, owl vase) → Claude controleert en past aan

7. **Meta description genereren** via Claude:
   ```
   [Producttype] van [Designer] by [Merk]. [Materiaal], [kleur]. [Subcategorie]. H x L x B cm.
   ```
   Altijd max 160 tekens, SEO-geoptimaliseerd, natuurlijke Nederlandse zin

8. **Prijzen berekenen**:
   - Verkoopprijs = inkoopprijs × marge (uit config)
   - Controleer of prijs voor stuk of giftbox geldt

9. **Foto-URLs koppelen** vanuit foto-export Excel (aparte stap of kolom in masterdata)

10. **Status zetten**:
    - Alles compleet → `status = "ready"`
    - Ontbrekende verplichte data → `status = "review"`

## Claude agent inschakelen voor
- Meta description (altijd)
- Producttitel edge cases
- Categorie twijfelgevallen (vraag gebruiker of kies op basis van productnaam)
- Onbekende materialen / kleuren

## Output
- Supabase `products` bijgewerkt met `status = "ready"` of `"review"`
- Terminal: X ready, Y review, Z fouten

## Edge cases
- Ontbrekende foto-URLs → `status = "review"`
- Afmetingen 0 of leeg → log als waarschuwing
- Designer naam onbekend → gebruik leveranciersnaam als fallback
