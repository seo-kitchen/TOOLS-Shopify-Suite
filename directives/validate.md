# Directive: validate.py — kwaliteitscheck

## Doel
Controleer alle producten met `status = "ready"` op kwaliteitsproblemen voordat ze geëxporteerd worden.

## Input
- `--fase` — fasecode (bijv. `3`)

## Checks

| Check | Actie bij fout |
|---|---|
| Dubbele EAN in database | Flag beide records als `"review"`, log |
| Lege verplichte velden (titel, EAN, categorie, prijs) | `status = "review"` |
| Afmetingen met komma ipv punt | Automatisch corrigeren |
| Afmeting = 0 of leeg | Log als waarschuwing |
| Ontbrekende foto-URLs | Log als waarschuwing |
| Meta description > 160 tekens | Log, auto-truncate als mogelijk |
| Prijs = 0 of negatief | `status = "review"` |
| Tags ontbreken | Log als waarschuwing |

## Output
- Terminal-rapport:
  ```
  ✓ X producten klaar voor export
  ⚠ Y producten voor handmatige controle (zie .tmp/review_fase3.csv)
  ✗ Z fouten gevonden
  ```
- `.tmp/review_fase{n}.csv` — overzicht van alle producten met `status = "review"` + reden
