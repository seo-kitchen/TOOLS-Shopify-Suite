# SEOkitchen Dashboard v2

Unified Streamlit dashboard voor de hele Shopify-onboarding pipeline.

## Quick start

```bash
streamlit run dashboard_v2/app.py
```

Open http://localhost:8501 — de sidebar toont 6 groepen met in totaal 22 pagina's.

## Eerste keer gebruik

1. **Supabase tabellen aanmaken** — draai [`execution/schema_v2_dashboard.sql`](../execution/schema_v2_dashboard.sql) in Supabase SQL editor. Creëert `seo_learnings` + `seo_job_locks`.
2. **Learnings migreren** — draai het migratie-script:
   ```bash
   python -m execution.migrate_learnings_to_supabase --commit
   ```
   (dry-run: weglaten `--commit`). Importeert de 24 rijen uit `config/learnings.json` naar `seo_learnings`.
3. **Start de app** en ga naar **Learning system → Learnings** om de migratie te verifiëren.

## Structuur

```
dashboard_v2/
├── app.py                 # entry (st.navigation)
├── ui/
│   ├── supabase_client.py # cached get_supabase() + get_claude_client()
│   ├── layout.py          # page_header, side_by_side, kpi_card, result_panel
│   ├── column_detect.py   # SKU/EAN/NAAM aliases + detect_column
│   ├── website_tree.py    # Master Files xlsx → boom-structuur
│   ├── learnings.py       # save_pending / apply_learning / list_learnings
│   ├── job_lock.py        # seo_job_locks advisory lock
│   ├── export_schemas.py  # column allow-list per taak
│   ├── script_runner.py   # thread runner + progress callback
│   └── session.py         # typed session-state accessors
├── pages/                 # 22 pages (00_Home, 10_Ingest, …, 90_Learnings)
├── execution/             # frozen-snapshot kopieën van de scripts in ../execution/
│   └── __init__.py
└── README.md              # dit bestand
```

## Pagina-overzicht

### Overview
- **Home** — KPI scorecards + recent import runs

### Pipeline (1→5)
- **1. Ingest** — upload masterdata Excel → seo_products (status=raw)
- **2. Match** — SKU/EAN matching tegen Shopify-index, met twijfelgeval-UI
- **3. Transform** — categoriseer/vertaal/titel/meta · **batch-cap 25 rijen**
- **4. Validate** — quality checks + auto-fixes
- **5. Export** — Hextom bulk Excel (nieuw + archief)

### Post-export
- **Prijzen updaten** — nieuwe prijslijst + giftbox-regel + 3 Excels · **giftbox test-knop**
- **Foto's resizen** — Serax CDN → <5000 px → Supabase Storage
- **Bynder matching** — Bynder exports tegen producten
- **Serax dimensies** — "L 13,8 W 13,8 H 7" → lengte/breedte/hoogte

### Setup & reference
- **Masterdata-mapping** — kolom-mapping detecteren & opslaan per leverancier
- **Categorie-seed** — baseline mapping seeden (upsert/reset)
- **Categorie uitbreiden** — NEW_MAPPINGS toevoegen
- **Website-structuur** — Shopify exports → shopify_index + collections + filters
- **Meta audit loader** — active export → shopify_meta_audit

### Overzichten (read-only)
- **Producten** — rijke filter-sidebar, paginatie per 100, bulk-acties
- **Categorieën** — 3 tabs: Mapping · Website-tree · Orphans
- **Import runs** — history log
- **Shopify index** — geladen snapshot
- **Collections & filters** — website meta
- **Meta audit overzicht** — lengte-flags + filters

### Learning system
- **Learnings** — approval queue: pending / applied / alles. Typ correctie in NL, Claude parset 'm naar rule, chef keurt goed, `seo_products` wordt bijgewerkt. `transform.py` leest applied rules automatisch.

## Contract tussen `pages/` en `execution/`

Elke pipeline-pagina importeert de pure functie uit `dashboard_v2/execution/`:

```python
from execution.ingest       import ingest_masterdata
from execution.match        import match_fase        # met on_conflict callback
from execution.transform    import transform_batch   # accepteert ids= voor batch-cap
from execution.validate     import validate_fase
from execution.export       import export_fase
from execution.export_standaard import export_standaard
from execution.update_prices import run_price_update
from execution.resize_photos import resize_photos
from execution.match_bynder_photos import match_bynder
from execution.fix_serax_dimensions import fix_serax_dimensions
from execution.load_website_structure import load_website_structure
from execution.meta_audit_loader import load_meta_audit
from execution.setup_masterdata import detect_and_store_mapping
from execution.seed_categories import seed_categories
from execution.extend_category_mapping import extend_category_mapping
```

Alle pure functies accepteren optioneel:
- `progress: Callable[[int, int, str], None]` — wordt aangeroepen per rij
- `logger: Callable[[str], None]` — elke regel die geprint zou worden

Elke functie retourneert een kleine dataclass `…Result` met counts + errors.

## Job locks

Lange operaties (match, transform, validate, export, ingest) vragen een lock via
`ui/job_lock.py :: acquire(fase, step)`. Als de lock al vastzit toont de pagina
wie 'm heeft. Stale locks (>30 min oud) worden automatisch vrijgegeven.

## Column allow-list guard

`ui/export_schemas.py` definieert per taak welke kolommen van `seo_products`
geschreven mogen worden. Voorkomt dat nieuwe exports gecureerde data overschrijven.
Pagina's die schrijven moeten `check_allowed(task, columns)` roepen vóór de
update.

## Backup

Een complete werkende snapshot van het oude dashboard staat in
[`backup/dashboard_v1/`](../backup/dashboard_v1/). Als er iets misgaat, val
daarop terug — zie de README daar.
