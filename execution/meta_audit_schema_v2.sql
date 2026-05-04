-- ============================================================================
-- Migratie v2: voeg export_batch toe voor batch-tracking
-- Run in Supabase SQL editor.
-- Veilig om 2x te runnen (IF NOT EXISTS).
-- ============================================================================

alter table shopify_meta_audit
  add column if not exists export_batch text;

create index if not exists idx_meta_audit_export_batch
  on shopify_meta_audit(export_batch);

comment on column shopify_meta_audit.export_batch is
  'Naam van de export-batch waarin dit product is meegenomen (bv. ''2026-04-21'' of ''batch_april_salt_pepper''). Null = nog niet geëxporteerd.';

-- Backfill: producten die we al geëxporteerd hebben krijgen batch '2026-04-20-legacy'
update shopify_meta_audit
set export_batch = '2026-04-20-legacy'
where review_status in ('exported', 'pushed')
  and export_batch is null;
