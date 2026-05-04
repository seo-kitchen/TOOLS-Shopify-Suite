-- ============================================================
-- shopify_meta_sync migratie — extra kolommen voor live Shopify data
-- Eenmalig uitvoeren in Supabase SQL Editor.
-- Bestaande data/kolommen worden NIET gewijzigd.
-- ============================================================

ALTER TABLE shopify_meta_audit
  ADD COLUMN IF NOT EXISTS product_status  TEXT,          -- active | draft | archived
  ADD COLUMN IF NOT EXISTS product_type    TEXT,
  ADD COLUMN IF NOT EXISTS price           NUMERIC(10,2),
  ADD COLUMN IF NOT EXISTS tags            TEXT,
  ADD COLUMN IF NOT EXISTS published_at    TIMESTAMPTZ;

-- Handig voor filteren in het dashboard
CREATE INDEX IF NOT EXISTS idx_meta_audit_product_status
  ON shopify_meta_audit (product_status);

CREATE INDEX IF NOT EXISTS idx_meta_audit_price
  ON shopify_meta_audit (price);
