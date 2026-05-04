-- ============================================================
-- Foto + productomschrijving kolommen toevoegen aan shopify_meta_audit
-- Eenmalig uitvoeren in Supabase SQL Editor.
-- ============================================================

ALTER TABLE shopify_meta_audit
  ADD COLUMN IF NOT EXISTS has_image           BOOLEAN,
  ADD COLUMN IF NOT EXISTS image_count         INTEGER  DEFAULT 0,
  ADD COLUMN IF NOT EXISTS first_image_src     TEXT,
  ADD COLUMN IF NOT EXISTS first_image_alt     TEXT,
  ADD COLUMN IF NOT EXISTS image_alt_status    TEXT,   -- ok | missing
  ADD COLUMN IF NOT EXISTS image_name_status   TEXT,   -- seofriendly | supplier | unknown
  ADD COLUMN IF NOT EXISTS has_description     BOOLEAN,
  ADD COLUMN IF NOT EXISTS description_length  INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS sku                 TEXT;   -- eerste variant SKU

CREATE INDEX IF NOT EXISTS idx_meta_audit_has_image
  ON shopify_meta_audit (has_image);

CREATE INDEX IF NOT EXISTS idx_meta_audit_image_alt_status
  ON shopify_meta_audit (image_alt_status);

CREATE INDEX IF NOT EXISTS idx_meta_audit_image_name_status
  ON shopify_meta_audit (image_name_status);

CREATE INDEX IF NOT EXISTS idx_meta_audit_has_description
  ON shopify_meta_audit (has_description);
