-- ============================================================
-- shopify_meta_audit — volledig setup script
-- Plak dit in Supabase SQL Editor en voer het uit.
-- Idempotent: veilig om meerdere keren te draaien.
-- ============================================================

-- 1. Basistabel aanmaken
CREATE TABLE IF NOT EXISTS shopify_meta_audit (
  id                   BIGSERIAL    PRIMARY KEY,
  shopify_product_id   TEXT         NOT NULL UNIQUE,
  handle               TEXT         NOT NULL,
  product_title        TEXT,
  vendor               TEXT,

  -- Live Shopify velden (gevuld door shopify_meta_sync.py)
  product_type         TEXT,
  product_status       TEXT,        -- active | draft | archived
  price                NUMERIC(10,2),
  tags                 TEXT,
  published_at         TIMESTAMPTZ,

  -- Huidige SEO state (snapshot)
  current_meta_title       TEXT,
  current_meta_description TEXT,
  current_title_length     INT,
  current_desc_length      INT,

  -- Audit resultaat (deterministisch)
  title_status         TEXT,        -- ok | missing | too_long | too_short | duplicate | templated
  desc_status          TEXT,        -- ok | missing | too_long | too_short | duplicate | templated

  -- Claude suggestie
  suggested_meta_title       TEXT,
  suggested_meta_description TEXT,
  suggested_title_length     INT,
  suggested_desc_length      INT,

  -- Workflow
  review_status        TEXT         DEFAULT 'pending',
  approved_title       TEXT,
  approved_desc        TEXT,
  approved_at          TIMESTAMPTZ,
  pushed_at            TIMESTAMPTZ,

  notes                TEXT,
  created_at           TIMESTAMPTZ  DEFAULT NOW(),
  updated_at           TIMESTAMPTZ  DEFAULT NOW()
);

-- 2. Extra kolommen toevoegen als de tabel al bestond zonder ze
ALTER TABLE shopify_meta_audit
  ADD COLUMN IF NOT EXISTS product_type    TEXT,
  ADD COLUMN IF NOT EXISTS product_status  TEXT,
  ADD COLUMN IF NOT EXISTS price           NUMERIC(10,2),
  ADD COLUMN IF NOT EXISTS tags            TEXT,
  ADD COLUMN IF NOT EXISTS published_at    TIMESTAMPTZ;

-- 3. Indexen
CREATE INDEX IF NOT EXISTS idx_meta_audit_title_status    ON shopify_meta_audit (title_status);
CREATE INDEX IF NOT EXISTS idx_meta_audit_desc_status     ON shopify_meta_audit (desc_status);
CREATE INDEX IF NOT EXISTS idx_meta_audit_review_status   ON shopify_meta_audit (review_status);
CREATE INDEX IF NOT EXISTS idx_meta_audit_vendor          ON shopify_meta_audit (vendor);
CREATE INDEX IF NOT EXISTS idx_meta_audit_product_status  ON shopify_meta_audit (product_status);
