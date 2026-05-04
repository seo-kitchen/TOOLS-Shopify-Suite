-- Table: shopify_meta_audit
-- Purpose: audit + rewrite workflow voor Shopify meta title & meta description
-- Raakt onboarding tabellen (products, etc.) NIET aan.

create table if not exists shopify_meta_audit (
  id                   bigserial primary key,
  shopify_product_id   text not null unique,
  handle               text not null,
  product_title        text,
  vendor               text,

  -- Huidige Shopify state (snapshot van de export)
  current_meta_title       text,
  current_meta_description text,
  current_title_length     int,
  current_desc_length      int,

  -- Audit resultaat (deterministisch, geen Claude)
  title_status        text,  -- ok | missing | too_long | too_short | duplicate
  desc_status         text,  -- ok | missing | too_long | too_short | duplicate

  -- Claude suggestie (leeg tot rewrite draait)
  suggested_meta_title       text,
  suggested_meta_description text,
  suggested_title_length     int,
  suggested_desc_length      int,

  -- Workflow
  review_status       text default 'pending',  -- pending | approved | edited | skipped | pushed
  approved_title      text,
  approved_desc       text,
  approved_at         timestamptz,
  pushed_at           timestamptz,

  notes               text,
  created_at          timestamptz default now(),
  updated_at          timestamptz default now()
);

create index if not exists idx_meta_audit_title_status  on shopify_meta_audit(title_status);
create index if not exists idx_meta_audit_desc_status   on shopify_meta_audit(desc_status);
create index if not exists idx_meta_audit_review_status on shopify_meta_audit(review_status);
create index if not exists idx_meta_audit_vendor        on shopify_meta_audit(vendor);
