-- ============================================================================
-- SEOkitchen unified dashboard — schema v2
-- Run this in Supabase SQL editor (Project → SQL Editor → New query → paste → Run)
-- Safe to run twice: uses IF NOT EXISTS everywhere.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1) seo_learnings: replaces config/learnings.json
--    Chef's corrections flow in here as 'pending', get approved, then 'applied'.
--    transform.py reads status='applied' rows each run and applies them on top
--    of its hardcoded base rules.
-- ----------------------------------------------------------------------------
create table if not exists seo_learnings (
  id              bigserial primary key,
  created_at      timestamptz not null default now(),
  stap            text        not null,    -- 'categorie' | 'titel' | 'vertaling' | 'meta'
  scope           text        not null default 'global',
                                            -- 'global' | 'leverancier:serax' | 'fase:3'
  rule_type       text        not null,    -- 'name_rule' | 'title_rule'
                                            -- | 'category_mapping' | 'translation'
                                            -- | 'name_rule_bulk' | 'unclear'
  input_text      text,                     -- chef's original free-text correction
  action          jsonb       not null,     -- structured rule (was learnings.json 'actie')
  raw_response    text,                     -- Claude's raw JSON, for debugging
  status          text        not null default 'pending',
                                            -- 'pending' | 'approved' | 'applied'
                                            -- | 'rejected' | 'superseded'
  applied_at      timestamptz,
  applied_by      text,                     -- user email
  applied_rows    int,                      -- how many seo_products were affected
  example_before  text,
  example_after   text,
  superseded_by   bigint references seo_learnings(id),
  notes           text
);

create index if not exists seo_learnings_stap_status_idx
  on seo_learnings (stap, status);
create index if not exists seo_learnings_rule_type_status_idx
  on seo_learnings (rule_type, status);
create index if not exists seo_learnings_created_at_idx
  on seo_learnings (created_at desc);

comment on table  seo_learnings            is
  'Chef corrections for the transform pipeline. Replaces config/learnings.json. transform.py reads status=applied rows to extend its base rules.';
comment on column seo_learnings.stap       is
  'Pipeline step this rule affects: categorie | titel | vertaling | meta';
comment on column seo_learnings.rule_type  is
  'Shape of the action payload: name_rule | title_rule | category_mapping | translation | name_rule_bulk | unclear';
comment on column seo_learnings.status     is
  'pending (awaiting approval) | approved (reviewed) | applied (active in transforms) | rejected | superseded';
comment on column seo_learnings.action     is
  'Structured rule body. Shape depends on rule_type.';


-- ----------------------------------------------------------------------------
-- 2) seo_job_locks: prevents 2 users running the same step on the same phase.
--    Dashboard takes a lock before running match/transform/validate/export,
--    releases on success or failure. Stale locks (>30 min) can be force-released.
-- ----------------------------------------------------------------------------
create table if not exists seo_job_locks (
  id              bigserial primary key,
  fase            text        not null,
  step            text        not null,    -- 'ingest'|'match'|'transform'|'validate'|'export'|...
  started_at      timestamptz not null default now(),
  started_by      text        not null,    -- user email
  heartbeat_at    timestamptz not null default now(),
  status          text        not null default 'running',  -- 'running' | 'released' | 'failed'
  released_at     timestamptz,
  details         jsonb
);

-- one active lock per (fase, step) at a time
create unique index if not exists seo_job_locks_active_uq
  on seo_job_locks (fase, step)
  where status = 'running';

create index if not exists seo_job_locks_started_at_idx
  on seo_job_locks (started_at desc);

comment on table seo_job_locks is
  'Advisory lock for long-running pipeline steps. Unique active lock per (fase,step). Dashboard refuses to start a step if a running lock exists.';


-- ============================================================================
-- Verify installation
-- ============================================================================
-- After running this, run these two SELECTs; both should return 0 rows (empty
-- tables) but no errors:
--
-- select count(*) from seo_learnings;
-- select count(*) from seo_job_locks;
--
-- Also confirm indexes:
-- select indexname from pg_indexes where tablename in ('seo_learnings','seo_job_locks');
