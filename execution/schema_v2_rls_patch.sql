-- ============================================================================
-- SEOkitchen unified dashboard — RLS patch voor schema v2
-- Run this in Supabase SQL editor nadat schema_v2_dashboard.sql gedraaid is.
-- ============================================================================
--
-- Probleem: de standaard Supabase-client (met anon key) wordt tegengehouden
-- door Row-Level Security op de nieuwe tabellen. De andere seo_* tabellen
-- hebben RLS uit staan of een permissive policy, daarom werken die al.
--
-- Simpelste en veiligste oplossing voor een intern dashboard:
-- zet RLS uit op deze twee tabellen, net als bij de rest.
--
-- Als jij liever RLS aan houdt: vervang dit door expliciete policies. Zie
-- onderaan voor een kant-en-klare variant met policies i.p.v. disable.

alter table seo_learnings  disable row level security;
alter table seo_job_locks  disable row level security;

-- ============================================================================
-- Verify
-- ============================================================================
-- select tablename, rowsecurity
-- from pg_tables
-- where schemaname = 'public' and tablename in ('seo_learnings','seo_job_locks');
-- Beide moeten rowsecurity=false tonen.


-- ============================================================================
-- ALTERNATIEF: policies in plaats van RLS uit (draai NIET als je hierboven
-- al RLS hebt uitgezet — kies één van de twee)
-- ============================================================================
-- alter table seo_learnings enable row level security;
-- alter table seo_job_locks  enable row level security;
--
-- create policy "anyone_all_learnings" on seo_learnings
--   for all using (true) with check (true);
-- create policy "anyone_all_job_locks" on seo_job_locks
--   for all using (true) with check (true);
