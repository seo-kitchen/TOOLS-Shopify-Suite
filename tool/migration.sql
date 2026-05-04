-- ============================================================
-- SEOkitchen tool/ — database migraties
-- Draaien in Supabase SQL Editor (eenmalig)
-- ============================================================

-- 1. Audit trail voor Hextom-downloads
--    Elke keer dat een medewerker een Hextom Excel downloadt én bevestigt
--    dat die ook geïmporteerd is, wordt dat hier gelogd.
CREATE TABLE IF NOT EXISTS seo_export_files (
    id             UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id      TEXT         NOT NULL DEFAULT 'interieurshop',
    task_type      TEXT         NOT NULL,       -- 'nieuwe_producten' | 'prijsupdate' | 'collectie_seo'
    fase           TEXT,                        -- alleen relevant bij nieuwe_producten
    file_name      TEXT         NOT NULL,
    row_count      INTEGER      DEFAULT 0,
    generated_at   TIMESTAMPTZ  DEFAULT NOW(),
    generated_by   TEXT         DEFAULT 'chef@seokitchen.nl',
    confirmed_at   TIMESTAMPTZ,                -- NULL = nog niet bevestigd in Hextom
    confirmed_by   TEXT
);

CREATE INDEX IF NOT EXISTS idx_export_files_pending
    ON seo_export_files (client_id, confirmed_at);

CREATE INDEX IF NOT EXISTS idx_export_files_recent
    ON seo_export_files (generated_at DESC);


-- 2. Notitieboekje voor het team
CREATE TABLE IF NOT EXISTS seo_notes (
    id               UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id        TEXT         NOT NULL DEFAULT 'interieurshop',
    tekst            TEXT         NOT NULL,
    label            TEXT         DEFAULT 'overig',   -- foto | meta | categorie | prijs | overig
    aangemaakt_op    TIMESTAMPTZ  DEFAULT NOW(),
    aangemaakt_door  TEXT         DEFAULT 'chef@seokitchen.nl',
    opgelost         BOOLEAN      DEFAULT FALSE,
    opgelost_op      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_notes_client_open
    ON seo_notes (client_id, opgelost, aangemaakt_op DESC);


-- ============================================================
-- 3. client_id toevoegen aan seo_job_locks (uitvoeren VOOR tweede klant)
--    Zolang er maar één klant is, is dit nog niet nodig.
--    Als je een tweede klant toevoegt zonder deze migratie, crashen
--    job-locks omdat beide klanten dezelfde (fase, step) key delen.
--
-- ALTER TABLE seo_job_locks ADD COLUMN IF NOT EXISTS client_id TEXT DEFAULT 'interieurshop';
-- DROP INDEX IF EXISTS seo_job_locks_fase_step_running_idx;      -- naam kan afwijken
-- CREATE UNIQUE INDEX seo_job_locks_client_fase_step_running_idx
--     ON seo_job_locks (client_id, fase, step)
--     WHERE status = 'running';
-- ============================================================
