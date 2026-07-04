-- ============================================================
-- PropIntel Pipeline State Tracker — Neon DB Schema
-- ============================================================
-- Database: Prop_intel_tracker (hosted on Neon serverless Postgres)
--
-- Purpose: Track every file through every layer of the Medallion
-- Architecture. Provides full audit trail: which files were processed,
-- when, by which run, how many rows, and what errors occurred.
--
-- To set up from scratch: Copy-paste this entire file into the
-- Neon SQL Editor (https://console.neon.tech) and run it.
-- ============================================================


-- 1. ENUM TYPES
-- Using Postgres enums instead of VARCHAR constraints gives us:
--   - Type safety (can't insert typos like 'BRONZEE')
--   - Self-documenting (the valid values ARE the schema)
--   - Slightly faster comparisons than string matching

CREATE TYPE pipeline_layer AS ENUM ('BRONZE', 'SILVER', 'GOLD');
CREATE TYPE task_status AS ENUM ('RUNNING', 'PROCESSING', 'SUCCESS', 'FAILED', 'PARTIAL_SUCCESS');


-- 2. PIPELINE RUNS TABLE
-- One row per ETL execution. When you run move_to_bronze.py,
-- it creates a run here with status 'RUNNING', then updates
-- to 'SUCCESS' or 'FAILED' when it finishes.

CREATE TABLE pipeline_runs (
    run_id      SERIAL PRIMARY KEY,
    layer_name  pipeline_layer NOT NULL,        -- Which layer this run processed
    status      task_status NOT NULL,            -- Current status of this run
    started_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ended_at    TIMESTAMP WITH TIME ZONE,
    
    -- Summary stats (updated when the run finishes)
    files_processed  INTEGER DEFAULT 0,          -- How many files succeeded
    files_failed     INTEGER DEFAULT 0           -- How many files failed
);


-- 3. FILE LINEAGE TABLE
-- One row per file per layer. This is the core audit log.
-- The composite primary key (file_hash, layer) means each unique
-- file gets exactly one entry per layer it passes through.
--
-- The file_hash is the MD5 of the ORIGINAL CSV file, and it stays
-- the same as the file moves through Bronze, Silver, Gold. This is
-- how we track a file's journey across layers.

CREATE TABLE file_lineage (
    file_hash       VARCHAR(64) NOT NULL,
    layer           pipeline_layer NOT NULL,
    file_name       VARCHAR(255) NOT NULL,       -- Name of the output file in this layer
    run_id          INTEGER NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    status          task_status NOT NULL,
    row_count       INTEGER,                     -- Number of rows in the output file
    file_size_bytes BIGINT,                      -- Size of the output file in bytes
    error_message   TEXT,                        -- Error details if status = 'FAILED'
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT file_lineage_pk PRIMARY KEY (file_hash, layer)
);

-- Index for fast lookups by status (used by Silver to find eligible Bronze files)
CREATE INDEX idx_file_lineage_status ON file_lineage (status);

-- Index for fast lookups by layer + status combo (most common query pattern)
CREATE INDEX idx_file_lineage_layer_status ON file_lineage (layer, status);
