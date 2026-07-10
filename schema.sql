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
--
-- NOTE ON LINEAGE KEY DESIGN:
-- The file_hash (MD5 of original CSV) is used as the lineage key
-- across ALL layers. This means a Silver row with layer='SILVER'
-- still stores the hash of the original CSV that produced it.
-- This is intentional — it allows tracing any processed artifact
-- back to its original source file.
-- ============================================================


-- 1. ENUM TYPES

-- pipeline_layer: Valid layers in the Medallion Architecture
-- GOLD is available but the Gold layer (Apache Iceberg) is built in Phase 2.
CREATE TYPE pipeline_layer AS ENUM ('BRONZE', 'SILVER', 'GOLD');

-- run_status: All valid states a pipeline run can be in.
-- RUNNING:          A pipeline_run is actively executing.
-- SUCCESS:          The run completed and all files in the batch succeeded.
-- PARTIAL_SUCCESS:  The run completed, but at least one file in the batch failed.
-- FAILED:           The run failed (e.g., due to an unhandled script crash).
CREATE TYPE run_status AS ENUM (
    'RUNNING',
    'SUCCESS',
    'PARTIAL_SUCCESS',
    'FAILED'
);

-- file_status: All valid states an individual file can be in during a run.
-- PENDING:          Reserved for future use.
-- PROCESSING:       Pre-logged to file_lineage before ETL work begins (crash-safe).
--                   If the script crashes mid-file, this record stays PROCESSING,
--                   and the next run picks it up and retries it.
-- SUCCESS:          File processed cleanly with row count confirmed.
-- FAILED:           File failed processing. retry_count incremented. Will be
--                   retried on next run until retry_count reaches MAX_RETRIES.
-- QUARANTINED:      File has failed MAX_RETRIES times. Skipped automatically.
--                   Requires human inspection of the source file.
CREATE TYPE file_status AS ENUM (
    'PENDING',
    'PROCESSING',
    'SUCCESS',
    'FAILED',
    'QUARANTINED'
);


-- 2. PIPELINE RUNS TABLE
-- One row per ETL script execution. Tracks the overall health of each run.
-- When move_to_bronze.py runs, it opens a run, does work, then closes it.

CREATE TABLE pipeline_runs (
    run_id          SERIAL PRIMARY KEY,
    layer_name      pipeline_layer NOT NULL,        -- Which layer this run processed
    status          run_status NOT NULL,            -- Overall status of this run
    started_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ended_at        TIMESTAMP WITH TIME ZONE,

    -- Summary counters (updated atomically when run finishes)
    files_processed INTEGER NOT NULL DEFAULT 0,     -- Count of files that hit SUCCESS
    files_failed    INTEGER NOT NULL DEFAULT 0      -- Count of files that hit FAILED
);

-- Index for querying recent runs by layer
CREATE INDEX idx_pipeline_runs_layer ON pipeline_runs (layer_name, started_at DESC);


-- 3. FILE LINEAGE TABLE
-- The core audit log. One row per file per layer.
--
-- Key decisions:
-- - Primary key is (file_hash, layer): each unique source CSV gets exactly
--   one lineage entry per layer it passes through.
-- - file_hash is the MD5 of the ORIGINAL CSV, constant across all layers.
-- - retry_count tracks how many times a file has been attempted.
--   When retry_count >= MAX_RETRIES (defined in pipeline_tracker.py),
--   the file is set to QUARANTINED and ignored by future runs.

CREATE TABLE file_lineage (
    -- Identity
    file_hash           VARCHAR(64) NOT NULL,
    layer               pipeline_layer NOT NULL,

    -- Source info
    file_name           VARCHAR(255) NOT NULL,       -- Output filename in this layer
    run_id              INTEGER NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,

    -- Status tracking
    status              file_status NOT NULL,
    retry_count         INTEGER NOT NULL DEFAULT 0,  -- Times this file has been attempted

    -- Output metrics (populated after successful processing)
    row_count           INTEGER,                     -- Rows in the output file
    file_size_bytes     BIGINT,                      -- Size of output file in bytes

    -- Error tracking
    error_message       TEXT,                        -- Last error if status = FAILED

    -- Timestamps
    created_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT file_lineage_pk PRIMARY KEY (file_hash, layer)
);

-- Index for fast status lookups (used by pipeline_tracker.py batch eligibility checks)
CREATE INDEX idx_file_lineage_status ON file_lineage (status);

-- Index for the most common query pattern: layer + status combo
-- e.g., "Give me all BRONZE SUCCESS files not yet in SILVER"
CREATE INDEX idx_file_lineage_layer_status ON file_lineage (layer, status);
