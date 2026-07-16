-- ============================================================
-- PropIntel Pipeline — Database Schema (v2.0)
-- ============================================================
-- Target: Azure Database for PostgreSQL (currently Neon serverless)
--
-- PURPOSE:
-- This schema serves a dual role in the PropIntel architecture:
--   1. FILE LINEAGE TRACKING — Audit trail of every file processed
--      through the Medallion Architecture (Landing → Bronze → Silver → Gold).
--   2. APACHE ICEBERG JDBC CATALOG — Neon/Azure Postgres acts as the
--      Iceberg "phonebook", storing pointers to the physical data files
--      in cloud storage (Azure Blob / S3). Iceberg manages its own
--      catalog tables automatically; we only define our custom lineage.
--
-- SETUP:
-- Copy-paste this entire file into the Azure Portal SQL Editor
-- (or the Neon SQL Console) and execute it.
--
-- DESIGN DECISIONS:
--   - The `pipeline_runs` table from v1 has been REMOVED.
--     Apache Airflow now owns run orchestration, status tracking,
--     and retry logic. Storing duplicate run metadata in Postgres
--     would bloat the database and conflict with Airflow's own state.
--     We store only the Airflow run_id as a foreign reference.
--
--   - The `PROCESSING` file status from v1 has been REMOVED.
--     In v1, each file was pre-logged as PROCESSING before work began
--     (a crash-safety mechanism). This caused an N+1 network latency
--     bottleneck (one DB round-trip per file). In v2, we use a
--     RAM-buffered bulk-flush strategy: files are processed in memory,
--     and their results are committed to the database in a single
--     atomic bulk upsert at the end of the batch. If the script crashes
--     mid-run, the PostgreSQL transaction rolls back automatically,
--     and Airflow retries the entire batch cleanly.
--
--   - The file_hash (SHA-256 of the original raw CSV) remains the lineage
--     key across ALL layers. A Silver record with layer='SILVER' still
--     stores the hash of the original CSV that produced it, enabling
--     full source-to-output traceability.
-- ============================================================


-- ============================================================
-- 1. ENUM TYPES
-- ============================================================

-- pipeline_layer: Valid layers in the Medallion Architecture.
CREATE TYPE pipeline_layer AS ENUM ('BRONZE', 'SILVER', 'GOLD');

-- file_status: Valid terminal states for a processed file.
--   SUCCESS:      File processed cleanly with row count confirmed.
--   FAILED:       File failed processing. Will be retried on next
--                 Airflow run until retry_count hits the threshold.
--   QUARANTINED:  File has exceeded the maximum retry limit.
--                 Permanently skipped. Requires human inspection.
CREATE TYPE file_status AS ENUM ('SUCCESS', 'FAILED', 'QUARANTINED');


-- ============================================================
-- 2. FILE LINEAGE TABLE (The Core Audit Log)
-- ============================================================
-- One row per file per layer. This is the single source of truth
-- for "what data has been processed, where, and by whom."
--
-- PRIMARY KEY: (file_hash, layer)
--   Each unique source CSV gets exactly one lineage entry per layer.
--   The ON CONFLICT upsert clause in our Python bulk-flush ensures
--   that Airflow retries never create duplicate records (idempotency).

CREATE TABLE file_lineage (
    -- Identity --
    file_hash           VARCHAR(64)     NOT NULL,       -- SHA-256 hash of the original raw CSV
    layer               pipeline_layer  NOT NULL,       -- Which Medallion layer this record represents

    -- Source Info --
    file_name           VARCHAR(255)    NOT NULL,       -- Output filename produced in this layer
    airflow_run_id      VARCHAR(250),                   -- Airflow DAG run ID (e.g., 'scheduled__2026-07-16T00:00:00+00:00')
                                                        -- Nullable to allow manual/standalone script execution during development

    -- Status Tracking --
    status              file_status     NOT NULL,       -- Terminal state: SUCCESS, FAILED, or QUARANTINED
    retry_count         INTEGER         NOT NULL DEFAULT 0,  -- Number of processing attempts. Incremented on each FAILED attempt.

    -- Output Metrics (populated on SUCCESS) --
    row_count           INTEGER,                        -- Number of rows in the output file
    file_size_bytes     BIGINT,                         -- Size of the output file in bytes

    -- Error Tracking --
    error_message       TEXT,                           -- Last error message if status = FAILED

    -- Timestamps --
    created_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Constraints --
    CONSTRAINT file_lineage_pk PRIMARY KEY (file_hash, layer)
);


-- ============================================================
-- 3. INDEXES (Optimized for the Bulk-Flush Query Pattern)
-- ============================================================

-- Used at script STARTUP: "Give me all hashes that succeeded in BRONZE"
-- This is the ONE query we run to populate the in-memory cache.
CREATE INDEX idx_lineage_layer_status
    ON file_lineage (layer, status);

-- Used for monitoring dashboards and Airflow callbacks:
-- "Show me everything from today's run"
CREATE INDEX idx_lineage_airflow_run
    ON file_lineage (airflow_run_id);

-- Used for quarantine monitoring:
-- "Which files have been retried too many times?"
CREATE INDEX idx_lineage_retry_count
    ON file_lineage (retry_count)
    WHERE status = 'FAILED';


-- ============================================================
-- 4. HELPER FUNCTION: Auto-update `updated_at` on every upsert
-- ============================================================
-- This trigger automatically refreshes the `updated_at` timestamp
-- whenever a row is inserted or updated via our bulk flush,
-- so we never have to manage timestamps in Python code.

CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_lineage_updated_at
    BEFORE UPDATE ON file_lineage
    FOR EACH ROW
    EXECUTE FUNCTION update_timestamp();
