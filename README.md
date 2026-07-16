# PropIntel — Property Intelligence ETL Platform

A production-grade data engineering project that builds a complete **Medallion Architecture** (Landing → Bronze → Silver → Gold) for processing Pakistani real estate market data at scale. The pipeline is orchestrated by **Apache Airflow**, transforms data through progressively cleaner layers using **Polars** and **DuckDB**, and outputs analytical datasets as **Apache Iceberg** tables backed by a **JDBC Catalog** on PostgreSQL.

> **📖 Engineering Decisions**: See [`problems_solved.md`](problems_solved.md) for a detailed walkthrough of the architectural bottlenecks encountered during development and the engineering solutions implemented to solve them.

---

## Architecture

```
                                ┌──────────────────────────────┐
                                │   PostgreSQL (Neon / Azure)  │
                                │                              │
                                │  ┌────────────────────────┐  │
                                │  │    file_lineage        │  │  ← Custom audit log
                                │  │   (Bulk-Flushed)       │  │    (RAM-buffered writes)
                                │  └────────────────────────┘  │
                                │  ┌────────────────────────┐  │
                                │  │  Iceberg JDBC Catalog  │  │  ← Table metadata phonebook
                                │  │  (Auto-managed)        │  │    (managed by Iceberg)
                                │  └────────────────────────┘  │
                                └───────────────┬──────────────┘
                                                │
  ┌─────────────┐                               │
  │   Apache    │── Orchestrates ──────────────▶ │
  │   Airflow   │   (DAGs, retries,             │
  │             │    micro-batching)             │
  └─────────────┘                               │
                                                │
  ┌────────────┐    ┌──────────┐    ┌──────────┐    ┌──────────────┐
  │  Landing   │───▶│  Bronze  │───▶│  Silver  │───▶│    Gold      │
  │  Zone      │    │  Layer   │    │  Layer   │    │   (Iceberg)  │
  │ (Raw CSV)  │    │(Parquet) │    │(Cleaned) │    │ (Aggregated) │
  └────────────┘    └──────────┘    └──────────┘    └──────────────┘
       │               Polars          DuckDB          DuckDB +
       ▼                                               Iceberg
  ┌────────────┐
  │  Archive   │
  │  Zone      │◀── After Bronze success
  │(Processed) │
  └────────────┘
```

### Data Flow

| Layer | Format | Engine | Purpose |
|-------|--------|--------|---------|
| **Landing** | Raw CSV (~50 MB each) | — | Incoming data drop zone |
| **Bronze** | Parquet | Polars (streaming `sink_parquet`) | 1-to-1 format conversion, schema preservation |
| **Silver** | Parquet (cleaned) | DuckDB | Type casting, geo-fencing, unit normalization, price capping, SCD2 columns |
| **Gold** | Apache Iceberg | DuckDB + Iceberg | City-level aggregations, price trend analytics |

### Pipeline State Tracking

File lineage is tracked via a **single PostgreSQL database** (currently Neon, migrating to Azure PostgreSQL):

- **`file_lineage`** — Tracks individual files across every Medallion layer using the MD5 hash of the original CSV as the lineage key. Stores status, row counts, file sizes, error messages, and retry counts.

#### Performance Engineering

The original pipeline suffered from **N+1 network latency** — every file triggered a separate database round-trip. This was solved using a **RAM-Buffered Bulk-Flush** strategy:

1. **Startup**: Query the database **once** to load all processed hashes into an in-memory set.
2. **Processing**: Execute all ETL work locally. Append metadata results to a bounded in-memory buffer.
3. **Flush**: When the buffer hits a configurable threshold (default: 1,000 files), perform a single bulk `INSERT ... ON CONFLICT DO UPDATE` to the database and clear the buffer.
4. **Final Flush**: At script completion, flush any remaining buffered records.

This reduces database round-trips by **99.9%** while keeping RAM usage flat and predictable regardless of dataset size.

---

## Data Source

- **Origin**: Pakistani real estate listings (Zameen.com)
- **Format**: Daily snapshot CSVs with ~150,000+ rows each
- **Columns**: `property_id`, `location_id`, `page_url`, `property_type`, `price`, `location`, `city`, `province_name`, `latitude`, `longitude`, `baths`, `bedrooms`, `purpose`, `date_added`, `agency`, `agent`, `Area Type`, `Area Size`, `snapshot_date`
- **Scale**: 122 files × ~50 MB = ~6 GB of raw data (architecture designed for Terabyte scale)

---

## Project Structure

```
PropI/
├── dags/                               # Airflow DAG definitions
│   └── propintel_daily_etl.py          # Main orchestration DAG
│
├── src/                                # Core pipeline code
│   ├── ingestion/
│   │   └── bronze_ingest.py            # Landing → Bronze (Polars)
│   │
│   ├── transformation/
│   │   └── silver_transform.py         # Bronze → Silver (DuckDB)
│   │
│   ├── loading/
│   │   └── gold_publish.py             # Silver → Gold (Iceberg)
│   │
│   └── utils/
│       ├── database.py                 # PostgreSQL connection management
│       └── lineage.py                  # RAM-buffered bulk-flush tracker
│
├── schema.sql                          # Database schema (run on Azure/Neon)
├── problems_solved.md                  # Engineering decisions log
├── requirements.txt
├── .env.example
├── .gitignore
├── .dockerignore
└── README.md

data/                                   # Data Lake (gitignored)
├── landing_zone/                       # Raw CSVs awaiting processing
├── archive_zone/                       # Processed CSVs (moved after Bronze)
├── bronze/                             # Parquet files (1-to-1 from CSV)
├── silver/                             # Cleaned Parquet files
└── gold/                               # Iceberg tables
```

---

## Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Configure database credentials
cp .env.example .env   # Fill in your PostgreSQL connection string

# Initialize the database schema
# Copy the contents of schema.sql into your Azure/Neon SQL editor and execute
```

---

## Tech Stack

| Tool | Role |
|------|------|
| **Python** | Pipeline scripting |
| **Polars** | High-speed streaming CSV → Parquet conversion (Bronze) |
| **DuckDB** | Vectorized SQL engine for data transformations (Silver & Gold) |
| **Apache Iceberg** | Open table format with ACID transactions and time-travel (Gold) |
| **Apache Airflow** | DAG-based workflow orchestration, scheduling, and retries |
| **PostgreSQL** | File lineage tracking + Iceberg JDBC Catalog (Neon → Azure) |

---

## Status

- [x] Landing → Bronze ETL (Polars)
- [x] Bronze → Silver ETL (DuckDB transformations)
- [x] Database schema v2 (optimized for bulk-flush)
- [ ] RAM-buffered lineage tracker (`src/utils/lineage.py`)
- [ ] Airflow DAG orchestration
- [ ] Gold Layer (Apache Iceberg integration)
- [ ] Cloud migration (Azure Blob + Azure PostgreSQL)
