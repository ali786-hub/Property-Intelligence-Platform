# PropIntel — Property Intelligence ETL Platform

A data engineering project that builds a complete **Medallion Architecture** (Landing → Bronze → Silver → Gold) for processing Pakistani real estate market data. The pipeline ingests daily property snapshots, transforms them through progressively cleaner layers, and outputs analytical datasets via **Apache Iceberg** tables.

---

## Architecture

```
                        ┌──────────────────────────┐
                        │     Neon DB (Postgres)    │
                        │   Pipeline State Tracker  │
                        │  ┌────────────────────┐   │
                        │  │   pipeline_runs    │   │
                        │  │   file_lineage     │   │
                        │  └────────────────────┘   │
                        └──────────┬───────────────┘
                                   │ Lineage tracking
                                   │
  ┌────────────┐    ┌──────────┐   │   ┌──────────┐    ┌──────────────┐
  │  Landing   │───▶│  Bronze  │───┼──▶│  Silver  │───▶│    Gold      │
  │  Zone      │    │  Layer   │   │   │  Layer   │    │   (Iceberg)  │
  │ (Raw CSV)  │    │(Parquet) │   │   │(Cleaned) │    │ (Aggregated) │
  └────────────┘    └──────────┘   │   └──────────┘    └──────────────┘
       │                           │
       ▼                           │
  ┌────────────┐                   │
  │  Archive   │                   │
  │  Zone      │◀──────────────────┘
  │(Processed) │    After Bronze success
  └────────────┘
```

### Data Flow

| Layer | Format | Engine | Purpose |
|-------|--------|--------|---------|
| **Landing** | Raw CSV (~50 MB each) | — | Incoming data drop zone |
| **Bronze** | Parquet | Polars | 1-to-1 format conversion, schema preservation |
| **Silver** | Parquet (cleaned) | DuckDB | Type casting, geo-fencing, unit normalization, price capping, SCD2 columns |
| **Gold** | Apache Iceberg | PyIceberg | City-level aggregations, price trend analytics *(in progress)* |

### Pipeline State Tracking

Every file is tracked through every layer via **Neon DB** (serverless Postgres):
- **`pipeline_runs`** — records each ETL execution (run ID, status, timestamps)
- **`file_lineage`** — tracks individual files (MD5 hash, layer, status, row count, errors)

This provides full auditability: you can query Neon to see exactly which files have been processed, in which layer, and whether they succeeded or failed.

---

## Data Source

- **Origin**: Pakistani real estate listings (Zameen.com)
- **Format**: Daily snapshot CSVs with ~150,000+ rows each
- **Columns**: `property_id`, `location_id`, `page_url`, `property_type`, `price`, `location`, `city`, `province_name`, `latitude`, `longitude`, `baths`, `bedrooms`, `purpose`, `date_added`, `agency`, `agent`, `Area Type`, `Area Size`, `snapshot_date`
- **Scale**: 122 files × ~50 MB = ~6 GB of raw data

---

## Project Structure

```
PropI/                              # Git Repository
├── Prop_intel_etl/
│   ├── move_to_bronze.py           # Landing → Bronze ETL (Polars)
│   ├── move_to_silver.py           # Bronze → Silver ETL (DuckDB)
│   ├── rollback.py                 # Pipeline rollback/cleanup utility
│   ├── propi_backend.py            # FastAPI backend (WIP)
│   └── .env                        # Database credentials (git-ignored)
├── User_Interface/            # Frontend dashboard (WIP)
├── .gitignore
├── .dockerignore
├── .env.example
├── requirements.txt
└── README.md

data/                               # Data Lake (outside git)
├── landing_zone/                   # Raw CSVs awaiting processing
├── archive_zone/                   # Processed CSVs (moved after Bronze)
├── bronze/                         # Parquet files (1-to-1 from CSV)
├── silver/                         # Cleaned Parquet files
└── gold/                           # Iceberg tables (coming soon)
```

---

## Usage

### Setup
```bash
pip install -r requirements.txt
cp .env.example Prop_intel_etl/.env   # Fill in your Neon DB credentials
```

### Running the Pipeline
```bash
# Process all files through Bronze
python Prop_intel_etl/move_to_bronze.py

# Process only 3 files through Bronze
python Prop_intel_etl/move_to_bronze.py --batch 3

# Transform all eligible Bronze files to Silver
python Prop_intel_etl/move_to_silver.py

# Rollback Silver layer (delete Silver files + Neon lineage)
python Prop_intel_etl/rollback.py --layer silver

# Full factory reset (wipe all layers + restore CSVs to landing)
python Prop_intel_etl/rollback.py --layer all --restore
```

---

## Tech Stack

| Tool | Role |
|------|------|
| **Python** | Pipeline scripting |
| **Polars** | High-speed CSV → Parquet conversion (Bronze) |
| **DuckDB** | In-memory SQL engine for data transformations (Silver) |
| **Apache Iceberg** | Open table format for Gold layer *(planned)* |
| **Neon DB** | Serverless Postgres for pipeline state tracking |
| **FastAPI** | REST API backend *(planned)* |
| **Docker** | Containerization *(planned)* |

---

## Status

- [x] Landing → Bronze ETL (Polars + Neon lineage)
- [x] Bronze → Silver ETL (DuckDB transformations + Neon lineage)
- [x] Pipeline rollback utility
- [ ] Gold Layer (Apache Iceberg)
- [ ] Backend API
- [ ] Frontend Dashboard
- [ ] Cloud deployment & automation
