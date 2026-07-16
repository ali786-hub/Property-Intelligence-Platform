# Engineering Architecture Decisions THoughoutthe Journey:
**Project:** PropIntel Data Pipeline  
**Author:** Ali Zaman (Me)

This document tracks the evolution of the PropIntel architecture, focusing on the bottlenecks I encountered while scaling the system to handle Terabyte-level data, the trade-offs I analyzed, and the engineering solutions I implemented.

---

## 1. The N+1 Network Latency Bottleneck
**The Problem:**
In my initial version (`version1-neon-tracker`), I used Neon DB (a serverless PostgreSQL database) to track file lineage. However, I quickly identified a severe architectural flaw: the pipeline was making a remote network call to the database for *every single file* it processed. While this worked for small datasets, I realized that scaling to Terabytes of data would cause massive network I/O latency (the classic N+1 query problem). The pipeline would spend more time waiting on the internet than crunching data.

**My Solution (Metadata Caching / Bulk Flushing):**
I completely redesigned the tracking logic to eliminate network round-trips. I engineered a "Metadata Caching" pattern where the pipeline queries the database exactly *once* at startup to fetch the current state into RAM. The script then processes the files locally, updates the statuses in a Python list, and performs a single bulk `executemany` flush to the database at the very end of the run. This reduced latency by 99%.

---

## 2. The RAM Constraint (A Side Effect of Caching)
**The Problem:**
My Metadata Caching solution perfectly solved the network latency, but it introduced a new risk. If the pipeline processed 1,000,000 files in a single run, holding all 1,000,000 lineage records in Python's memory before the final database flush would cause RAM usage to spike. This would drastically increase my cloud compute costs and risk Out Of Memory (OOM) crashes.

**My Solution (Bounded Buffer Chunking):**
To fix this, I engineered a memory constraint using a Bounded Buffer pattern. I mathematically calculated the "sweet spot" between RAM overhead and network calls and set a `FLUSH_THRESHOLD` (e.g., 1,000 files). 
The script appends metadata to a local list. The moment the list hits 1,000 items, it triggers a bulk flush to the database and immediately clears the RAM buffer. This constraint ensures my RAM usage remains completely flat and predictable (under a few megabytes) regardless of whether I process a thousand files or ten million.

---

## 3. The Orchestration vs. Lineage Trap (Database Bloat)
**The Problem:**
To scale the pipeline and handle micro-batches (time-based partitioning), I decided to introduce Apache Airflow. Initially, I considered using Airflow's internal Postgres database to store my custom file lineage to keep everything in one place. However, I realized that dumping millions of custom business metadata rows into Airflow's operational database would bloat the scheduler's backend and severely degrade orchestration performance.

**My Solution (Separation of Concerns):**
I enforced a strict separation of concerns. I decided to use Airflow *strictly* for operational orchestration (managing DAGs, retries, and time). I kept Neon DB separate, dedicating it entirely to business metadata (file lineage). 

---

## 4. Iceberg Integration & The Catalog Dilemma
**The Problem:**
To push DuckDB's limits and move to an industry-standard data lake, I decided to swap raw Parquet files for Apache Iceberg. Iceberg requires a "Catalog" (a metadata phonebook) to track table states. Running a full REST Catalog locally via Docker would hog my local machine's RAM. On the other hand, setting up a local SQLite catalog would make migrating to Azure/S3 extremely difficult later.

**My Solution (The JDBC Catalog Pivot):**
Instead of spinning up new infrastructure, I realized I could use a JDBC Catalog, effectively telling Iceberg to use my *existing* Neon DB as its catalog. This was a massive win: I didn't have to bloat my local Docker environment with new databases, and I achieved a clean, cloud-native architecture where a single remote database handled both my custom file lineage and the Iceberg catalog.

---

## 5. Idempotency and Airflow Retries
**The Problem:**
Airflow's greatest strength is automatically retrying failed tasks. However, if an Airflow task crashes 90% of the way through writing to the Silver layer, a blind retry would cause duplicated data in the data lake.

**My Solution:**
I designed the pipeline to be strictly idempotent. Instead of using raw `INSERT` statements when writing to the Iceberg tables, I designed the DuckDB transformations to use `MERGE INTO` (Upserts) based on a composite primary key (`property_id` + `snapshot_date`). This ensures that no matter how many times Airflow retries a task, the resulting data lake state is exactly the same.
