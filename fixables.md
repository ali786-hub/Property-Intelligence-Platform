# 🛠️ Pipeline Anomalies & Future Fixes (Fixables)

This document records architectural edge cases, anomalies detected during pipeline execution, and their technical solutions.

---

## 1. The Bronze Layer `010.jpg.parquet` Anomaly

### The Issue
During inspection of the Azure Data Lake Storage Gen2 container structures, the Bronze container was found to contain entries like `010.jpg.parquet`, alongside normal structured Parquet files (e.g. `property_data_*.parquet`). Additionally, some filenames appeared to be duplicate instances.

### Root Cause Analysis
1. **Source File Pattern Match**:
   In `src/ingestion/bronze_ingest.py`, the file scanner searches using `*.csv`:
   ```python
   search_pattern = f"{LANDING_ZONE}/*.csv"
   csv_files = landing_fs.glob(search_pattern)
   ```
   A file named strictly `010.jpg` would **never** match this pattern. However, if a file named `010.jpg.csv` entered the landing zone (either due to a Google Colab notebook syncing non-data files from Google Drive or manual testing), it would match the search pattern and get processed.
2. **Blind String Replacement**:
   The output parquet filename is calculated as:
   ```python
   parquet_name = file_name.replace(".csv", ".parquet")
   ```
   If the input is `010.jpg.csv`, this evaluates directly to `010.jpg.parquet`, causing the anomalous file to be written to the Bronze layer.

---

## 2. Silent Overwrite Vulnerability in Bronze Storage

### The Hazard
Currently, files are matched in the Postgres `file_lineage` table using their SHA-256 hash to ensure idempotency. If a file with the same hash is processed again, it is skipped.

However, the physical file destination in Azure Storage is determined **strictly by name**:
```python
output_path = f"{BRONZE_ZONE}/{parquet_name}"
```

This leads to a silent data-loss vulnerability:
1. **File A** (`property_data_1.csv`) is ingested. Hash: `AAA`. It is processed and written to `property_data_1.parquet`.
2. Later, **File B** is uploaded with the **exact same name** `property_data_1.csv` but new data. Hash: `BBB`.
3. The pipeline checks if Hash `BBB` has been processed. Since it hasn't, the pipeline processes File B.
4. File B is written to the exact same location: `property_data_1.parquet`, **silently overwriting** File A's raw content.
5. In PostgreSQL, two lineage entries are marked as `SUCCESS` (one for Hash `AAA`, one for `BBB`), but physical access to Content A in the Bronze layer is lost.

*Note: This does not affect our daily snapshot runs (since files contain unique date suffix names like `property_data_2025-03-01.csv`), but it is a major bug for general file ingestion.*

---

## 💡 Technical Resolution (Fix)

To prevent both anomalies (processing files with double extensions and duplicate filename overwriting), we can modify `src/ingestion/bronze_ingest.py` to:
1. Validate that the filename has a valid `.csv` suffix (not a double extension).
2. Append the unique SHA-256 hash prefix to the Bronze parquet file name to prevent naming collisions.

```python
# Proposed implementation in process_file_task:
clean_base_name = file_name.rsplit('.', 1)[0]
# E.g., 'property_data_2025-03-01'

# Append first 8 characters of hash to make it globally unique in storage
parquet_name = f"{clean_base_name}_{file_hash[:8]}.parquet"
output_path = f"{BRONZE_ZONE}/{parquet_name}"
```
This guarantees that:
- Every unique raw file has its own unique Parquet footprint in Bronze.
- Overwrites are physically impossible.
- Anomaly files like `010.jpg.csv` map to `010.jpg_hash.parquet` instead of looking like a pure image conversion.
