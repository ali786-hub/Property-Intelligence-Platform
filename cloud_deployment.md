# PropIntel Azure Cloud Deployment Architecture

Because you have Azure credits, an existing Azure PostgreSQL database, and want to scale to TBs of data, Microsoft Azure is the perfect target for production. 

The beauty of the DuckDB + PyIceberg architecture we built is that it is **cloud-native by default**. You will not need to rewrite your transformation logic (SQL) or your schemas. Moving to Azure simply means changing *where* the files are saved and *where* Airflow runs. Everything—storage and compute—will run 100% in the cloud.

---

## 1. Storage Layer: Azure Data Lake Storage Gen2 (ADLS Gen2)

Currently, your Bronze, Silver, and Gold zones sit on your local `C:/` drive. In the cloud, these will move to **Azure Blob Storage (configured with Hierarchical Namespace to act as ADLS Gen2)**.

### How it changes the code:
DuckDB and PyIceberg can natively read from and write to Azure Blob Storage over the network. You will simply change your `.env` paths:
```env
# Local 
BRONZE_ZONE="C:/Omnijourney_Kofking_github/data/bronze"

# Cloud
BRONZE_ZONE="abfs://propintel-lake@propidatalake.dfs.core.windows.net/bronze"
SILVER_ZONE="abfs://propintel-lake@propidatalake.dfs.core.windows.net/silver"
GOLD_ZONE="abfs://propintel-lake@propidatalake.dfs.core.windows.net/gold"
```

### Required Dependencies:
You will add the `adlfs` (Azure Data Lake File System) package to your `requirements.txt`. DuckDB will automatically use this to stream data directly into memory without downloading the files first.

## 2. Compute Layer (100% Cloud): Azure Container Instances (ACI) or VMs

Your Airflow orchestration and DuckDB compute will move completely off your local machine into Azure. 

Since we already created a custom `Dockerfile` and `docker-compose.yml`, deploying compute to Azure is incredibly straightforward. You have two main options for 100% cloud compute:

1. **Azure Virtual Machine (IaaS):** Spin up a standard Linux VM (e.g., a memory-optimized `E-series` VM since DuckDB loves RAM), SSH in, and run `docker-compose up -d`. This provides a dedicated cloud server doing all the heavy lifting.
2. **Azure Container Instances (ACI):** A fully managed, serverless Docker environment. You can deploy your Airflow containers directly without managing the underlying OS.

**Recommendation:** Start with an Azure VM running Docker Compose. It gives you the exact same terminal control you are used to locally, but with massive cloud server RAM.

## 3. Metadata Layer: Azure Database for PostgreSQL

You are already doing this! The `file_lineage` table and the PyIceberg `iceberg_tables` catalog are already hosted on your Azure Flexible Server. 

### How it changes the code:
**Zero changes required.** When you deploy your compute to the cloud VM, it will connect to the exact same Postgres connection string you are already using.

---

## The Migration Plan (When you are ready)

When you are ready to use your cloud credits and test the 110+ files:

1. **Fork the Repo:** Create a branch/fork named `azure-production` so your local environment remains untouched.
2. **Create the Storage Account:** In Azure, create a Storage Account with "Hierarchical namespace" enabled (ADLS Gen2) and create a container named `propintel-lake`.
3. **Update `.env`:** Change your zone paths to `abfs://...` and add your `AZURE_STORAGE_KEY`.
4. **Deploy Compute:** Spin up an Azure VM, clone the repo, and run `docker-compose up --build`.
5. **Run the Pipeline:** The cloud Airflow instance will wake up, DuckDB will securely stream data from Azure Blob into the VM's RAM, process the SCD Type 2 logic, and write the Iceberg files directly back to Azure Blob.
