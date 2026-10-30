# Cloud E-Commerce Data Pipeline Architecture (ETL)

## 📌 Architecture Overview
An automated, production-ready data orchestration pipeline that ingests raw, relational e-commerce source data from Kaggle, runs strict schema quality checks, streams it into a cloud data warehouse, and compiles a high-performance analytics Star Schema.

[ Kaggle Server ] (API Source Data)
│
▼
[ GitHub Codespaces App ] (Python Engine)
│
▼
[ Google Cloud Storage (GCS) ] (Raw Landing Lake)
│
▼
[ Google BigQuery ] (Data Warehouse Sandbox)
│
▼  (SQL Transformation Framework)
[ Fact & Dimension Star Schema ]

## 🛠️ Tech Stack & Infrastructure
* **Orchestration Core:** Apache Airflow 3.x (Task graph dependencies and scheduling)
* **Ingestion Engine:** Python 3.14 (Pandas dataframe streaming, Google Cloud API integration)
* **Data Warehouse:** Google BigQuery (Columnar storage, transformations)
* **Data Modeling Strategy:** Star Schema Design (`fact_orders`, `dim_customers`)

## ⚙️ Core Pipeline Phases
1. **`extract_and_ingest_raw_data`:** Interacts with the Kaggle API, downloads datasets asynchronously, drops raw data into BigQuery tables, and handles unescaped text strings gracefully via custom load job configurations.
2. **`run_data_quality_validations`:** Acts as a data gatekeeper by running pre-load integrity assertions to catch orphan records and `NULL` keys before processing down-funnel.
3. **`compile_star_schema_transformations`:** Executes high-speed SQL compilation directly inside BigQuery to format fact operational tables and dimension location tables.

## 🚀 How to Run Locally
1. Clone the repository.
2. Populate `gcp-key.json` and `~/.kaggle/access_token`.
3. Start the orchestrator server:
   ```bash
   airflow standalone
   ```