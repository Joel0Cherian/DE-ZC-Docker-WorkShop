import os
import glob
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import bigquery

# 1. Credentials Configuration
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'env-gcp-key.json'

# 2. Pipeline Settings
DATASET = "olistbr/brazilian-ecommerce"
PROJECT_ID = "my-de-project-start"        # Confirm this matches your exact Project ID
BIGQUERY_DATASET = "Raw_ecommerce"         # Matches your exact case-sensitive dataset name

client = bigquery.Client(project=PROJECT_ID)

def run_ingestion():
    """Extracts raw CSV files from Kaggle and loads them into BigQuery."""
    print(f"🚀 [1/3] Fetching dataset '{DATASET}' from Kaggle...")
    
    # Initialize and log in to Kaggle
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(DATASET, path='./data', unzip=True)
    print("✅ Raw CSV data downloaded.")

    csv_files = glob.glob("./data/*.csv")
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        allow_quoted_newlines=True,
        quote_character='"',
        max_bad_records=100
    )

    print("📥 [2/3] Streaming raw files into cloud data warehouse...")
    for file_path in csv_files:
        raw_table_name = os.path.basename(file_path).replace('.csv', '')
        table_name = raw_table_name.replace('_dataset', '') 
        target_table = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"

        df = pd.read_csv(file_path)
        job = client.load_table_from_dataframe(df, target_table, job_config=job_config)
        job.result()  
        print(f"   ✨ Loaded {len(df)} rows into `{table_name}`.")

def validate_raw_data():
    """Runs a quality check to ensure critical tables do not contain broken or NULL primary keys."""
    print("🔍 [Quality Check] Validating primary integrity keys...")
    
    # Query to count missing customer IDs
    check_query = f"""
        SELECT COUNT(*) as missing_keys 
        FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.olist_customers` 
        WHERE customer_id IS NULL
    """
    query_job = client.query(check_query)
    results = query_job.result()
    
    for row in results:
        if row.missing_keys > 0:
            # Raise an explicit pipeline failure if data quality is compromised
            raise ValueError(f"🚨 Pipeline Aborted: Found {row.missing_keys} NULL keys in raw customer dataset!")
    
    print("   ✅ Quality validations cleared. No critical missing keys detected.")


def run_transformations():
    """Triggers BigQuery compilation to clean and build the Star Schema."""
    print("⚙️ [3/3] Executing SQL Transformation Framework...")

    # Dictionary of tables to build alongside their respective SQL code strings
    transform_queries = {
        "dim_customers": f"""
            CREATE OR REPLACE TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.dim_customers` AS
            SELECT
              customer_id,
              customer_unique_id,
              INITCAP(customer_city) AS customer_city,
              UPPER(customer_state) AS customer_state,
              customer_zip_code_prefix
            FROM
              `{PROJECT_ID}.{BIGQUERY_DATASET}.olist_customers`;
        """,
        "fact_orders": f"""
            CREATE OR REPLACE TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.fact_orders` AS
            SELECT
              o.order_id,
              o.customer_id,
              oi.seller_id,
              oi.product_id,
              TIMESTAMP(o.order_purchase_timestamp) AS purchase_at,
              TIMESTAMP(o.order_delivered_customer_date) AS delivered_at,
              oi.price AS item_price,
              oi.freight_value AS shipping_cost,
              (oi.price + oi.freight_value) AS total_order_value
            FROM
              `{PROJECT_ID}.{BIGQUERY_DATASET}.olist_orders` o
            JOIN
              `{PROJECT_ID}.{BIGQUERY_DATASET}.olist_order_items` oi
            ON
              o.order_id = oi.order_id
            WHERE
              o.order_status = 'delivered';
        """
    }

    # Iterate through the compilation jobs sequentially
    for table_name, sql_query in transform_queries.items():
        print(f"   🔨 Compiling and populating `{table_name}`...")
        query_job = client.query(sql_query)
        query_job.result()  # Wait for the BigQuery transformation job to complete
        print(f"   ✅ `{table_name}` is live.")

if __name__ == "__main__":
    print("🎬 Starting Data Pipeline Orchestration Run...")
    run_ingestion()
    validate_raw_data() 
    run_transformations()
    print("🎉 Pipeline Run Completed Successfully!")
