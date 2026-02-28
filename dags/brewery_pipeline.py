import os
import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

default_args = {
    "owner": "diogo.toledo",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="brewery_pipeline",
    default_args=default_args,
    description="BEES Data Engineering Case — Medallion Architecture (Bronze -> Silver -> Gold)",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["inbev", "breweries", "medallion"],
) as dag:

    def run_bronze():
        from src.bronze.ingest_api import ingest_breweries
        filepath = ingest_breweries()
        logger.info(f"Bronze task complete. File: {filepath}")
        return filepath

    def run_silver():
        from src.silver.transform import transform_breweries
        path = transform_breweries()
        logger.info(f"Silver task complete. Path: {path}")
        return path

    def run_gold():
        try:
            import pandas as pd

            silver_path = "/opt/airflow/data/silver"
            gold_path   = "/opt/airflow/data/gold"

            # Debug: lista o que existe no silver
            logger.info(f"Checking silver path: {silver_path}")
            logger.info(f"Silver exists: {os.path.exists(silver_path)}")
            if os.path.exists(silver_path):
                for root, dirs, files in os.walk(silver_path):
                    for f in files:
                        logger.info(f"Found silver file: {os.path.join(root, f)}")

            from src.gold.aggregate import aggregate_gold
            path = aggregate_gold()
            logger.info(f"Gold task complete. Path: {path}")
            return path

        except Exception as e:
            logger.error(f"Gold task FAILED with error: {type(e).__name__}: {e}")
            raise

    def run_quality_check():
        try:
            import pandas as pd

            gold_file = "/opt/airflow/data/gold/breweries_aggregated.parquet"

            if not os.path.exists(gold_file):
                raise FileNotFoundError(
                    f"Data quality check FAILED: gold file not found at {gold_file}"
                )

            df = pd.read_parquet(gold_file, engine="pyarrow")

            if df.empty:
                raise ValueError("Data quality check FAILED: gold layer is empty.")

            total = df["brewery_count"].sum()
            if total == 0:
                raise ValueError("Data quality check FAILED: brewery_count sums to zero.")

            key_cols = ["brewery_type", "country", "state"]
            null_counts = df[key_cols].isnull().sum()
            if null_counts.any():
                raise ValueError(
                    f"Data quality check FAILED: null values in key columns: "
                    f"{null_counts[null_counts > 0].to_dict()}"
                )

            logger.info(
                f"Data quality check PASSED — "
                f"{len(df)} groups | {total} total breweries | "
                f"{df['country'].nunique()} countries | {df['state'].nunique()} states"
            )

        except Exception as e:
            logger.error(f"Quality check FAILED with error: {type(e).__name__}: {e}")
            raise

    bronze_task = PythonOperator(
        task_id="bronze_ingest_api",
        python_callable=run_bronze,
    )

    silver_task = PythonOperator(
        task_id="silver_transform_parquet",
        python_callable=run_silver,
    )

    gold_task = PythonOperator(
        task_id="gold_aggregate",
        python_callable=run_gold,
    )

    quality_task = PythonOperator(
        task_id="data_quality_check",
        python_callable=run_quality_check,
    )

    bronze_task >> silver_task >> gold_task >> quality_task