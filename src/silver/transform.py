import pandas as pd
import os
import json
import logging
from glob import glob

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BRONZE_PATH = "/opt/airflow/data/bronze"
SILVER_PATH = "/opt/airflow/data/silver"


def load_latest_bronze(bronze_path: str = BRONZE_PATH) -> list:
    """Load the most recent raw JSON file from the bronze layer."""
    files = sorted(glob(os.path.join(bronze_path, "breweries_raw_*.json")))
    if not files:
        raise FileNotFoundError(f"No bronze files found at: {bronze_path}")
    latest = files[-1]
    logger.info(f"Loading bronze file: {latest}")
    with open(latest, "r", encoding="utf-8") as f:
        return json.load(f)


def clean_and_transform(data: list) -> pd.DataFrame:
    """
    Transformations applied to the raw data:
      1. Convert raw list of dicts to a DataFrame.
      2. Normalize column names to lowercase and strip whitespace.
      3. Return empty DataFrame early if input is empty or has no rows.
      4. Drop rows where brewery_type or state is null
         (both are required for silver partitioning and gold aggregation).
      5. Fill remaining missing string fields with 'unknown'
         to avoid null propagation downstream.
      6. Cast latitude and longitude to float for potential geo analysis.
    """
    df = pd.DataFrame(data)

    if df.empty:
        logger.warning("Input data is empty. Returning empty DataFrame.")
        return df

    df.columns = [c.lower().strip() for c in df.columns]

    # Drop rows missing critical fields
    before = len(df)
    df = df.dropna(subset=["brewery_type", "state"])
    dropped = before - len(df)
    if dropped > 0:
        logger.warning(f"Dropped {dropped} rows with missing brewery_type or state")

    # Fill missing optional string fields
    str_cols = ["city", "country", "state_province", "postal_code", "phone", "website_url", "address_1", "address_2", "address_3"]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].fillna("unknown")

    # Cast geographic coordinates to float
    for coord in ["latitude", "longitude"]:
        if coord in df.columns:
            df[coord] = pd.to_numeric(df[coord], errors="coerce")

    logger.info(f"Transformation complete: {len(df)} rows, {len(df.columns)} columns")
    return df


def save_to_silver(df: pd.DataFrame, silver_path: str = SILVER_PATH) -> str:
    """
    Persist the transformed DataFrame to the silver layer as Parquet files,
    partitioned by country and state for efficient location-based queries.
    Uses existing_data_behavior='delete_matching' to support idempotent reruns.
    """
    os.makedirs(silver_path, exist_ok=True)

    df.to_parquet(
        silver_path,
        engine="pyarrow",
        partition_cols=["country", "state"],
        index=False,
        existing_data_behavior="delete_matching",
    )

    logger.info(f"Silver layer written at: {silver_path} (partitioned by country/state)")
    return silver_path


def transform_breweries() -> str:
    """Main entry point: load bronze data, transform it, and persist to silver."""
    logger.info("Starting silver transformation...")
    data = load_latest_bronze()

    if not data:
        raise ValueError("Bronze layer returned empty data. Aborting transformation.")

    df = clean_and_transform(data)
    path = save_to_silver(df)
    logger.info(f"Silver transformation complete. Data at: {path}")
    return path


if __name__ == "__main__":
    transform_breweries()