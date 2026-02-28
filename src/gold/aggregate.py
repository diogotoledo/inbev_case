import pandas as pd
import os
import logging
from glob import glob

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SILVER_PATH = "/opt/airflow/data/silver"
GOLD_PATH   = "/opt/airflow/data/gold"


def load_silver(silver_path: str = SILVER_PATH) -> pd.DataFrame:
    """Load all Parquet files from the silver layer into a single DataFrame.

    Uses explicit glob to collect only .parquet files, safely ignoring any
    non-Parquet files (e.g. .gitkeep) that may exist in the directory.
    """
    if not os.path.exists(silver_path):
        raise FileNotFoundError(f"Silver path not found: {silver_path}")

    parquet_files = glob(os.path.join(silver_path, "**/*.parquet"), recursive=True)

    if not parquet_files:
        raise FileNotFoundError(
            f"No Parquet files found in silver path: {silver_path}"
        )

    logger.info(f"Found {len(parquet_files)} Parquet file(s) in silver layer")

    df = pd.read_parquet(parquet_files, engine="pyarrow")

    # Parquet partition columns are read as Categorical — convert to string
    # to avoid groupby expansion issues (observed=False FutureWarning)
    for col in ["country", "state", "brewery_type"]:
        if col in df.columns and hasattr(df[col], "cat"):
            df[col] = df[col].astype(str)

    logger.info(f"Loaded silver layer: {len(df)} rows, {len(df.columns)} columns")
    return df


def aggregate_breweries(df: pd.DataFrame) -> pd.DataFrame:
    """
    Gold layer aggregation:
      - Groups breweries by brewery_type, country and state.
      - Counts the number of breweries per group.
      - observed=True ensures only existing combinations are returned,
        avoiding pandas expansion of all categorical combinations.

    Output columns: brewery_type, country, state, brewery_count
    """
    if df.empty:
        raise ValueError("Input DataFrame is empty. Cannot aggregate.")

    agg = (
        df.groupby(["brewery_type", "country", "state"], as_index=False, observed=True)
        .agg(brewery_count=("id", "count"))
        .sort_values(["country", "state", "brewery_type"])
        .reset_index(drop=True)
    )

    logger.info(
        f"Aggregation complete: {len(agg)} groups, "
        f"{agg['brewery_count'].sum()} total breweries"
    )
    return agg


def save_to_gold(df: pd.DataFrame, gold_path: str = GOLD_PATH) -> str:
    """Persist the aggregated DataFrame as a single Parquet file in the gold layer."""
    os.makedirs(gold_path, exist_ok=True)
    filepath = os.path.join(gold_path, "breweries_aggregated.parquet")
    df.to_parquet(filepath, engine="pyarrow", index=False)
    logger.info(f"Gold layer saved at: {filepath}")
    return filepath


def aggregate_gold() -> str:
    """Main entry point: load silver data, aggregate, and persist to gold."""
    logger.info("Starting gold aggregation...")
    df = load_silver()
    agg = aggregate_breweries(df)
    path = save_to_gold(agg)
    logger.info("Gold aggregation complete.")
    return path


if __name__ == "__main__":
    aggregate_gold()