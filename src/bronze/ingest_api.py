import requests
import json
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_URL = "https://api.openbrewerydb.org/v1/breweries"
BRONZE_PATH = "/opt/airflow/data/bronze"


def fetch_breweries(page: int = 1, per_page: int = 200) -> list:
    """Fetch a single page of breweries from Open Brewery DB API."""
    url = f"{BASE_URL}?page={page}&per_page={per_page}"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Fetched {len(data)} breweries from page {page}")
        return data
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error on page {page}: {e}")
        raise
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error: {e}")
        raise
    except requests.exceptions.Timeout:
        logger.error(f"Request timed out on page {page}")
        raise


def fetch_all_breweries() -> list:
    """Paginate through the entire API and return all breweries."""
    all_breweries = []
    page = 1

    while True:
        data = fetch_breweries(page=page)
        if not data:
            logger.info(f"No more data at page {page}. Total fetched: {len(all_breweries)}")
            break
        all_breweries.extend(data)
        logger.info(f"Accumulated {len(all_breweries)} breweries so far...")
        page += 1

    return all_breweries


def save_to_bronze(data: list, bronze_path: str = BRONZE_PATH) -> str:
    """Persist raw JSON data to the bronze layer with a timestamp in the filename."""
    os.makedirs(bronze_path, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(bronze_path, f"breweries_raw_{timestamp}.json")

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved {len(data)} breweries to bronze layer: {filepath}")
    return filepath


def ingest_breweries() -> str:
    """Main entry point: fetch all breweries from the API and persist to bronze."""
    logger.info("Starting brewery ingestion from Open Brewery DB API...")
    data = fetch_all_breweries()

    if not data:
        raise ValueError("No data fetched from API. Aborting ingestion.")

    filepath = save_to_bronze(data)
    logger.info(f"Ingestion complete. File saved at: {filepath}")
    return filepath


if __name__ == "__main__":
    ingest_breweries()