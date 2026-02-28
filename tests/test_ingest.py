import pytest
import json
from unittest.mock import patch, MagicMock

from src.bronze.ingest_api import fetch_breweries, fetch_all_breweries, save_to_bronze

MOCK_PAGE_1 = [
    {"id": "1", "name": "Brew A", "brewery_type": "micro", "state": "California", "country": "United States"},
    {"id": "2", "name": "Brew B", "brewery_type": "nano",  "state": "Texas",      "country": "United States"},
]


@patch("src.bronze.ingest_api.requests.get")
def test_fetch_breweries_returns_correct_data(mock_get):
    """fetch_breweries should return parsed JSON on a successful API call."""
    mock_response = MagicMock()
    mock_response.json.return_value = MOCK_PAGE_1
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    result = fetch_breweries(page=1)

    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["name"] == "Brew A"
    assert result[1]["brewery_type"] == "nano"


@patch("src.bronze.ingest_api.requests.get")
def test_fetch_breweries_raises_on_http_error(mock_get):
    """fetch_breweries should propagate HTTPError so Airflow can retry."""
    import requests
    mock_get.side_effect = requests.exceptions.HTTPError("404 Not Found")

    with pytest.raises(requests.exceptions.HTTPError):
        fetch_breweries(page=999)


@patch("src.bronze.ingest_api.requests.get")
def test_fetch_breweries_raises_on_timeout(mock_get):
    """fetch_breweries should propagate Timeout so Airflow can retry."""
    import requests
    mock_get.side_effect = requests.exceptions.Timeout()

    with pytest.raises(requests.exceptions.Timeout):
        fetch_breweries(page=1)


@patch("src.bronze.ingest_api.fetch_breweries")
def test_fetch_all_breweries_paginates_until_empty(mock_fetch):
    """fetch_all_breweries should stop pagination when API returns an empty list."""
    mock_fetch.side_effect = [MOCK_PAGE_1, MOCK_PAGE_1, []]

    result = fetch_all_breweries()

    assert len(result) == 4
    assert mock_fetch.call_count == 3


def test_save_to_bronze_creates_valid_json(tmp_path):
    """save_to_bronze should create a timestamped JSON file with the correct content."""
    filepath = save_to_bronze(MOCK_PAGE_1, bronze_path=str(tmp_path))

    assert filepath.endswith(".json")
    assert "breweries_raw_" in filepath

    with open(filepath, encoding="utf-8") as f:
        saved = json.load(f)

    assert len(saved) == 2
    assert saved[0]["id"] == "1"
    assert saved[1]["id"] == "2"