import pytest
import pandas as pd

from src.silver.transform import clean_and_transform
from src.gold.aggregate import aggregate_breweries

MOCK_DATA = [
    {
        "id": "1", "name": "Brew A", "brewery_type": "micro",
        "state": "California", "country": "United States",
        "city": "Los Angeles", "latitude": "34.05", "longitude": "-118.24",
    },
    {
        "id": "2", "name": "Brew B", "brewery_type": "nano",
        "state": "Texas", "country": "United States",
        "city": "Austin", "latitude": "30.26", "longitude": "-97.74",
    },
    {
        "id": "3", "name": "Brew C", "brewery_type": None,
        "state": None, "country": "United States",
        "city": None, "latitude": None, "longitude": None,
    },
]


# ── Silver tests ─────────────────────────────────────────────────────────────

def test_transform_drops_rows_with_missing_critical_fields():
    """Rows with null brewery_type or state must be removed in the silver layer."""
    df = clean_and_transform(MOCK_DATA)
    assert len(df) == 2


def test_transform_fills_missing_city_with_unknown():
    """Missing optional string fields should be filled with 'unknown'."""
    data = [
        {"id": "1", "brewery_type": "micro", "state": "California",
         "country": "United States", "city": None, "latitude": None, "longitude": None},
    ]
    df = clean_and_transform(data)
    assert df["city"].iloc[0] == "unknown"


def test_transform_casts_coordinates_to_float():
    """latitude and longitude must be cast to float for geo analysis."""
    df = clean_and_transform(MOCK_DATA)
    assert df["latitude"].dtype == float
    assert df["longitude"].dtype == float


def test_transform_normalizes_column_names():
    """Column names must be lowercase after transformation."""
    df = clean_and_transform(MOCK_DATA)
    for col in df.columns:
        assert col == col.lower(), f"Column '{col}' is not lowercase"


def test_transform_returns_empty_dataframe_on_empty_input():
    """clean_and_transform should return an empty DataFrame gracefully on empty input,
    without raising a KeyError on missing columns."""
    df = clean_and_transform([])
    assert isinstance(df, pd.DataFrame)
    assert df.empty


# ── Gold tests ───────────────────────────────────────────────────────────────

def test_aggregate_count_per_group():
    """brewery_count must reflect the correct number of breweries per group."""
    df = pd.DataFrame([
        {"id": "1", "brewery_type": "micro", "state": "California", "country": "United States"},
        {"id": "2", "brewery_type": "micro", "state": "California", "country": "United States"},
        {"id": "3", "brewery_type": "nano",  "state": "Texas",      "country": "United States"},
    ])
    agg = aggregate_breweries(df)

    micro_ca = agg[(agg["brewery_type"] == "micro") & (agg["state"] == "California")]
    nano_tx  = agg[(agg["brewery_type"] == "nano")  & (agg["state"] == "Texas")]

    assert micro_ca["brewery_count"].values[0] == 2
    assert nano_tx["brewery_count"].values[0] == 1


def test_aggregate_output_has_required_columns():
    """Gold output must contain exactly brewery_type, country, state, brewery_count."""
    df = pd.DataFrame([
        {"id": "1", "brewery_type": "micro", "state": "California", "country": "United States"},
    ])
    agg = aggregate_breweries(df)
    assert set(agg.columns) == {"brewery_type", "country", "state", "brewery_count"}


def test_aggregate_raises_on_empty_dataframe():
    """aggregate_breweries must raise ValueError when receiving an empty DataFrame."""
    with pytest.raises(ValueError, match="empty"):
        aggregate_breweries(pd.DataFrame())