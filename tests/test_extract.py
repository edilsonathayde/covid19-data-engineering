import pytest
from src.etl.extract import fetch_data_from_url

def test_fetch_data_from_url():
    url = "https://storage.googleapis.com/covid19-open-data/v3/latest/aggregated.csv"
    content = fetch_data_from_url(url)
    assert content is not None
    assert len(content) > 0
