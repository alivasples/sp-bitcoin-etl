"""
Configuration and fixtures for pytest
"""
import pytest
import pandas as pd
from unittest.mock import MagicMock
from datetime import datetime, timedelta
from src.utils.db_connector import DbConnector


@pytest.fixture
def mock_db_connector(monkeypatch):
    """Mock DB connector for testing"""
    mock_connector = MagicMock(spec=DbConnector)
    mock_connector.create_table = MagicMock()
    mock_connector.insert_many_data = MagicMock()
    mock_connector.execute_query = MagicMock()
    mock_connector.execute_query_many = MagicMock()

    # Mock the mysql connection
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    mock_connector.db_conn = mock_connection  # Ensure db_conn is set properly

    # Correctly patch the actual module where DbConnector is imported
    monkeypatch.setattr('src.utils.db_connector.DbConnector', MagicMock(return_value=mock_connector))

    return mock_connector


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing"""
    return pd.DataFrame({
        'Time': ['00:01:02', '01:02:03', '03:04:05'],
        'open': [1.0, 1.1, 1.2],
        'high': [2.0, 2.5, 1.8],
        'low': [0.8, 0.9, 1.0],
        'close': [1.5, 2.0, 1.7],
        'volume_btc': [100, 200, 300],
        'volume_currency': [150, 250, 400],
        'weighted_price': [1.7, 2.1, 1.6]
    })

@pytest.fixture
def empty_dataframe():
    """Create an empty DataFrame for testing"""
    return pd.DataFrame()
