"""
Unit tests for ETL class
"""
import pytest
import pandas as pd
from src.etl.main import ETL


def test_etl_initialization():
    """Test ETL class initialization"""
    etl = ETL()
    assert isinstance(etl, ETL)


def test_add_metadata(sample_dataframe):
    """Test add_metadata method"""
    etl = ETL()
    result = etl.add_metadata(sample_dataframe)
    assert isinstance(result, pd.DataFrame)
    assert 'audit_insertion_user' in result.columns
    assert 'audit_insertion_dt' in result.columns
    assert 'audit_insertion_id' in result.columns
    assert len(result) == len(sample_dataframe)


@pytest.mark.parametrize("date,expected", [
    ("2023-01-01", "2023-01-01"),
    ("2023-12-31", "2023-12-31")
])
def test_format_data(sample_dataframe, date, expected):
    """Test format_data method with parametrized dates"""
    etl = ETL()
    result = etl.format_data(sample_dataframe, date)
    assert isinstance(result, pd.DataFrame)
    assert 'Time' in result.columns
    assert all(result['Time'].astype(str).str.len() == 19)
    assert all(result['Time'].astype(str).str[:10] == expected)
    assert len(result) == len(sample_dataframe)


def test_filter_data(sample_dataframe):
    """Test filter_data method"""
    etl = ETL()
    result = etl.filter_data(sample_dataframe)
    assert isinstance(result, pd.DataFrame)
    assert len(result) <= len(sample_dataframe)  # Filtered data should not be larger


def test_save_data_into_db(sample_dataframe, mock_db_connector):
    """Test save_data_into_db method"""
    etl = ETL()
    df = sample_dataframe
    df['audit_col1'] = 'test_value'
    df['audit_col2'] = 'test_value'
    df['audit_col3'] = 'test_value'
    etl.save_data_into_db(
        sample_dataframe,
        table_name="test_table",
        replace_table=True
    )
