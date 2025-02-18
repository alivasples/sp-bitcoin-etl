"""
Unit tests for UtilsPandas class
"""
import pytest
import pandas as pd
from src.utils.utils_pandas import UtilsPandas


def test_log_df_metadata_decorator(sample_dataframe, caplog):
    """Test log_df_metadata decorator"""

    @UtilsPandas.log_df_metadata
    def test_function(df):
        return df

    result = test_function(sample_dataframe)
    # Verify the decorator logged the DataFrame info
    assert result is not None
    assert isinstance(result, pd.DataFrame)


def test_log_df_metadata_empty_dataframe(empty_dataframe, caplog):
    """Test log_df_metadata decorator with empty DataFrame"""

    @UtilsPandas.log_df_metadata
    def test_function(df):
        return df

    result = test_function(empty_dataframe)
    assert isinstance(result, pd.DataFrame)
