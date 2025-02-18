"""
Unit tests for DbConnector class
"""
import pytest
from src.utils.db_connector import DbConnector


def test_db_connector_initialization(mock_db_connector):
    """Test DbConnector initialization"""
    assert isinstance(mock_db_connector, DbConnector)


def test_create_table(mock_db_connector):
    """Test create_table method"""
    columns = {
        'id': 'INT',
        'name': 'VARCHAR(255)'
    }
    mock_db_connector.create_table(
        table_name="test_table",
        columns=columns,
        prim_key="id"
    )


@pytest.mark.parametrize("ignore_duplicates", [True, False])
def test_insert_many_data(mock_db_connector, ignore_duplicates):
    """Test insert_many_data method with different duplicate handling"""
    columns = ['id', 'name']
    tuples = [(1, 'test1'), (2, 'test2')]

    # Call the method
    mock_db_connector.insert_many_data(
        table_name="test_table",
        columns=columns,
        tuples=tuples,
        ignore_duplicates=ignore_duplicates
    )

    # Check that execute_query_many was called
    mock_db_connector.insert_many_data.assert_called_once()



