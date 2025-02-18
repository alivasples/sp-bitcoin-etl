"""
Author: Alexis Aspauza
Date: 2025-02-16
Description: This module contains the class DbConnector which handles with
MySQL operations.
"""
# Python libraries
import mysql.connector


class DbConnector:
    """
    This class handles with MySQL operations.
    """
    # Static attributes
    db_conn = None

    def __init__(self, host: str, user: str, password: str, database: str):
        """
        Constructor method using singleton pattern.

        Args:
            host (str): The host of the MySQL database.
            user (str): The user of the MySQL database.
            password (str): The password of the MySQL database.
            database (str): The database to be used.
        """
        if DbConnector.db_conn is None:
            DbConnector.db_conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )
            print(f"Connection for {database} created successfully.")
        else:
            print(f"Connection for {database} already exists.")

    def __del__(self):
        """
        Destructor method to close the database connection.
        """
        if DbConnector.db_conn is not None:
            DbConnector.db_conn.close()
            print("Database connection closed.")

    def execute_query(self, query: str):
        """
        Execute a query in the MySQL database.

        Args:
            query (str): The SQL query to be executed.
        """
        cursor = DbConnector.db_conn.cursor()
        cursor.execute(query)
        DbConnector.db_conn.commit()
        cursor.close()

    def execute_query_many(self, query: str, data: list):
        """
        Execute a query in the MySQL database mainly for list of inserts so they can be
        optimized in batches internally and therefore efficiently.

        Args:
            query (str): The SQL query to be executed.
            data (list): The list of tuples to be inserted.
        """
        cursor = DbConnector.db_conn.cursor()
        cursor.executemany(query, data)
        DbConnector.db_conn.commit()
        cursor.close()

    def create_table(self, table_name: str, columns: dict, prim_key: str, replace: bool = False):
        """
        Create a table in the MySQL database.

        Args:
            table_name (str): The name of the table to be created.
            columns (dict): A dictionary containing the column names and their data types.
            prim_key (str): The primary key columns separated by commas if composed.
            replace (bool):  Whether to replace the table if it already exists. Default is False.
        """
        if replace:
            query = f"DROP TABLE IF EXISTS {table_name}"
            self.execute_query(query)
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        for column_name, data_type in columns.items():
            query += f"{column_name} {data_type}, "
        if prim_key:
            query += f"PRIMARY KEY ({prim_key})"
        query += ")"
        self.execute_query(query)

    def insert_many_data(self, table_name: str, columns: list = [], tuples: list = [], ignore_duplicates: bool = False):
        """
        This function inserts efficiently tuples into the database table

        Args:
            table_name (str): The name of the table.
            columns (list): The list of columns where we will insert the values.
            tuples (list): The list of tuples to be inserted.
            ignore_duplicates (bool): True for ignore, False for updates.
        """
        if not tuples:
            print("No tuples to insert.")
            return
        assert len(columns) == len(
            tuples[0]), f"The number of columns {len(columns)} does not match the number of values {len(tuples[0])}."
        # Create the placeholders for the query
        column_header = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        ignore_str = "IGNORE" if ignore_duplicates else ""
        # Create the query
        query = f"INSERT {ignore_str} INTO {table_name} ({column_header}) VALUES ({placeholders})"
        if not ignore_duplicates:
            query += " ON DUPLICATE KEY UPDATE "
            query += ", ".join([f"{col} = VALUES({col})" for col in columns])
        # Execute the query
        self.execute_query_many(query, tuples)
