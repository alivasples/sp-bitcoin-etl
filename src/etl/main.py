"""
Author: Alexis Aspauza
Date: 2025-02-16
Description: Main class for Bitcoin ETL
"""

# Python modules imports
import os
import pandas as pd
import numpy as np
import uuid
from dotenv import dotenv_values

# Utils imports
from src.utils.utils_std import UtilsSTD
from src.utils.utils_pandas import UtilsPandas
from src.utils.db_connector import DbConnector

# Global definitions
CONFIG = dotenv_values('.env')
STAGE = CONFIG['STAGE']
DB_HOST = CONFIG['DB_HOST']
DB_PORT = CONFIG['DB_PORT']
DB_USER = CONFIG['DB_USER']
DB_PASS = CONFIG['DB_PASS']
DB_NAME = CONFIG['DB_NAME']
LANDING_FOLDER = 'data/landing/'
RAW_FOLDER = 'data/raw/'
FILE_TYPE = 'csv'
ETL_TAG = 'codech_alexis'

SRC_COLS = {
    'Time': 'str',
    'Open': 'float64',
    'High': 'float64',
    'Low': 'float64',
    'Close': 'float64',
    'Volume_(BTC)': 'float64',
    'Volume_(Currency)': 'float64',
    'Weighted_Price': 'float64'
}
DST_COLS = {
    'trx_datetime': 'datetime',
    'open': 'decimal(16,6)',
    'high': 'decimal(16, 6)',
    'low': 'decimal(16, 6)',
    'close': 'decimal(16, 6)',
    'volume_btc': 'decimal(16, 6)',
    'volume_currency': 'decimal(24, 6)',
    'weighted_price': 'decimal(16, 6)',
    'audit_insertion_user':  'varchar(100)',
    'audit_insertion_dt': 'datetime',
    'audit_insertion_id': 'varchar(100)'
}
DST_PK = 'trx_datetime'


class ETL():
    """
    This is the Main class which includes the code for implement the ETLs.
    """

    def __init__(self):
        """
        Constructor method. Instances the db connection object.
        """
        self._db_conn = DbConnector(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)

    @UtilsPandas.log_df_metadata
    def add_metadata(self, df: pd.DataFrame, **kwargs):
        """
        This method simple adds metadata columns

        Args:
            df (pd.Dataframe): The pandas dataframe which will be appended with metadata.
        """
        init_rows, init_cols = df.shape
        df['audit_insertion_user'] = ETL_TAG
        df['audit_insertion_dt'] = pd.to_datetime(UtilsSTD.get_now_str())
        df['audit_insertion_id'] = str(uuid.uuid4())
        end_rows, end_cols = df.shape
        # Validating there should not vary the number of rows at all
        assert init_rows == end_rows, f"The number of rows has changed. Before {init_rows} after:{end_rows}"
        return df

    @UtilsPandas.log_df_metadata
    def format_data(self, df: pd.DataFrame, measurement_date: str, **kwargs):
        """
        This method simple cleans and transform data giving them the correct format we except as output.

        Args:
            df (pd.Dataframe): The pandas dataframe which will be formatted.
            measurement_date (str): This is the load date of process
        """
        init_rows, init_cols = df.shape
        # Formatting columns
        df['Time'] = pd.to_datetime(measurement_date + ' ' + df['Time'])
        # Validating there should not vary the number of rows at all
        end_rows, end_cols = df.shape
        assert init_rows == end_rows, f"The number of rows has changed. Before {init_rows} after:{end_rows}"
        return df

    @UtilsPandas.log_df_metadata
    def filter_data(self, df: pd.DataFrame, **kwargs):
        """
        This method simple cleans and transform data giving them the correct format we except as output.

        Args:
            df (pd.Dataframe): The pandas dataframe which will be filtered.
        """
        init_rows, init_cols = df.shape
        # Dropping duplicates
        df = df.drop_duplicates(subset=['Time'], keep='first')
        # Dropping rows without time since it's the id
        df = df.dropna(subset=['Time'])
        # Uncomment next line if you want to also remove rows with all null values apart from Time
        # df = df.dropna(how='all', subset=df.columns.difference(['Time']))
        end_rows, end_cols = df.shape
        # Validating there are less or equal than original rows
        assert init_rows >= end_rows, f"The number of rows has augmented. Before {init_rows} after:{end_rows}"
        return df

    def save_data_into_db(self, df: pd.DataFrame, table_name: str, replace_table: bool = False,
                          ignore_duplicates: bool = True, **kwargs):
        """
        This method saves a dataframe using the self db connection.

        Args:
            df (pd.Dataframe): The pandas dataframe which will be filtered.
            table_name (str): The name of the table we will insert in.
            replace_table (bool): Whether to replace the table if it already exists. Default is False.
            ignore_duplicates (bool): True If we want to ignore duplicates, false if want to update tuples.
        """
        # Replacing np nan with nulls
        data = df.replace(np.nan, None)
        # Extracting all tuples
        data = list(data.itertuples(index=False, name=None))
        # Replacing np nan with nulls
        data = df.replace(np.nan, None)
        # Extracting all tuples
        data = list(data.itertuples(index=False, name=None))
        # Create the table if not exists
        self._db_conn.create_table(table_name=table_name, columns=DST_COLS, prim_key=DST_PK,
                                   replace=replace_table)
        # Inserting all data
        self._db_conn.insert_many_data(table_name=table_name, columns=list(DST_COLS.keys()), tuples=data,
                                       ignore_duplicates=True)

    def run_etl(self, db_replace_table: bool = False, db_ignore_duplicates=True):
        # List of all files in the landing folder
        filenames = UtilsSTD.get_filenames(LANDING_FOLDER, suffix=FILE_TYPE)
        # Process one by one
        for i, filename in enumerate(filenames):
            # The files in landing follow structure <table_name>-yyyy-mm-dd.csv
            # Then, we can extract the table name and the measurement date
            table_name, measurement_date = filename.split('-', 1)
            measurement_date = measurement_date.split('.')[0]
            year, month, day = measurement_date.split('-')
            print(f"Processing file {filename}")
            # Move file to raw folder so it won't be ingested again in the future
            raw_part_folder = f"{RAW_FOLDER}{table_name}/{year}/{month}/"
            if not os.path.exists(raw_part_folder):
                os.makedirs(raw_part_folder)
            os.rename(f"{LANDING_FOLDER}{filename}", f"{raw_part_folder}{filename}")
            # Reading data file as dataframe
            data_batch_path = f"{raw_part_folder}{filename}"
            df = pd.read_csv(data_batch_path, dtype=SRC_COLS)
            # data transformation
            df = self.format_data(df, measurement_date, log_df_name='CleanedDF')
            df = self.filter_data(df, log_df_name='FilteredDF')
            df = self.add_metadata(df, log_df_name='MetadataDF')
            # Printing completion percentage since there are many files
            prc_completion = int((i+1)*100/len(filenames))
            print('*'*prc_completion, f'{prc_completion}% Completed')
            # Saving results into the database
            self.save_data_into_db(df, table_name=table_name, replace_table=db_replace_table,
                                   ignore_duplicates=db_ignore_duplicates)



main_etl = ETL()
main_etl.run_etl(db_replace_table=False, db_ignore_duplicates=True)
