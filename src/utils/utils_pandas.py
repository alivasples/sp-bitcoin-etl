"""
Author: Alexis Aspauza
Date: 2025-02-16
Description: Utils functions for pandas.
"""


class UtilsPandas():
    # For printing or not all logs
    verbose = False

    @staticmethod
    def log_df_metadata(func):
        """ Decorator to log dataframe metadata """

        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            df_name = kwargs.get('log_df_name', 'DataFrame')
            if UtilsPandas.verbose:
                print('=' * 30)
                print(f'{df_name} shape: {result.shape}')
                print('=' * 30)
                print(f'{df_name} columns:')
                print(result.dtypes)
                print('=' * 30)
                print(f'{df_name} head:')
                print(result.head())
            return result

        return wrapper
