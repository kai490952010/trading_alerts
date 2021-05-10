from contextlib import closing

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


class CustomPostgresHook(PostgresHook):
    def write_pandas_df(self, json_df, parameters=None, **kwargs):
        """
        Executes the sql and returns a pandas dataframe

        :param df: the pandas dataframe to be written
        :type sql: str or list
        :param parameters: The parameters to render the SQL query with.
        :type parameters: dict or iterable
        :param kwargs: (optional) passed into pandas.io.sql.read_sql method
        :type kwargs: dict
        """
        df = pd.read_json(json_df)

        with closing(self.get_conn()) as conn:  # NOQA
            # get uri connection from parent class
            connection = super(PostgresHook, self).get_uri()
            df.to_sql(con=connection, **kwargs)
