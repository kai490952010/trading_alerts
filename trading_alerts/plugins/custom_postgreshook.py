from contextlib import closing

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


class CustomPostgresHook(PostgresHook):
    def write_pandas_df(self, json_df, parameters=None, **kwargs):
        """
        Convert JSON object to Pandas dataframe and writes to table.

        Args:
            json_df (json): dataframe which is passed in as a JSON object.
            kwargs (dict): (optional) passed into
                pandas.io.sql.read_sql method to control how data is
                written.
        Returns:
            None
        """

        df = pd.read_json(json_df)

        with closing(self.get_conn()) as conn:  # NOQA
            # get uri connection from parent class
            connection = super(PostgresHook, self).get_uri()
            df.to_sql(con=connection, **kwargs)
