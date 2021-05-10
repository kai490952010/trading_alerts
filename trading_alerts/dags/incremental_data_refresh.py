from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from custom_postgreshook import CustomPostgresHook
from tickerdata.cryptocurrency import BinanceAPI

# from tickerdata.meta import metaApi

POSTGRES_CONN_ID = "postgres_default"
pg_hook = CustomPostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
default_args = {
    "owner": "airflow",
}


@dag(
    "incremental_data_refresh",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["crypto"],
)
def incremental_data_refresh(symbol, start_ts, end_ts, granularity):
    """
    ### Incremental data refresh
    Pulls and loads incremental data for a symbol between a given
    time range.
    It can be used to pull data for different time intervals.
    """

    @task()
    def extract(symbol, start_ts, end_ts, granularity):
        """
        Used to pull incremental data specified by a
        time range in epoc timestamp - (start_ts, end_ts)
        Args
            - symbol (str): coinpair which Binance API recognises.
            - start_ts (int): time range beginning, in epoch timestamp
            - end_ts (int): time range end, in epoch timestamp
            - granularity (str): Time period of granularity.
                ex: 1m, 5m, 15m.
                See, Binance's API documentation for further options.

        Returns
            - Returns JSON object, containing Pandas Dataframe.
        """
        api = BinanceAPI()
        data = api.get_historical(
            symbol, start_ts=start_ts, end_ts=end_ts, granularity=granularity
        )
        # use symbol table to embed integer symbol_id
        # TODO : replace with symbol_id
        data.loc[:, "symbol_id"] = 1
        return data.to_json()

    @task()
    def delete_data(symbol, symbol_id, start_ts, end_ts, granularity):
        """
        Deletes rows from table specified by a time range - (start_ts, end_ts)
        before writing to the table.

        Args
            - symbol (str): coinpair which Binance API recognises.
            - start_ts (int): time range beginning, in epoch timestamp
            - end_ts (int): time range end, in epoch timestamp
            - granularity (str): Time period of granularity.
                ex: 1m, 5m, 15m.
                See, Binance's API documentation for further options.

        Returns
            - Returns JSON object, containing Pandas Dataframe.
        """
        table = "ticker_data_{:s}".format(granularity)
        CMD = """
            DELETE FROM {table}
            WHERE close_time BETWEEN {start_ts} AND {end_ts}
                AND symbol_id = {symbol_id}
        """.format(
            table=table, start_ts=start_ts, end_ts=end_ts, symbol_id=symbol_id
        )

        PostgresOperator(
            task_id="delete_data_{:s}_{:s}".format(symbol, table),
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=CMD,
        )
        return True

    @task()
    def load(df, granularity):
        """
        Loads data pulled by extract task into database.
        Args
            - df (json): dataframe encoded into JSON object.
            - granularity (str): Time period of granularity.
                ex: 1m, 5m, 15m.
                See, Binance's API documentation for further options.

        Returns
            - None
        """
        pg_hook.write_pandas_df(
            df,
            name="tickerdata_{:s}".format(granularity),
            if_exists="append",
            index=False,
        )

    order_data = extract(symbol, start_ts, end_ts, granularity)
    symbol_id = 1  # TODO: fetch symbol_id from symbol_table.get(symbol)
    delete_successful = delete_data(symbol_id, start_ts, end_ts, granularity)
    if delete_successful:
        load(order_data, granularity)


incremental_data_refresh_dag = incremental_data_refresh(
    symbol="BTCUSDT", start_ts=1620211249, end_ts=1620221249, granularity="1m"
)
