import json
import logging
from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from custom_postgreshook import CustomPostgresHook
from tickerdata.cryptocurrency import CoingeckoAPI

# from tickerdata.meta import metaApi

LOGGER = logging.getLogger("GetSymbols")
POSTGRES_CONN_ID = "postgres_default"
pg_hook = CustomPostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
default_args = {
    "owner": "airflow",
}


@dag(
    "get_symbols",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
    tags=["crypto"],
)
def get_symbols(market):
    """
    Gets top 100 symbols by market cap for which
    historical and incremental data refresh dags will be run.
    """

    @task()
    def find_symbols(market):
        """
        Gets the most popular X number of symbols and stores them
        in the symbol table.
        For crypto exchanges, by default, 100 symbols sorted by market cap
        are extracted.

        Args
            - market (str): identifies which market to pull symbols for.
                Expected values: crypto, shanghai, nyse

        Returns
            - Returns None
        """
        if market == "crypto":
            api = CoingeckoAPI()
            coinpairs = api.get_coin_pairs("binance")
            return json.dumps({"symbols": coinpairs})
        elif market == "shanghai":
            raise NotImplementedError
        elif market == "nyse":
            raise NotImplementedError

    @task()
    def load_symbols(symbol_list, market):
        """
        Loads freshly pulled symbols into symbol table
        Args
            - json_list: comma separated list of symbol names.

        Returns
            - None
        """
        symbol_list = json.loads(symbol_list)["symbols"]
        df_symbols = pg_hook.get_pandas_df("SELECT * FROM symbols")
        new_symbols = pd.DataFrame(
            {
                "symbol_name": pd.Series(
                    [
                        symbol
                        for symbol in symbol_list
                        if symbol not in df_symbols.symbol_name.unique()
                    ]
                ),
                "symbol_last_updated_at": datetime.now().timestamp(),
                "symbol_created_at": datetime.now().timestamp(),
                # TODO: make exchange id linking robust
                "exchange_id": 1 if market == "crypto" else None,
            }
        )
        print(new_symbols.head())

        LOGGER.info("Getting {:d} new symbols".format(new_symbols.shape[0]))
        if new_symbols.shape[0]:
            pg_hook.write_pandas_df(
                # fmt: off
                new_symbols.to_json(),
                name="symbols",
                if_exists="append",
                index=False
                # fmt: on
            )

    symbol_list = find_symbols(market)
    load_symbols(symbol_list, market)


get_symbols_dag = get_symbols(market="crypto")
