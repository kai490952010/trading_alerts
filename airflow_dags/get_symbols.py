import json
import logging

LOGGER = logging.getLogger("GetSymbols")

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from custom_postgreshook import CustomPostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd

from datetime import datetime
from tickerdata.cryptocurrency import CoingeckoAPI
from tickerdata.meta import metaApi

POSTGRES_CONN_ID = 'postgres_default'
pg_hook = CustomPostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
default_args = {
    'owner': 'airflow',
}


@dag("get_symbols", 
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None, 
    tags=['crypto'])
def get_symbols(market):
    """
    ### Incremental data refresh
    Pulls and loads incremental data for a symbol between a given 
    time range.
    It can be used to pull data for different time intervals.
    """

    @task()
    def find_symbols(market):
        """
        Gets the most popular X number of symbols and stores them in the symbol table.
        For crypto exchanges, by default, 100 symbols are shown.
        Args
            - market (str): identifies which market to pull symbols for.
                Expected values: crypto, shanghai, nyse

        Returns
            - Returns None
        """
        if market == 'crypto':
            api = CoingeckoAPI()
            coinpairs = api.get_coin_pairs('binance')
            return json.dumps({"symbols": coinpairs})
        elif market == 'shanghai':
            raise NotImplementedError
        elif market == 'nyse':
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
        print("Initial : ", symbol_list)
        symbol_list = json.loads(symbol_list)['symbols']
        print("pythonic : ", symbol_list, type(symbol_list))
        df_symbols = pg_hook.get_pandas_df("SELECT * FROM symbols")
        # import pdb; pdb.set_trace()
        new_symbols = pd.DataFrame({
            'symbol_name': pd.Series([symbol for symbol in symbol_list
                if symbol not in df_symbols.symbol_name]),
            'symbol_last_updated_at': datetime.now().timestamp(),
            'symbol_created_at': datetime.now().timestamp()
        })
        print(new_symbols.head())

        # TODO: make exchange id linking robust
        new_symbols.loc[:, 'exchange_id'] = 1 if market == 'crypto' else NULL
        LOGGER.info("Getting {:d} new symbols".format(new_symbols.shape[0]))
        if new_symbols.shape[0]:
            pg_hook.write_pandas_df(
                new_symbols.to_json(),
                name='symbols',
                if_exists='append',
                index=False)

    symbol_list = find_symbols(market)
    load_symbols(symbol_list, market)


get_symbols_dag = get_symbols(market='crypto')
