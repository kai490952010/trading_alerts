import logging
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.utils.helpers import chain
from custom_postgreshook import CustomPostgresHook
from tickerdata.cryptocurrency import BinanceAPI

POSTGRES_CONN_ID = "postgres_default"
pg_hook = CustomPostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
logger = logging.getLogger("Historical refresh")


def create_incremental_refresh_dag(
    # fmt: off
    dag_id, schedule, symbol, granularity,
    symbol_id, default_args
    # fmt: on
):
    """
    Function returns a dag instance which loads data from
    Binance's S3 bucket to load incremental data.
    S3 Bucket containing monthly/daily data can be used to load
    data containing different intervals.

    Args:
        dag_id (str): dag ID.
        schedule (str): schedule at which DAG should run.
        symbol (str): Stock/Crypto symbol which will be used to pull data.
        symbol_id (int): Symbol Id of the symbol which was shared.
        granularity (str): Time period for which candles are created.
                Ex: 1m, 5m etc.
    Returns:
        instance of Airflow DAG.
    """

    def load_data(symbol, symbol_id, start_ts, end_ts, granularity):
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

        def get_ts_shift(ts, i):
            shifted_date = ts + 86400 * i
            return round(shifted_date.timestamp())

        # calculate total 24h batches required to backfill data

        time_diff_secs = (end_ts - start_ts).total_seconds()
        batches = round(time_diff_secs / 60 / 60 / 24)

        log_message = "Getting data: {:d} {:d} to {:d}"
        logger.info(log_message.format(symbol, start_ts, end_ts))
        data = [
            api.get_historical(
                symbol,
                start_ts=get_ts_shift(start_ts, i),
                end_ts=get_ts_shift(start_ts, i + 1) + 1,
                granularity=granularity,
            )
            for i in range(batches)
        ]

        df = pd.concat(data)
        df.loc[:, "symbol_id"] = symbol_id
        pg_hook.write_pandas_df(
            df.to_json(),
            name="tickerdata_{:s}".format(granularity),
            if_exists="append",
            index=False,
        )

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

    dag = DAG(dag_id, schedule_interval=schedule, default_args=default_args)

    with dag:
        # fmt: off
        t1 = PythonOperator(
            task_id="delete_data_{:s}".format(symbol),
            python_callable=delete_data
        )
        t2 = PythonOperator(
            task_id="load_data_{:s}".format(symbol),
            python_callable=load_data
        )
        # fmt: on

        SQL = """UPDATE symbols
            SET symbol_last_updated_at=timezone('utc', NOW())
            WHERE symbol_id = {:d}""".format(
            symbol_id
        )
        t3 = PostgresOperator(
            task_id="Set_{:s}_last_updated_at".format(symbol),
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=SQL,
        )
        t1 >> t2 >> t3

    return dag


def get_symbols_needing_incremental_refresh():
    """
    Function runs a SQL statement to identify
    symbols which have not received fresh data
    (last recieved data is more than 30 days old).
    """
    N = 1  # number of symbols to update per dag run
    symbols_needing_incremental_refresh = """
        SELECT x.symbol_id, x.symbol_name, last_close_time
        FROM symbols x
        LEFT JOIN (
            SELECT symbol_id, max(close_time) last_close_time
            FROM tickerdata_1m
            GROUP BY 1) y ON x.symbol_id = y.symbol_id
        WHERE (EXTRACT(EPOCH
                FROM last_close_time - timezone('utc', now())
                )/3600/24 < 30)
    """
    # For Testing purposes:
    # symbols_needing_incremental_refresh = """
    #     SELECT 3649 as symbol_id, 'EOSUSDT' as symbol_name
    # """
    symbol_df = pg_hook.get_pandas_df(symbols_needing_incremental_refresh)
    symbol_df = symbol_df.head(N)
    return symbol_df


def create_subdag_operator(**kwargs):
    """
    Creates subdags used to pull incremental data specified by a
    time range in epoc timestamp - (start_ts, end_ts)

    Args
        - symbol (str): coinpair which Binance API recognises.
        - symbol_id (int): foreign key to data rows in database.
        - start_ts (int): time range beginning, in epoch timestamp
        - end_ts (int): time range end, in epoch timestamp
        - granularity (str): Time period of granularity.
            ex: 1m, 5m, 15m.
            See, Binance's API documentation for further options.

    Returns
        - Returns JSON object, containing Pandas Dataframe.
    """
    symbol = kwargs.get("symbol")
    symbol_id = kwargs.get("symbol_id")
    granularity = kwargs.get("granularity")
    schedule = "@once"

    subdag_id_suffix = "incremental_{}".format(str(symbol))
    # print(subdag_id_suffix)
    parent_dag_id = parent_dag.dag_id.split(".")[0]
    subdag_id = "{}.{}".format(parent_dag_id, subdag_id_suffix)
    logger.info("Creating dag for {:s}".format(subdag_id_suffix))
    subdag = create_incremental_refresh_dag(
        subdag_id, schedule, symbol, granularity, symbol_id, default_args
    )

    # fmt: off
    sd_op = SubDagOperator(task_id=subdag_id_suffix,
                           dag=parent_dag,
                           subdag=subdag)
    # fmt: on
    return sd_op


def create_subdag_operators(parent_dag, symbol_df, **kwargs):
    """
    Function breaks down intervals for which data is missing into
    smaller time periods. This makes it easier to fetch data.

    Args:
        parent_dag (DAG instance): parent to which subdags will
            be linked to. Should be an Airflow DAG instance.
        symbol_df (dataframe): Dataframe containing symbols,
            time_since last refresh.

    Returns:
        None
    """

    subdags = []
    if not symbol_df.empty:

        # for each symbol
        for index, row in symbol_df.iterrows():
            schedule = "@once"
            symbol_id = row["symbol_id"]
            symbol_name = row["symbol_name"]
            last_known_ts = row["last_close_time"]
            current_ts = datetime.utcnow()

            # make list of subdag operators
            subdags.extend(
                [
                    create_subdag_operator(
                        parent_dag=parent_dag,
                        schedule=schedule,
                        symbol=symbol_name,
                        granularity=kwargs.get("granularity"),
                        symbol_id=symbol_id,
                        start_ts=last_known_ts,
                        end_ts=current_ts,
                        default_args=default_args,
                    )
                ]
            )
            # chain subdag-operators together
            chain(*subdags)
        return subdags


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "catchup": False,  # prevents backfill jobs
}

with DAG(
    dag_id="incremental_refresh_dag_v1",
    default_args=default_args,
    schedule_interval=None,
) as parent_dag:
    symbol_df = get_symbols_needing_incremental_refresh()
    # fmt: off
    subdag_ops = create_subdag_operators(parent_dag,
                                         symbol_df,
                                         granularity="1m")
    # fmt: on

    # Dummy operations added to see quick progress on Airflow UI
    dummy_op_start = DummyOperator(task_id="dummy_op_start", dag=parent_dag)
    dummy_op_start >> subdag_ops[0]

    dummy_op_end = DummyOperator(task_id="dummy_op_end", dag=parent_dag)
    subdag_ops[-1] >> dummy_op_end
