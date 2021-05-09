import logging
import os
import tempfile

import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.helpers import chain
from botocore import UNSIGNED
from botocore.client import Config
from custom_postgreshook import CustomPostgresHook

POSTGRES_CONN_ID = "postgres_default"
pg_hook = CustomPostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
logger = logging.getLogger("Historical refresh")


def create_historical_refresh_dag(
    # fmt: off
    dag_id, schedule, symbol, granularity,
    bucket, prefix, symbol_id, default_args
    # fmt: on
):
    """
    Function returns a dag instance which loads data from
    Binance's S3 bucket to load historical data quickly.
    S3 Bucket containing monthly/daily data can be used to load
    data containing different intervals.
    Args:
        schedule: schedule at which DAG should run.
        symbol (str): Stock/Crypto symbol which will be used to pull data.
        symbol_id (int): Symbol Id of the symbol which was shared.
        granularity (str): Time period for which candles are created.
                Ex: 1m, 5m etc.
        bucket (str): Binance's S3 bucket from which data is procured.
        prefix (str): Prefix to S3 folder within bucket.
    Returns:
        instance of Airflow DAG.
    """

    def get_list_from_s3(*args, **context):
        """
        Get list of data files from S3 bucket
        """
        updated_prefix = prefix.format(
            frequency="monthly", symbol=symbol, granularity=granularity
        )

        filepaths = []
        bucket_obj = S3Hook("aws_default").get_bucket(bucket)

        for obj in bucket_obj.objects.filter(Prefix=updated_prefix):
            path, filename = os.path.split(obj.key)
            if filename.endswith("zip"):
                filepaths.append(obj.key)

        context["ti"].xcom_push(key="file_list", value=filepaths)
        logging.info("Found {:d} files".format(len(filepaths)))

    def get_historical_data(*args, **context):

        filepaths = context["ti"].xcom_pull(
            task_ids="get_list_from_s3_{:s}".format(symbol), key="file_list"
        )
        for key in filepaths:
            logging.info("Downloading file: {:s}".format(key))
            # do not sign s3 requests
            config_no_sign = Config(signature_version=UNSIGNED)
            client = boto3.client("s3", config=config_no_sign)
            obj = client.get_object(Bucket=bucket, Key=key)
            file = obj.get("Body").read()
            temp_file = tempfile.NamedTemporaryFile().name
            with open(temp_file, "wb") as file_handle:
                file_handle.write(file)

            df = pd.read_csv(
                temp_file,
                compression="zip",
                header=None,
                names=[
                    "open_time",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "close_time",
                    "quote_av",
                    "trades",
                    "tb_base_av",
                    "tb_quote_av",
                    "ignore",
                ],
                dtype={"open_time": int, "close_time": int},
            )
            df = df.drop("ignore", axis=1)
            df.loc[:, "symbol_id"] = symbol_id

            pg_hook.write_pandas_df(
                df.to_json(),
                name="tickerdata_{:s}".format(granularity),
                if_exists="append",
                index=False,
            )

    dag = DAG(dag_id, schedule_interval=schedule, default_args=default_args)

    with dag:
        t1 = PythonOperator(
            task_id="get_list_from_s3_{:s}".format(symbol),
            python_callable=get_list_from_s3,
            provide_context=True,
        )
        t2 = PythonOperator(
            task_id="get_historical_data_{:s}".format(symbol),
            python_callable=get_historical_data,
            provide_context=True,
        )

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


def get_symbols_needing_historical():
    """
    Function runs a SQL statement to identify
    symbols which do not have any historical data.
    Idea is to backfill large years of data using historical feeds
    and use incremental data refresh to fill data spanning a few days.
    """
    N = 1  # number of symbols to update per dag run
    symbols_with_no_historic_data = """
        SELECT x.symbol_id, x.symbol_name
        FROM symbols x
        LEFT JOIN (SELECT symbol_id, max(close_time) last_close_time
          FROM tickerdata_1m
          GROUP BY 1) y ON x.symbol_id = y.symbol_id
        WHERE last_close_time IS NULL
    """
    # For Testing purposes:
    # symbols_with_no_historic_data = """
    #     SELECT 3649 as symbol_id, 'EOSUSDT' as symbol_name
    # """
    symbol_list = pg_hook.get_pandas_df(symbols_with_no_historic_data).head(N)
    logger.info("Getting historical data for {:d} symbols".format(N))
    return symbol_list


def create_subdag_operator(
    # fmt: off
    parent_dag, schedule, symbol, granularity,
    bucket, prefix, symbol_id, default_args
    # fmt: on
):
    """
    Function creates dag which will be consumed by SubDagOperator.
    Args:
        schedule: schedule at which DAG should run.
        symbol (str): Stock/Crypto symbol which will be used to pull data.
        symbol_id (int): Symbol Id of the symbol which was shared.
        granularity (str): Time period for which candles are created.
                Ex: 1m, 5m etc.
        bucket (str): Binance's S3 bucket from which data is procured.
        prefix (str): Prefix to S3 folder within bucket.
    Returns:
        instance of Airflow DAG.
    """
    subdag_id_suffix = "historical_data_refresh_{}".format(str(symbol))
    parent_dag_id = parent_dag.dag_id.split(".")[0]
    subdag_id = "{}.{}".format(parent_dag_id, subdag_id_suffix)
    logger.info("Creating dag for {:s}".format(subdag_id_suffix))
    subdag = create_historical_refresh_dag(
        subdag_id,
        schedule,
        symbol,
        granularity,
        bucket,
        prefix,
        symbol_id,
        default_args,
    )
    # fmt: off
    sd_op = SubDagOperator(task_id=subdag_id_suffix,
                           dag=parent_dag,
                           subdag=subdag)
    # fmt: on
    return sd_op


def create_subdag_operators(parent_dag, symbol_list):
    """
    Private function to create subdag operators based on
    given parent dag and list of symbols.
    """
    subdags = []
    for index, row in symbol_list.iterrows():
        symbol_id, symbol = row.loc["symbol_id"], row.loc["symbol_name"]
        granularity = "1m"
        bucket = "data.binance.vision"
        prefix = "data/spot/{frequency}/klines/{symbol}/{granularity}/"
        schedule = "@once"
        subdags.append(
            create_subdag_operator(
                parent_dag,
                schedule,
                symbol,
                granularity,
                bucket,
                prefix,
                symbol_id,
                default_args,
            )
        )
    # chain subdag-operators together
    chain(*subdags)
    return subdags


default_args = {
    "owner": "airflow",
    "start_date": "2020-05-09",
    "catchup": False,  # prevents backfill jobs
}

parent_dag = DAG(
    dag_id="historical_refresh_dag_v1",
    default_args=default_args,
    schedule_interval=None,
)

symbol_list = get_symbols_needing_historical()
subdag_ops = create_subdag_operators(parent_dag, symbol_list)

# Dummy operations added to see quick progress on Airflow UI
dummy_op_start = DummyOperator(task_id="dummy_op_start", dag=parent_dag)
dummy_op_start >> subdag_ops[0]

dummy_op_end = DummyOperator(task_id="dummy_op_end", dag=parent_dag)
subdag_ops[-1] >> dummy_op_end
