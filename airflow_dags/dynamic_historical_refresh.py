import os
import logging
import tempfile
from datetime import datetime

import boto3
from botocore import UNSIGNED
from botocore.client import Config
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from custom_postgreshook import CustomPostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

POSTGRES_CONN_ID = 'postgres_default'
pg_hook = CustomPostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
logger = logging.getLogger("Historical refresh")

def historical_refresh_dag(dag_id,
              schedule,
              symbol,
              granularity,
              bucket,
              prefix,
              symbol_id,
              default_args):

    # function to retrieve filepaths from an s3 bucket 
    def get_list_from_s3(*args, **context):
        updated_prefix = prefix.format(frequency='monthly',
            symbol=symbol,
            granularity=granularity)
        print('/'.join([bucket, updated_prefix]))


        filepaths = []
        bucket_obj = S3Hook('aws_default').get_bucket(bucket)

        for obj in bucket_obj.objects.filter(Prefix=updated_prefix):
            path, filename = os.path.split(obj.key)
            if filename.endswith('zip'):
                filepaths.append(obj.key)

        context['ti'].xcom_push(key="file_list", value=filepaths)
        print("Found {:d} files".format(len(filepaths)))


    def get_historical_data(*args, **context):

        filepaths = context['ti'].xcom_pull(
            task_ids='get_list_from_s3_{:s}'.format(symbol),
            key="file_list")
        for key in filepaths:
        #     logging.info("Downloading file: {:s}".format(key))
        #     client = boto3.client(
        #         's3',
        #         config=Config(signature_version=UNSIGNED)
        #     )
        #     obj = client.get_object(Bucket=bucket, Key=key)
        #     file = obj.get('Body').read()
        #     temp_file = tempfile.NamedTemporaryFile().name
        #     with open(temp_file, 'wb') as file_handle:
        #         file_handle.write(file)

        #     df = pd.read_csv(temp_file,
        #                 compression='zip', 
        #                 header=None, 
        #                 names=['open_time',
        #                       'open',
        #                       'high',
        #                       'low',
        #                       'close',
        #                       'volume',
        #                       'close_time',
        #                       'quote_av',
        #                       'trades',
        #                       'tb_base_av',
        #                       'tb_quote_av',
        #                       'ignore' ],
        #                   dtype={
        #                       'open_time': int,
        #                       'close_time': int
        #                   })
        #     df = df.drop('ignore', axis=1)
        #     df.loc[:, 'symbol_id'] = symbol_id

        #     pg_hook.write_pandas_df(
        #       df.to_json(),
        #       name='tickerdata_{:s}'.format(granularity),
        #       if_exists='append',
        #       index=False)
        pass

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args)

    with dag:
        t1 = PythonOperator(
            task_id='get_list_from_s3_{:s}'.format(symbol),
            python_callable=get_list_from_s3,
            provide_context=True)
        t2 = PythonOperator(
            task_id='get_historical_data_{:s}'.format(symbol),
            python_callable=get_historical_data,
            provide_context=True)

        SQL = """UPDATE symbols
            SET symbol_last_updated_at=timezone('utc', NOW())
            WHERE symbol_id = {:d}""".format(symbol_id)
        t3 = PostgresOperator(
            task_id = 'Set_{:s}_last_updated_at'.format(symbol),
            postgres_conn_id = POSTGRES_CONN_ID,
            sql = SQL
        )
        t1 >> t2 >> t3

    return dag

default_args = {
                'owner': 'airflow',
                'start_date': datetime(2021, 1, 1)
}

symbols_with_no_historic_data = """
    SELECT x.symbol_id, x.symbol_name
    FROM symbols x
    LEFT JOIN (SELECT symbol_id, max(close_time) last_close_time
      FROM tickerdata_1m
      GROUP BY 1) y ON x.symbol_id = y.symbol_id
    WHERE last_close_time IS NULL
"""
# symbols_with_no_historic_data = """
#     SELECT 21313 as symbol_id, 'SFPUSDT' as symbol_name
# """
symbol_list = pg_hook.get_pandas_df(symbols_with_no_historic_data).head(1)
logger.info("Getting historical data for {:d} symbols".format(symbol_list.shape[0]))

dag_list = []
for index, row in symbol_list.iterrows():
    symbol_id, symbol = row.loc['symbol_id'], row.loc['symbol_name']
    dag_id = 'historical_data_refresh_{}'.format(str(symbol))
    granularity = '1m'
    bucket = "data.binance.vision"
    prefix = "data/spot/{frequency}/klines/{symbol}/{granularity}/"
    schedule = "@once"

    logger.info("Beginning historical refresh for {:s}".format(symbol))
    dag = historical_refresh_dag(dag_id,
                                  schedule,
                                  symbol,
                                  granularity,
                                  bucket,
                                  prefix,
                                  symbol_id,
                                  default_args)
    globals()[dag_id] = dag
    print(type(dag), dag)
    dag_list.append(dag_id)