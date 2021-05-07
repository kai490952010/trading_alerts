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

pg_hook = CustomPostgresHook(postgres_conn_id='postgres_default')

def create_dag(dag_id,
               schedule,
               symbol,
               granularity,
               bucket,
               prefix,
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

    def download_file(*args, **context):

        filepaths = context['ti'].xcom_pull(
            task_ids='get_list_from_s3_{:s}'.format(symbol),
            key="file_list")
        print(filepaths)
        for key in filepaths:
            logging.info("Downloading file: {:s}".format(key))
            client = boto3.client(
                's3',
                config=Config(signature_version=UNSIGNED)
            )
            obj = client.get_object(Bucket=bucket, Key=key)
            file = obj.get('Body').read()
            temp_file = tempfile.NamedTemporaryFile().name
            with open(temp_file, 'wb') as file_handle:
                file_handle.write(file)

            df = pd.read_csv(temp_file,
                        compression='zip', 
                        header=None, 
                        names=['open_time',
                              'open',
                              'high',
                              'low',
                              'close',
                              'volume',
                              'close_time',
                              'quote_av',
                              'trades',
                              'tb_base_av',
                              'tb_quote_av',
                              'ignore' ],
                          dtype={
                              'open_time': int,
                              'close_time': int
                          })
            pg_hook.write_pandas_df(
              df.to_json(),
              name='tickerdata_{:s}'.format(granularity),
              if_exists='append',
              index=False)

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args)

    with dag:
        t1 = PythonOperator(
            task_id='get_list_from_s3_{:s}'.format(symbol),
            python_callable=get_list_from_s3,
            provide_context=True)
        t2 = PythonOperator(
            task_id='download_file_{:s}'.format(symbol),
            python_callable=download_file,
            provide_context=True)
        t1 >> t2

    return dag


# get symbols which don't have historical data
# for symbol in symbols_need_historical_refresh:
for symbol in ['BTCUSDT']:
    logger = logging.getLogger("Historical refresh {}".format(symbol))
    dag_id = 'historical_data_refresh_{}'.format(str(symbol))
    granularity = '1m'
    bucket = "data.binance.vision"
    prefix = "data/spot/{frequency}/klines/{symbol}/{granularity}/"

    logger.info("Beginning historical refresh for {:s}".format(symbol))
    # run historical update if data is more than 1 mo old
    # TODO: find condition at which historical data should run
    default_args = {
                    'owner': 'airflow',
                    'start_date': datetime(2021, 1, 1)
                    }

    schedule = None

    globals()[dag_id] = create_dag(dag_id,
                                  schedule,
                                  symbol,
                                  granularity,
                                  bucket,
                                  prefix,
                                  default_args)



