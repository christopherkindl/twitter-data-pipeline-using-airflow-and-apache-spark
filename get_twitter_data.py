from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta
import logging
import pandas as pd
import numpy as np
import re
import requests
import json
import io


log = logging.getLogger(__name__)


# =============================================================================
# 1. Set up the main configurations of the dag
# =============================================================================


default_args = {
    'start_date': datetime(2021, 3, 8),
    'owner': 'Airflow',
    'filestore_base': '/tmp/airflowtemp/',
    'email_on_failure': True,
    'email_on_retry': False,
    'aws_conn_id': 'aws_default_christopherkindl',
    'bucket_name': Variable.get('london-housing-webapp', deserialize_json=True)['bucket_name'],
    #'postgres_conn_id': 'engineering_groupwork_carina',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    #'db_name': Variable.get("housing_db", deserialize_json=True)['db_name']
}

dag = DAG('london-housing-webapp',
          description='Test run to get flat file',
          schedule_interval='@daily',
          catchup=False,
          default_args=default_args,
          max_active_runs=1)


def get_flat_file_station_information(**kwargs):

    # establishing connection to S3 bucket
    bucket_name = kwargs['bucket_name']
    key = Variable.get('london-housing-webapp_get_csv', deserialize_json=True)['key1']
    s3 = S3Hook(kwargs['aws_conn_id'])
    log.info('Established connection to S3 bucket')

    # get the task instance
    task_instance = kwargs['ti']
    print(task_instance)
    log.info('get the task instance')

    # read the content of the key from the bucket
    stations = s3.read_key(key, bucket_name)
    print("String read from s3", stations)
    log.info('Read the content..')

    # read the CSV
    stations = pd.read_csv(io.StringIO(stations))
    log.info('read csv and transformed to dataframe')

    # only keep essential columns
    stations.drop(columns = ['OS X', 'OS Y', 'Zone', 'Postcode'], inplace = True)

    log.info('droped unnecessary columns')


# =============================================================================
# 3. Set up the main configurations of the dag
# =============================================================================
get_flat_file_station_information = PythonOperator(
    task_id='get_flat_file_station_information',
    python_callable=get_flat_file_station_information,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    op_kwargs=default_args,
    provide_context=True,
    dag=dag,
)


# =============================================================================
# 4. Indicating the order of the dags
# =============================================================================

get_flat_file_station_information

#For Alternative method
# create_schema >> web_scraping_task_dexters >> s3_save_file_func
