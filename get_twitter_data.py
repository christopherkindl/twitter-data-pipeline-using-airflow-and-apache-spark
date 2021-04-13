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
import twint


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
    'output_key': Variable.get('twitter_results',deserialize_json=True)['output_key']
    #'db_name': Variable.get("housing_db", deserialize_json=True)['db_name']
}

dag = DAG('london-housing-webapp',
          description='Test run to get flat file',
          schedule_interval='@daily',
          catchup=False,
          default_args=default_args,
          max_active_runs=1)


def get_flat_file_station_information(**kwargs):

    import twint

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

    log.info('dropped unnecessary columns')


    #create dataframe
    df_total = pd.DataFrame(columns=['id', 'tweet'])

    # solve compatibility issues with notebooks and RunTime errors
    #import nest_asyncio
    #nest_asyncio.apply()

    c = twint.Config()

    # search London tweets
    c.Limit = 2
    c.Search = "London"

    # save to df
    c.Pandas = True

    # run
    twint.run.Search(c)

    # create dataframe and assign to total dataframe
    df = twint.storage.panda.Tweets_df[["id", "tweet"]]

    # # configure twint and fetch tweets from particular subway station
    # c = twint.Config()
    #
    # for index in range(len(stations[:5])):
    #
    #     # limit tweets to 20 (1 unit represents 20 tweets)
    #     c.Limit = 2
    #
    #     # hide console output
    #     c.Hide_output = True
    #
    #     # only scrape text tweets
    #     c.Images = False
    #     c.Videos = False
    #
    #     # scrape tweets from a radius of 1km around a particular subway station
    #     c.Geo= str(stations['Latitude'][index])+','+str(stations['Longitude'][index])+', 1km'
    #
    #     # only scrape English tweets
    #     c.Lang = "en"
    #
    #     # only tweets since last week
    #     ## make it dynamic so that it automatically calculates -7 days
    #     c.Since = '2020-08-01'
    #
    #     # save in df format
    #     c.Pandas = True
    #
    #     # run
    #     twint.run.Search(c)
    #
    #     # create dataframe and assign to total dataframe
    #     df = twint.storage.panda.Tweets_df[["id", "tweet"]]
    #
    #     print(len(twint.storage.panda.Tweets_df))
    #
    #     df_total = df_total.append(df)
    #
    #     print('Scraped tweets around '+str(stations['Station'][index]))

    df_total = df

    # #convert df into dict
    data_dict = df_total.to_dict('series')



    return data_dict


def save_to_s3(**kwargs):


    #Establishing S3 connection
    s3 = S3Hook(kwargs['aws_conn_id'])
    key = kwargs['output_key']
    bucket_name = kwargs['bucket_name']

    # Get the task instance
    task_instance = kwargs['ti']

    # Get the output of the previous task
    collected_data = task_instance.xcom_pull(
        task_ids='get_flat_file_station_information')

    log.info('xcom from collecting data task:{0}'.format(
        collected_data))

    # create dataframe for collected data
    df = pd.DataFrame(collected_data)

    # Prepare the file to send to s3
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    # Save the pandas dataframe as a csv to s3
    s3 = s3.get_resource_type('s3')

    # Get the data type object from pandas dataframe, key and connection object to s3 bucket
    data = csv_buffer.getvalue()

    print("Saving CSV file")
    object = s3.Object(bucket_name, key)

    # Write the file to S3 bucket in specific path defined in key
    object.put(Body=data)

    log.info('Finished saving the scraped tweets to s3')



# def lambda_preprocessing(**kwargs):
#     hook = AwsLambdaHook('twitterpreprocessing',
#                          region_name='eu-west-1',
#                          log_type='None',
#                          qualifier='$LATEST',
#                          invocation_type='RequestResponse',
#                          config=None,
#                          aws_conn_id='aws_default_christopherkindl')
#     response_1 = hook.invoke_lambda(payload='null')
#     print('Response--->', response_1)

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

save_to_s3 = PythonOperator(
    task_id='save_to_s3',
    provide_context=True,
    python_callable=save_to_s3,
    op_kwargs=default_args,
    dag=dag,
)

# lambda_preprocessing = PythonOperator(
#     task_id='lambda_preprocessing',
#     provide_context=True,
#     python_callable=lambda_preprocessing,
#     op_kwargs=default_args,
#     dag=dag,
# )


# =============================================================================
# 4. Indicating the order of the dags
# =============================================================================

get_flat_file_station_information >> save_to_s3

#For Alternative method
# create_schema >> web_scraping_task_dexters >> s3_save_file_func
