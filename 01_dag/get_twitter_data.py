from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.hooks.base_hook import BaseHook
import boto3, json, pprint, requests, textwrap, time, logging, requests
import os
from datetime import datetime
from typing import Optional, Union

from typing import Iterable

import ast
import time
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.emr_hook import EmrHook
from botocore.exceptions import ClientError
from airflow.exceptions import AirflowException
from typing import Dict, List, Optional, Set, Any, Callable, Generator, Union

#from mwaalib.emr_submit_and_monitor_step import EmrSubmitAndMonitorStepOperator
#import mwaalib.workflow_lib as etlclient

from datetime import datetime
from datetime import timedelta
import logging
import pandas as pd
import numpy as np
import re
import requests
import json
import io
import os
import shlex
#import twint
import tweepy


log = logging.getLogger(__name__)


# =============================================================================
# 1. Set up the main configurations of the dag
# =============================================================================



JOB_FLOW_OVERRIDES = {
    "Name": "sentiment_analysis",
    "ReleaseLabel": "emr-5.33.0",
    "BootstrapActions": [
        {'Name': 'install python libraries',
                'ScriptBootstrapAction': {
                'Path': 's3://london-housing-webapp/scripts/python-libraries.sh'}
                            }
                        ],
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}], # We want our EMR cluster to have HDFS and Spark
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {
                    "PYSPARK_PYTHON": "/usr/bin/python3",
                    "spark.pyspark.virtualenv.enabled": "true",
                    "spark.pyspark.virtualenv.type":"native",
                    "spark.pyspark.virtualenv.bin.path":"/usr/bin/virtualenv"
                    #"spark.pyspark.virtualenv.enabled": "true"
                    },
                    #"spark.pyspark.virtualenv.enabled": "true", # by default EMR uses py2, change it to py3
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT", # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "Ec2SubnetId": "subnet-0427e49b255238212",
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False, # this lets us programmatically terminate the cluster
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    #"vpc_id" : :"vpc-06a31dd5f9ebde9ef"
    #"subnet_id" : "vpc-06a31dd5f9ebde9ef",
}
#
#
#
SPARK_STEPS = [ # Note the params values are supplied to the operator

    {
        "Name": "run test script",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://london-housing-webapp/scripts/sentiment_analysis.py",
            ],
        },
    },
]



default_args = {
    'start_date': datetime(2021, 3, 8),
    'owner': 'Airflow',
    'filestore_base': '/tmp/airflowtemp/',
    'email_on_failure': True,
    'email_on_retry': False,
    'aws_conn_id': 'aws_default_christopherkindl',
    'emr_conn_id' : 'emr_default_christopherkindl', # might change
    'bucket_name': Variable.get('london-housing-webapp', deserialize_json=True)['bucket_name'],
    'postgres_conn_id': 'engineering_groupwork_carina', #change with your credentials
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'output_key': Variable.get('twitter_results',deserialize_json=True)['output_key'],
    'db_name': Variable.get('housing_db', deserialize_json=True)['db_name']
}

dag = DAG('london-housing-webapp',
          description='Test runs',
          schedule_interval='@weekly',
          catchup=False,
          default_args=default_args,
          max_active_runs=1)

# region = etlclient.detect_running_region()
# etlclient.client(region_name=region)

# Creating schema if inexistant
def create_schema(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    log.info('Initialised connection') #change column types to float
    sql_queries = """

    CREATE SCHEMA IF NOT EXISTS london_schema;
    DROP TABLE IF EXISTS london_schema.stations;
    CREATE TABLE IF NOT EXISTS london_schema.stations(
        "tweets" varchar(256),
        "date" timestamp,
        "station" varchar(256)
    );
    """

    cursor.execute(sql_queries)
    conn.commit()
    log.info("Created Schema and Table")


def get_twitter_data(**kwargs):

    # twitter api credentials
    consumer_key = Variable.get('london-housing-webapp_twitter_api', deserialize_json=True)['consumer_key']
    consumer_secret = Variable.get('london-housing-webapp_twitter_api', deserialize_json=True)['consumer_secret']
    access_token = Variable.get('london-housing-webapp_twitter_api', deserialize_json=True)['access_token']
    access_token_secret = Variable.get('london-housing-webapp_twitter_api', deserialize_json=True)['access_token_secret']

    # assign credentials
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    log.info('credentials provided')

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

    log.info('station information file in final df format')

    # test twitter api with a test query
    # max number of tweets
    number_of_tweets = 5

    # max number of stations
    number_of_stations = 3

    # store search results as list items
    tweets = []
    station = []
    date = []

    log.info('about to run search query via Twitter API')
    # run query with geolocation information obtained from station flat file
    for index in range(len(stations[:number_of_stations])):
        for i in tweepy.Cursor(api.search, q = 'london', lang = 'en', geocode= \
                               str(stations['Latitude'][index])+','+str(stations['Longitude'][index])+',1km').\
            items(number_of_tweets):
            tweets.append(i.text)
            station.append(stations['Station'][index])
            date.append(i.created_at)

    log.info('query run')

    # create dataframe
    df = pd.DataFrame({'tweets': tweets, 'date':date, 'station': station})

    log.info('query results converted into df')

    # solve compatibility issues with notebooks and RunTime errors
    #import nest_asyncio
    #nest_asyncio.apply()

    # c = twint.Config()
    #
    # # search London tweets
    # c.Limit = 2
    # c.Search = "London"
    #
    # # save to df
    # c.Pandas = True
    #
    # # run
    # twint.run.Search(c)
    #
    # # create dataframe and assign to total dataframe
    # df = twint.storage.panda.Tweets_df[["id", "tweet"]]

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

    #df_total = df

    # #convert df into dict
    #data_dict = stations.to_dict('series')
    #log.info('dict created')

    #Establishing S3 connection
    s3 = S3Hook(kwargs['aws_conn_id'])
    key = Variable.get('twitter_output', deserialize_json=True)['output_key']
    bucket_name = kwargs['bucket_name']

    # Prepare the file to send to s3
    csv_buffer = io.StringIO()

    #Ensuring the CSV files treats "NAN" as null values
    data_csv= df.to_csv(csv_buffer, index=False)

    # Save the pandas dataframe as a csv to s3
    s3 = s3.get_resource_type('s3')

    # Get the data type object from pandas dataframe, key and connection object to s3 bucket
    data = csv_buffer.getvalue()


    object = s3.Object(bucket_name, key)

    # Write the file to S3 bucket in specific path defined in key
    object.put(Body=data)

    log.info('Finished saving the scraped twitter data to s3')


    return


# Saving Dexter file to postgreSQL database
def save_result_to_postgres_db(**kwargs):

    #Establishing connection to S3 bucket
    bucket_name = kwargs['bucket_name']
    key = Variable.get('twitter_output', deserialize_json=True)['output_key']
    s3 = S3Hook(kwargs['aws_conn_id'])
    log.info("Established connection to S3 bucket")


    # Get the task instance
    task_instance = kwargs['ti']
    print(task_instance)


    # Read the content of the key from the bucket
    csv_bytes = s3.read_key(key, bucket_name)
    # Read the CSV
    df = pd.read_csv(io.StringIO(csv_bytes ))#, encoding='utf-8')

    log.info('passing data from S3 bucket')

    # Connect to the PostgreSQL database
    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    log.info('Initialised connection')

    log.info('Loading row by row into database')

    #Load the rows into the PostgresSQL database
    s = """INSERT INTO london_schema.stations(tweets, date, station) VALUES (%s, %s, %s)"""

    for index in range(len(df)):
        obj = []

        obj.append([df.tweets[index],
                    df.date[index],
                    df.station[index]])

        cursor.executemany(s, obj)
        conn.commit()

    log.info('Finished saving the data to postgres database')

# =============================================================================
# 3. Set up the main configurations of the dag
# =============================================================================


# create an EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default_christopherkindl",
    emr_conn_id="emr_default_christopherkindl",
    dag=dag,
)

# Add your steps to the EMR cluster
step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default_christopherkindl",
    steps=SPARK_STEPS,
    # params={ # these params are used to fill the paramterized values in SPARK_STEPS json
    #     "BUCKET_NAME": Variable.get("london-housing-webapp", deserialize_json=True)["bucket_name"],
    #     "s3_data": s3_data,
    #     "s3_script": s3_script,
    #     "s3_clean": s3_clean,
    #},
    dag=dag,
)

last_step = len(SPARK_STEPS) - 1 # this value will let the sensor know the last step to watch
# wait for the steps to complete
step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_default_christopherkindl",
    dag=dag,
)

# Terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default_christopherkindl",
    dag=dag,
)

start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)
end_data_pipeline = DummyOperator(task_id = 'end_data_pipeline', dag=dag)




# =============================================================================
# 4. Indicating the order of the dags
# =============================================================================


start_data_pipeline >> create_emr_cluster >> step_adder >> step_checker >> terminate_emr_cluster >> end_data_pipeline
