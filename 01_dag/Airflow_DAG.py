# Airflow specific modules
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

# other modules
import boto3, json, pprint, requests, textwrap, time, logging
import os
from datetime import datetime
from typing import Optional, Union
from typing import Iterable
from datetime import datetime
from datetime import timedelta
import logging
import pandas as pd
import numpy as np
import re
import io
import shlex
import tweepy


log = logging.getLogger(__name__)


# =============================================================================
# 1a. Set up the main configurations for Amazon EMR (Spark, Hadoop)
# =============================================================================

# EMR configurations for Spark and Hadoop

JOB_FLOW_OVERRIDES = {
    "Name": "sentiment_analysis",
    "ReleaseLabel": "emr-5.33.0",
    "LogUri": "s3n://london-housing-webapp/logs/",
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
                    },
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
}

# define spark jobs that are executed on EMR create_emr_cluster

SPARK_STEPS = [
    {
        "Name": "move raw data from S3 to HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://london-housing-webapp/api_output/twitter_results.parquet",
                "--dest=/twitter_results",
            ],
        },
    },
    {
        "Name": "run sentiment analysis",
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
    {
        "Name": "move final result of sentiment analysis from HDFS to S3",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=/output",
                "--dest=s3://london-housing-webapp/sentiment/",
            ],
        },
    },
    {
        "Name": "run topics analysis",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://london-housing-webapp/scripts/topic_analysis.py",
            ],
        },
    },
    {
        "Name": "move final result of topic analysis from HDFS to S3",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=/output",
                "--dest=s3://london-housing-webapp/topics/",
            ],
        },
    },
]

# =============================================================================
# 1b. Set up the main configurations for DAG
# =============================================================================

default_args = {
    'start_date': datetime(2021, 3, 8),
    'owner': 'Airflow',
    'filestore_base': '/tmp/airflowtemp/',
    'email_on_failure': True,
    'email_on_retry': False,
    'aws_conn_id': 'aws_default_christopherkindl',
    'emr_conn_id' : 'emr_default_christopherkindl',
    'bucket_name': Variable.get('london-housing-webapp', deserialize_json=True)['bucket_name'],
    'postgres_conn_id': 'postgres_id_christopherkindl',
    'output_key': Variable.get('twitter_api',deserialize_json=True)['output_key'],
    'db_name': Variable.get('housing_db', deserialize_json=True)['db_name']
}

dag = DAG('london-housing-webapp',
          description='fetch tweets via API, run sentiment and topic analysis via Spark, save results to PostgreSQL',
          schedule_interval='@weekly',
          catchup=False,
          default_args=default_args,
          max_active_runs=1)


# =============================================================================
# 2. Define functions
# =============================================================================


def create_schema(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    log.info('initialised connection')
    sql_queries = """

    CREATE SCHEMA IF NOT EXISTS london_schema;
    CREATE TABLE IF NOT EXISTS london_schema.sentiment(
        "tweets" varchar,
        "date" timestamp,
        "station" varchar(256),
        "sentiment" numeric
    );

    CREATE TABLE IF NOT EXISTS london_schema.topics(
        "date" timestamp,
        "topics" varchar
    );

    CREATE TABLE IF NOT EXISTS london_schema.data_lineage(
        "batch_nr" numeric,
        "job_nr" numeric,
        "timestamp" timestamp,
        "step_airflow" varchar(256),
        "source" varchar(256),
        "destination" varchar(256)
    );
    """

    # execute query
    cursor.execute(sql_queries)
    conn.commit()
    log.info("created schema and table")


def get_twitter_data(**kwargs):

    # document step nr for data lineage
    job_nr = 1

    # get twitter api credentials
    consumer_key = Variable.get('london-housing-webapp_twitter_api', deserialize_json=True)['consumer_key']
    consumer_secret = Variable.get('london-housing-webapp_twitter_api', deserialize_json=True)['consumer_secret']
    access_token = Variable.get('london-housing-webapp_twitter_api', deserialize_json=True)['access_token']
    access_token_secret = Variable.get('london-housing-webapp_twitter_api', deserialize_json=True)['access_token_secret']

    log.info('credentials received')

    # assign credentials
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    log.info('credentials provided')

    # establish connection to S3 bucket
    bucket_name = kwargs['bucket_name']
    key = Variable.get('london-housing-webapp_get_csv', deserialize_json=True)['key1']
    s3 = S3Hook(kwargs['aws_conn_id'])
    log.info('established connection to S3 bucket')

    # get the task instance
    task_instance = kwargs['ti']
    print(task_instance)
    log.info('get the task instance')

    # read the content of the key from the bucket
    stations = s3.read_key(key, bucket_name)
    log.info('read the content')

    # read the CSV
    stations = pd.read_csv(io.StringIO(stations))
    log.info('read csv and transformed to dataframe')

    # only keep essential columns
    stations.drop(columns = ['Zone'], inplace = True)

    log.info('station information file in df format')

    # max number of tweets per station
    number_of_tweets = 10

    # max number of stations (for test purposes)
    number_of_stations = 40

    # store search results as list items
    tweets = []
    station = []
    date = []

    log.info('about to run search query via Twitter API')
    # run query with geolocation information obtained from station flat file
    # add 1km radius (incorporated in the geocode in the end)
    for index in range(len(stations[:number_of_stations])):
        for i in tweepy.Cursor(api.search, q = 'london', lang = 'en', tweet_mode='extended', geocode= \
                               str(stations['Latitude'][index])+','+str(stations['Longitude'][index])+',1km').\
            items(number_of_tweets):
            tweets.append(i.full_text)
            station.append(stations['Station'][index])
            date.append(i.created_at)

    log.info('query run')

    # create dataframe
    df = pd.DataFrame({'tweets': tweets, 'date':date, 'station': station})

    log.info('query results converted into df')

    # establish S3 connection
    s3 = S3Hook(kwargs['aws_conn_id'])
    key = Variable.get('twitter_api', deserialize_json=True)['output_key']
    bucket_name = kwargs['bucket_name']

    # prepare the file to send to s3
    parquet_buffer = io.BytesIO()
    data_parquet=df.to_parquet(parquet_buffer)

    # save the pandas dataframe as a parquet file to s3
    s3 = s3.get_resource_type('s3')

    # get the data type object, key and connection object to s3 bucket
    data = parquet_buffer.getvalue()
    object = s3.Object(bucket_name, key)

    # write the file to S3 bucket in specific path defined in key
    object.put(Body=data)

    log.info('Finished saving the scraped data to s3')

    # update data lineage information

    # connect to the PostgreSQL database
    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    log.info('Initialised connection')

    log.info('Loading row by row into database')

    s = """INSERT INTO london_schema.data_lineage(batch_nr, job_nr, timestamp, step_airflow, source, destination) VALUES (%s, %s, %s, %s, %s, %s)"""

    # assign information
    batch_nr=datetime.now().strftime('%Y%m%d')
    timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    step_airflow="get_twitter_data"
    source = "External: Twitter API"
    destination = 's3://' + bucket_name + '/' + key

    # create list of lists to transmit information to database
    obj = []
    obj.append([batch_nr,
                job_nr,
                timestamp,
                step_airflow,
                source,
                destination])

    # execute query
    cursor.executemany(s, obj)
    conn.commit()

    log.info('data lineage updated')

    return

def summarised_data_lineage_spark(**kwargs):

    # document step nr for data lineage
    job_nr = 2

    # get bucket name
    bucket_name = kwargs['bucket_name']

    # connect to the PostgreSQL database
    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    log.info('Initialised connection')

    log.info('Loading row by row into database')

    s = """INSERT INTO london_schema.data_lineage(batch_nr, job_nr, timestamp, step_airflow, source, destination) VALUES (%s, %s, %s, %s, %s, %s)"""

    # assign information
    batch_nr=datetime.now().strftime('%Y%m%d')
    timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    step_airflow="all_spark_jobs"
    source = 's3://' + bucket_name + '/' + Variable.get('twitter_api', deserialize_json=True)['output_key']
    destination = 's3://' + bucket_name + '/' + 'sentiment/' + ', ' + 's3://' + bucket_name + '/' + 'topics/'

    # create list of lists to transmit information to database
    obj = []
    obj.append([batch_nr,
                job_nr,
                timestamp,
                step_airflow,
                source,
                destination])

    # execute query
    cursor.executemany(s, obj)
    conn.commit()

    log.info('data lineage updated')

    return

# function to filter keys after "last date modified" in s3 bucket
def modified_date_key(bucket_name, key):
    s3 = S3Hook(aws_conn_id='aws_default_christopherkindl')

    response = s3.get_conn().head_object(Bucket=bucket_name, Key=key)
    datetime_value = response["LastModified"]

    return datetime_value


# saving twitter sentiment results to postgres database
def save_result_to_postgres_db(**kwargs):

    # document step nr for data lineage
    job_nr = 3

    # establish connection to S3 bucket
    bucket_name = kwargs['bucket_name']
    s3 = S3Hook(kwargs['aws_conn_id'])
    log.info("Established connection to S3 bucket")

    # list all keys in subpath sentiment/
    keys = s3.list_keys(bucket_name, prefix="sentiment/", delimiter="")

    # identify latest date ("last modified") in the S3 subfolder by using max function
    max_date = ""
    find_max = max([modified_date_key(bucket_name, key) for key in keys])
    log.info("identified value for latest date")

    # create empty variable to assign desired key
    key_sentiment = ""

    # search corresponding key for the identified max date
    for key in keys:
        datetime_value = modified_date_key(bucket_name, key)
        if datetime_value == find_max:
            key_sentiment = key
    log.info("identified key for latest modified file")
    log.info(key_sentiment)

    # access bucket
    s3 = s3.get_resource_type('s3')
    response = s3.Object(bucket_name, key_sentiment).get()
    bytes_object = response['Body'].read()

    # convert parquet file into dataframe
    df = pd.read_parquet(io.BytesIO(bytes_object))

    log.info('passing sentiment data from S3 bucket')

    # connect to the PostgreSQL database
    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    log.info('Initialised connection')

    log.info('Loading row by row into database')

    # load the rows into the PostgresSQL database
    s = """INSERT INTO london_schema.sentiment(tweets, date, station, sentiment) VALUES (%s, %s, %s, %s)"""


    # create list of lists to transmit results to database
    for index in range(len(df)):
        obj = []

        obj.append([df.tweets[index],
                    df.date[index],
                    df.station[index],
                    df.sentiment[index]])

    # execute query
        cursor.executemany(s, obj)
        conn.commit()

    log.info('Finished saving the sentiment data to postgres database')

    # transmit topic analysis results to database

    # establish connection to S3 bucket
    bucket_name = kwargs['bucket_name']
    s3 = S3Hook(kwargs['aws_conn_id'])
    log.info("Established connection to S3 bucket")

    keys = s3.list_keys(bucket_name, prefix="topics/", delimiter="")

    # identify latest date ("last modified") in the S3 subfolder by using max function
    max_date = ""
    find_max = max([modified_date_key(bucket_name, key) for key in keys])
    log.info("identified value for latest date")

    # create empty variable to assign desired key
    key_topics = ""

    # search corresponding key for the identified max date
    for key in keys:
        datetime_value = modified_date_key(bucket_name, key)
        if datetime_value == find_max:
            key_topics = key
    log.info("identified key for latest modified file")
    log.info(key_topics)

    # access bucket
    s3 = s3.get_resource_type('s3')
    response = s3.Object(bucket_name, key_topics).get()
    bytes_object = response['Body'].read()

    # convert parquet file into dataframe
    df = pd.read_parquet(io.BytesIO(bytes_object))
    log.info('passing topics analysis data from S3 bucket')

    log.info('Loading row by row into database')


    s = """INSERT INTO london_schema.topics(date, topics) VALUES (%s, %s)"""

    # create list of lists to transmit results to database
    for index in range(len(df)):
        obj = []

        obj.append([df.date[index],
                    df.topics[index]])

    # execute query
        cursor.executemany(s, obj)
        conn.commit()

    log.info('Finished saving the topic analysis data to postgres database')

    # update data lineage information

    s = """INSERT INTO london_schema.data_lineage(batch_nr, job_nr, timestamp, step_airflow, source, destination) VALUES (%s, %s, %s, %s, %s, %s)"""

    batch_nr=datetime.now().strftime('%Y%m%d')
    timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    step_airflow="save_result_to_postgres_db"
    source = 's3://' + bucket_name + '/' + key_sentiment + ', ' + 's3://' + bucket_name + '/' + key_topics
    destination = 'postgres: london_schema.sentiment, london_schema.topics'

    # create list of lists to transmit results to database
    obj = []
    obj.append([batch_nr,
                job_nr,
                timestamp,
                step_airflow,
                source,
                destination])

    # execute query
    cursor.executemany(s, obj)
    conn.commit()

    log.info('update data lineage information')

# =============================================================================
# 3. Create Operators
# =============================================================================

create_schema = PythonOperator(
    task_id='create_schema',
    provide_context=True,
    python_callable=create_schema,
    op_kwargs=default_args,
    dag=dag,

)

get_twitter_data = PythonOperator(
    task_id='get_twitter_data',
    provide_context=True,
    python_callable=get_twitter_data,
    op_kwargs=default_args,
    dag=dag,

)

save_result_to_postgres_db = PythonOperator(
    task_id='save_result_to_postgres_db',
    provide_context=True,
    python_callable=save_result_to_postgres_db,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    op_kwargs=default_args,
    dag=dag,

)

create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default_christopherkindl",
    emr_conn_id="emr_default_christopherkindl",
    dag=dag,
)


step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default_christopherkindl",
    steps=SPARK_STEPS,
    dag=dag,
)

last_step = len(SPARK_STEPS) - 1 # this value will let the sensor know the last step to watch


step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_default_christopherkindl",
    dag=dag,
)

# terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default_christopherkindl",
    dag=dag,
)

# use seperate python operator to summarise all spark steps for data lineage
summarised_data_lineage_spark = PythonOperator(
    task_id='summarised_data_lineage_spark',
    provide_context=True,
    python_callable=summarised_data_lineage_spark,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    op_kwargs=default_args,
    dag=dag,

)

start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)
end_data_pipeline = DummyOperator(task_id = "end_data_pipeline", dag=dag)



# =============================================================================
# 4. Indicating the order of the dags
# =============================================================================


start_data_pipeline >> create_schema >> get_twitter_data >> create_emr_cluster >> step_adder
step_adder >> step_checker >> terminate_emr_cluster >> summarised_data_lineage_spark >> save_result_to_postgres_db
save_result_to_postgres_db >> end_data_pipeline
