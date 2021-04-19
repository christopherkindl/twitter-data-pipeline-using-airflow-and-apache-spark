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

# Configurations
BUCKET_NAME = 'london-housing-webapp'
s3_data = 'data/twitter_output.csv'
s3_script = 'scripts/sentiment_analysis.py'
s3_output = 'final_output'


JOB_FLOW_OVERRIDES = {
    "Name": "Sentiment Analysis",
    "ReleaseLabel": "emr-5.29.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}], # We want our EMR cluster to have HDFS and Spark
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"}, # by default EMR uses py2, change it to py3
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
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT", # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False, # this lets us programmatically terminate the cluster
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}


s3_clean = "final_output/"
SPARK_STEPS = [ # Note the params values are supplied to the operator
    {
        "Name": "Copy raw data from S3 to HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://{{ params.BUCKET_NAME }}/data",
                "--dest=/twitter",
            ],
        },
    },
    {
        "Name": "Run sentiment analysis",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script }}",
            ],
        },
    },
    {
        "Name": "Move final output from HDFS to S3",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=/output",
                "--dest=s3://{{ params.BUCKET_NAME }}/{{ params.s3_clean }}",
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
    'bucket_name': Variable.get('london-housing-webapp', deserialize_json=True)['bucket_name'],
    'postgres_conn_id': 'engineering_groupwork_carina', #change with your credentials
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'output_key': Variable.get('twitter_results',deserialize_json=True)['output_key'],
    'db_name': Variable.get("housing_db", deserialize_json=True)['db_name']
}

dag = DAG('london-housing-webapp',
          description='Test run to get flat file',
          schedule_interval='@daily',
          catchup=False,
          default_args=default_args,
          max_active_runs=1)


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
        "station" varchar(256),
        "latitude" numeric,
        "longitude" numeric
    );
    """

    cursor.execute(sql_queries)
    conn.commit()
    log.info("Created Schema and Table")


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
    #df_total = pd.DataFrame(columns=['id', 'tweet'])
    #df_total = pd.DataFrame(columns=['id', 'tweet'])

    log.info('df created')

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
    data_dict_csv= stations.to_csv(csv_buffer, index=False)

    # Save the pandas dataframe as a csv to s3
    s3 = s3.get_resource_type('s3')

    # Get the data type object from pandas dataframe, key and connection object to s3 bucket
    data = csv_buffer.getvalue()


    object = s3.Object(bucket_name, key)

    # Write the file to S3 bucket in specific path defined in key
    object.put(Body=data)

    log.info('Finished saving the scraped data to s3')


    return


# def save_to_s3(**kwargs):
#
#
#     #Establishing S3 connection
#     s3 = S3Hook(kwargs['aws_conn_id'])
#     key = kwargs['output_key']
#     bucket_name = kwargs['bucket_name']
#
#     # Get the task instance
#     task_instance = kwargs['ti']
#
#     # Get the output of the previous task
#     collected_data = task_instance.xcom_pull(
#         task_ids='get_flat_file_station_information')
#
#     log.info('xcom from collecting data task:{0}'.format(
#         collected_data))
#
#     # create dataframe for collected data
#     df = pd.DataFrame(collected_data)
#
#     # Prepare the file to send to s3
#     csv_buffer = io.StringIO()
#     df.to_csv(csv_buffer, index=False)
#
#     # Save the pandas dataframe as a csv to s3
#     s3 = s3.get_resource_type('s3')
#
#     # Get the data type object from pandas dataframe, key and connection object to s3 bucket
#     data = csv_buffer.getvalue()
#
#     print("Saving CSV file")
#     object = s3.Object(bucket_name, key)
#
#     # Write the file to S3 bucket in specific path defined in key
#     object.put(Body=data)
#
#     log.info('Finished saving the scraped tweets to s3')

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
    stations = pd.read_csv(io.StringIO(csv_bytes ))#, encoding='utf-8')

    log.info('passing data from S3 bucket')

    # Connect to the PostgreSQL database
    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    log.info('Initialised connection')

    log.info('Loading row by row into database')

    #Load the rows into the PostgresSQL database
    s = """INSERT INTO london_schema.stations(station, latitude, longitude) VALUES (%s, %s, %s)"""

    for index in range(len(stations)):
        obj = []

        obj.append([stations.Station[index],
                   stations.Latitude[index],
                   stations.Longitude[index]])

        cursor.executemany(s, obj)
        conn.commit()

    log.info('Finished saving the data to postgres database')

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

create_schema = PythonOperator(
    task_id='create_schema',
    python_callable=create_schema,
    op_kwargs=default_args,
    provide_context=True,
    dag=dag,
)

get_flat_file_station_information = PythonOperator(
    task_id='get_flat_file_station_information',
    python_callable=get_flat_file_station_information,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    op_kwargs=default_args,
    provide_context=True,
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

# Create an EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag,
)

# Add your steps to the EMR cluster
step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    params={ # these params are used to fill the paramterized values in SPARK_STEPS json
        "BUCKET_NAME": Variable.get('london-housing-webapp', deserialize_json=True)['bucket_name'],
        "s3_data": s3_data,
        "s3_script": s3_script,
        "s3_clean": s3_clean,
    },
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

# save_to_s3 = PythonOperator(
#     task_id='save_to_s3',
#     provide_context=True,
#     python_callable=save_to_s3,
#     op_kwargs=default_args,
#     dag=dag,
# )

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

create_emr_cluster >> terminate_emr_cluster
#create_schema >> get_flat_file_station_information >> create_emr_cluster >> add_steps >> watch_step >> terminate_emr_cluster >> save_result_to_postgres_db

#For Alternative method
# create_schema >> web_scraping_task_dexters >> s3_save_file_func
