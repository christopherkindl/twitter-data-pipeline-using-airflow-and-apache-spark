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

# Configurations
# BUCKET_NAME = Variable.get('london-housing-webapp', deserialize_json=True)['bucket_name']
# s3_data = 'data/twitter_output.csv'
# s3_script = 'scripts/sentiment_analysis.py'
# s3_clean = 'final_output/'
#
#
# class EmrSubmitAndMonitorStepOperator(BaseOperator):
#     """
#     An operator that adds steps to an existing EMR job_flow.
#     :param job_flow_id: id of the JobFlow to add steps to. (templated)
#     :type job_flow_id: Optional[str]
#     :param job_flow_name: name of the JobFlow to add steps to. Use as an alternative to passing
#         job_flow_id. will search for id of JobFlow with matching name in one of the states in
#         param cluster_states. Exactly one cluster like this should exist or will fail. (templated)
#     :type job_flow_name: Optional[str]
#     :param cluster_states: Acceptable cluster states when searching for JobFlow id by job_flow_name.
#         (templated)
#     :type cluster_states: list
#     :param aws_conn_id: aws connection to uses
#     :type aws_conn_id: str
#     :param steps: boto3 style steps or reference to a steps file (must be '.json') to
#         be added to the jobflow. (templated)
#     :type steps: list|str
#     :param do_xcom_push: if True, job_flow_id is pushed to XCom with key job_flow_id.
#     :type do_xcom_push: bool
#     """
#
#     non_terminal_states = {"PENDING", "RUNNING"}
#     failed_states = {"FAILED", "CANCELLED", "INTERRUPTED", "CANCEL_PENDING"}
#     template_fields = [
#         "job_flow_id",
#         "job_flow_name",
#         "cluster_states",
#         "steps",
#         "wait_for_completion",
#     ]
#     template_ext = (".json",)
#     ui_color = "#f9c915"
#
#     @apply_defaults
#     def __init__(
#         self,
#         *,
#         job_flow_id: Optional[str] = None,
#         job_flow_name: Optional[str] = None,
#         cluster_states: Optional[List[str]] = None,
#         aws_conn_id: str = "aws_default",
#         check_interval: int = 30,
#         steps: Optional[Union[List[dict], str]] = None,
#         wait_for_completion: bool = True,
#         **kwargs,
#     ):
#         if kwargs.get("xcom_push") is not None:
#             raise AirflowException(
#                 "'xcom_push' was deprecated, use 'do_xcom_push' instead"
#             )
#         if not (job_flow_id is None) ^ (job_flow_name is None):
#             raise AirflowException(
#                 "Exactly one of job_flow_id or job_flow_name must be specified."
#             )
#         super().__init__(**kwargs)
#         cluster_states = cluster_states or []
#         steps = steps or []
#         self.aws_conn_id = aws_conn_id
#         self.job_flow_id = job_flow_id
#         self.job_flow_name = job_flow_name
#         self.cluster_states = cluster_states
#         self.steps = steps
#         self.wait_for_completion = wait_for_completion
#         self.check_interval = check_interval
#
#     def execute(self, context: Dict[str, Any]) -> List[str]:
#         emr_hook = EmrHook(aws_conn_id=self.aws_conn_id)
#
#         emr = emr_hook.get_conn()
#
#         job_flow_id = self.job_flow_id or emr_hook.get_cluster_id_by_name(
#             str(self.job_flow_name), self.cluster_states
#         )
#
#         if not job_flow_id:
#             raise AirflowException(f"No cluster found for name: {self.job_flow_name}")
#
#         if self.do_xcom_push:
#             context["ti"].xcom_push(key="job_flow_id", value=job_flow_id)
#
#         self.log.info("Adding steps to %s", job_flow_id)
#
#         # steps may arrive as a string representing a list
#         # e.g. if we used XCom or a file then: steps="[{ step1 }, { step2 }]"
#         steps = self.steps
#         if isinstance(steps, str):
#             steps = ast.literal_eval(steps)
#
#         response = emr.add_job_flow_steps(JobFlowId=job_flow_id, Steps=steps)
#
#         if not response["ResponseMetadata"]["HTTPStatusCode"] == 200:
#             raise AirflowException("Adding steps failed: %s" % response)
#         else:
#             # Assumption : ONly a single step is submitted each time.
#             step_ids = response["StepIds"]
#             step_id = step_ids[0]
#             if self.wait_for_completion:
#                 self.check_status(
#                     job_flow_id,
#                     step_id,
#                     self.describe_step,
#                     self.check_interval,
#                 )
#             self.log.info("Steps %s added to JobFlow", response["StepIds"])
#             return response["StepIds"]
#
#     def check_status(
#         self,
#         job_flow_id: str,
#         step_id: str,
#         describe_function: Callable,
#         check_interval: int,
#         max_ingestion_time: Optional[int] = None,
#         non_terminal_states: Optional[Set] = None,
#     ):
#         """
#         Check status of a EMR Step
#         :param job_flow_id: name of the Cluster to check status
#         :type job_flow_id: str
#         :param step_id: the Step Id
#             that points to the Job
#         :type step_id: str
#         :param describe_function: the function used to retrieve the status
#         :type describe_function: python callable
#         :param args: the arguments for the function
#         :param check_interval: the time interval in seconds which the operator
#             will check the status of any EMR job
#         :type check_interval: int
#         :param max_ingestion_time: the maximum ingestion time in seconds. Any
#             EMR jobs that run longer than this will fail. Setting this to
#             None implies no timeout for any EMR job.
#         :type max_ingestion_time: int
#         :param non_terminal_states: the set of nonterminal states
#         :type non_terminal_states: set
#         :return: response of describe call after job is done
#         """
#         if not non_terminal_states:
#             non_terminal_states = self.non_terminal_states
#
#         sec = 0
#         running = True
#
#         while running:
#             time.sleep(check_interval)
#             sec += check_interval
#
#             try:
#                 response = describe_function(job_flow_id, step_id)
#                 status = response["Step"]["Status"]["State"]
#                 self.log.info(
#                     "Job still running for %s seconds... " "current status is %s",
#                     sec,
#                     status,
#                 )
#             except KeyError:
#                 raise AirflowException("Could not get status of the EMR job")
#             except ClientError:
#                 raise AirflowException("AWS request failed, check logs for more info")
#
#             if status in non_terminal_states:
#                 running = True
#             elif status in self.failed_states:
#                 raise AirflowException(
#                     "EMR Step failed because %s"
#                     % response["Step"]["Status"]["FailureDetails"]["Message"]
#                 )
#             else:
#                 running = False
#
#             if max_ingestion_time and sec > max_ingestion_time:
#                 # ensure that the job gets killed if the max ingestion time is exceeded
#                 raise AirflowException(
#                     f"EMR job took more than {max_ingestion_time} seconds"
#                 )
#
#         self.log.info("EMR Job completed")
#         response = describe_function(job_flow_id, step_id)
#         return response
#
#     def describe_step(self, clusterid: str, stepid: str) -> dict:
#         """
#         Return the transform job info associated with the name
#         :param clusterid: EMR Cluster ID
#         :type stepid: str: StepID
#         :return: A dict contains all the transform job info
#         """
#         emr_hook = EmrHook(aws_conn_id=self.aws_conn_id)
#         emr = emr_hook.get_conn()
#         return emr.describe_step(ClusterId=clusterid, StepId=stepid)

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
    # {
    #     "Name": "Copy raw data from S3 to HDFS",
    #     "ActionOnFailure": "CANCEL_AND_WAIT",
    #     "HadoopJarStep": {
    #         "Jar": "command-runner.jar",
    #         "Args": [
    #             "s3-dist-cp",
    #             "--src=s3://london-housing-webapp/output/twitter_output.csv",
    #             "--dest=/twitter",
    #         ],
    #     },
    # },
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
    # {
    #     "Name": "Move final output from HDFS to S3",
    #     "ActionOnFailure": "CANCEL_AND_WAIT",
    #     "HadoopJarStep": {
    #         "Jar": "command-runner.jar",
    #         "Args": [
    #             "s3-dist-cp",
    #             "--src=/twitter",
    #             "--dest=s3://london-housing-webapp/final_output/",
    #             #{{ params.s3_clean }}",
    #         ],
    #     },
    # },
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

# # Creates an EMR cluster
# def create_emr(**kwargs):
#     cluster_id = etlclient.create_cluster(region_name=region, cluster_name='sentiment_analysis', num_core_nodes=2)
#     return cluster_id
#
# # Waits for the EMR cluster to be ready to accept jobs
# def wait_for_completion(**kwargs):
#     ti = kwargs['ti']
#     cluster_id = ti.xcom_pull(task_ids='create_cluster')
#     etlclient.wait_for_cluster_creation(cluster_id)
#
# # Terminates the EMR cluster
# def terminate_emr(**kwargs):
#     ti = kwargs['ti']
#     cluster_id = ti.xcom_pull(task_ids='create_cluster')
#     etlclient.terminate_cluster(cluster_id)
#
# # Monitors Step function execution
# def monitor_step_function(**kwargs):
#     ti = kwargs['ti']
#     execution_arn = ti.xcom_pull(task_ids='trigger_stepfunctions')
#     response = etlclient.monitor_stepfunction_run(execution_arn)
#     return response
#
# # Starts the Step Function
# def trigger_step_function(**kwargs):
#     # get the Step function arn
#     statemachine_arn = kwargs['stateMachineArn']
#     sfn_execution_arn = etlclient.start_execution(statemachine_arn)
#     return sfn_execution_arn

# =============================================================================
# CHANGE LATER
# =============================================================================

# class etlclient(BaseOperator):
#     import boto3, json, pprint, requests, textwrap, time, logging, requests
#     import os
#     from datetime import datetime
#     from typing import Optional, Union
#
#     sfn_non_terminal_states = {"RUNNING"}
#     sfn_failed_states = {"FAILED", "TIMED_OUT", "ABORTED"}
#
#     def detect_running_region():
#         """Dynamically determine the region from a running MWAA Setup ."""
#         easy_checks = [
#             # check if set through ENV vars
#             os.environ.get('AWS_REGION'),
#             os.environ.get('AWS_DEFAULT_REGION'),
#             # else check if set in config or in boto already
#             boto3.DEFAULT_SESSION.region_name if boto3.DEFAULT_SESSION else None,
#             boto3.Session().region_name,
#         ]
#         for region in easy_checks:
#             if region:
#                 return region
#         # else Assuming Airflow is running in an EC2 environment
#         # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-identity-documents.html
#         r = requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document")
#         response_json = r.json()
#         return response_json.get('region')
#
#     def get_region():
#         return 'us-west-2'
#
#     def client(region_name):
#         global emr
#         emr = boto3.client('emr', region_name=region_name)
#         global sfn
#         sfn = boto3.client('stepfunctions', region_name=region_name)
#         global ssm
#         ssm = boto3.client('ssm', region_name=region_name)
#
#
#     def create_cluster(region_name, cluster_name='mwaa-emr-' + str(datetime.now()), release_label='emr-5.31.0',master_instance_type='m5.xlarge', num_core_nodes=2, core_node_instance_type='m5.xlarge'):
#         cluster_response = emr.run_job_flow(
#             Name=cluster_name,
#             ReleaseLabel=release_label,
#             Instances={
#                 'InstanceGroups': [
#                     {
#                         'Name': "Master nodes",
#                         'Market': 'ON_DEMAND',
#                         'InstanceRole': 'MASTER',
#                         'InstanceType': master_instance_type,
#                         'InstanceCount': 1
#                     },
#                     {
#                         'Name': "Slave nodes",
#                         'Market': 'ON_DEMAND',
#                         'InstanceRole': 'CORE',
#                         'InstanceType': core_node_instance_type,
#                         'InstanceCount': num_core_nodes
#                     }
#                 ],
#                 'KeepJobFlowAliveWhenNoSteps': True
#             },
#             VisibleToAllUsers=True,
#             JobFlowRole='EMR_EC2_DefaultRole',
#             ServiceRole='EMR_DefaultRole',
#             Applications=[
#                 { 'Name': 'hadoop' },
#                 { 'Name': 'spark' }
#             ]
#         )
#         return cluster_response['JobFlowId']
#
#
#     def wait_for_cluster_creation(cluster_id):
#         emr.get_waiter('cluster_running').wait(ClusterId=cluster_id)
#
#
#     def terminate_cluster(cluster_id):
#         emr.terminate_job_flows(JobFlowIds=[cluster_id])
#
#     def get_demo_bucket_name():
#         demo_bucket=ssm.get_parameter(Name="/mwaa/S3/DemoBucket")['Parameter']['Value']
#         return demo_bucket
#
#     def get_stepfunction_arn():
#         sfn_arn=ssm.get_parameter(Name="/mwaa/sfn/movielens")['Parameter']['Value']
#         return sfn_arn
#
#     def start_execution(
#         state_machine_arn: str,
#         name: Optional[str] = None,
#         state_machine_input: Union[dict, str, None] = None,
#     ) -> str:
#         execution_args = {'stateMachineArn': state_machine_arn}
#         if name is not None:
#             execution_args['name'] = name
#         if state_machine_input is not None:
#             if isinstance(state_machine_input, str):
#                 execution_args['input'] = state_machine_input
#             elif isinstance(state_machine_input, dict):
#                 execution_args['input'] = json.dumps(state_machine_input)
#
#         logging.info('Executing Step Function State Machine: %s', state_machine_arn)
#
#         response = sfn.start_execution(**execution_args)
#         logging.info('Execution arn: %s', response)
#         return response.get('executionArn')
#
#     def monitor_stepfunction_run(execution_arn):
#         sfn.describe_execution(executionArn=execution_arn)
#         sec = 0
#         running = True
#         check_interval = 30
#
#         while running:
#             time.sleep(30)
#             sec += check_interval
#
#             try:
#                 response = sfn.describe_execution(executionArn=execution_arn)
#                 status = response["status"]
#                 logging.info(
#                     "Step function still running for %s seconds... " "current status is %s",
#                     sec,
#                     status,
#                 )
#             except KeyError:
#                 raise AirflowException("Could not get status of the Step function workflow")
#             except ClientError:
#                 raise AirflowException("AWS request failed, check logs for more info")
#
#             if status in sfn_non_terminal_states:
#                 running = True
#             elif status in sfn_failed_states:
#                 raise AirflowException(
#                     "Step function failed.Check Step functions log for details"
#                 )
#             else:
#                 running = False
#         logging.info("Step function workflow completed")
#         response = sfn.describe_execution(executionArn=execution_arn)
#         return response.get('executionArn')
#
# # Creates an EMR cluster
# def create_emr(**kwargs):
#     cluster_id = etlclient.create_cluster(region_name=region, cluster_name='mwaa_emr_cluster', num_core_nodes=2)
#     return cluster_id
#
#
# # Waits for the EMR cluster to be ready to accept jobs
# def wait_for_completion(**kwargs):
#     ti = kwargs['ti']
#     cluster_id = ti.xcom_pull(task_ids='create_cluster')
#     etlclient.wait_for_cluster_creation(cluster_id)
#
#
# # Terminates the EMR cluster
# def terminate_emr(**kwargs):
#     ti = kwargs['ti']
#     cluster_id = ti.xcom_pull(task_ids='create_cluster')
#     etlclient.terminate_cluster(cluster_id)
#
# # Monitors Step function execution
# def monitor_step_function(**kwargs):
#     ti = kwargs['ti']
#     execution_arn = ti.xcom_pull(task_ids='trigger_stepfunctions')
#     response = etlclient.monitor_stepfunction_run(execution_arn)
#     return response
#
# # Starts the Step Function
# def trigger_step_function(**kwargs):
#     # get the Step function arn
#     statemachine_arn = kwargs['stateMachineArn']
#     sfn_execution_arn = etlclient.start_execution(statemachine_arn)
#     return sfn_execution_arn
# =============================================================================
# 3. Set up the main configurations of the dag
# =============================================================================

# create_schema = PythonOperator(
#     task_id='create_schema',
#     python_callable=create_schema,
#     op_kwargs=default_args,
#     provide_context=True,
#     dag=dag,
# )
#
# get_twitter_data = PythonOperator(
#     task_id='get_twitter_data',
#     python_callable=get_twitter_data,
#     trigger_rule=TriggerRule.ALL_SUCCESS,
#     op_kwargs=default_args,
#     provide_context=True,
#     dag=dag,
# )
#
# save_result_to_postgres_db = PythonOperator(
#     task_id='save_result_to_postgres_db',
#     provide_context=True,
#     python_callable=save_result_to_postgres_db,
#     trigger_rule=TriggerRule.ALL_SUCCESS,
#     op_kwargs=default_args,
#     dag=dag,
#
# )

# create an EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default_christopherkindl",
    emr_conn_id="emr_default_christopherkindl",
    dag=dag,
)
#
# Define the individual tasks using Python Operators
# create_cluster = PythonOperator(
#     task_id='create_cluster',
#     python_callable=create_emr,
#     dag=dag)
# #
# # wait_for_cluster_completion = PythonOperator(
# #     task_id='wait_for_cluster_completion',
# #     python_callable=wait_for_completion,
# #     dag=dag)
# #
# extract_load_data = EmrSubmitAndMonitorStepOperator(
#     task_id="extract_load_data",
#     trigger_rule="one_success",
#     steps=[
#         {
#             "Name": "extract_load_data",
#             "ActionOnFailure": "CONTINUE",
#             "HadoopJarStep": {
#                 "Jar": "command-runner.jar",
#                 "Args": [
#                     "/bin/bash",
#                     "-c",
#                     "wget s3://london-housing-webapp/output/twitter_output.csv",
#                     #"wget http://files.grouplens.org/datasets/movielens/ml-latest.zip && unzip ml-latest.zip && aws s3 cp ml-latest s3://{}/raw --recursive".format(
#                     #    etlclient.get_demo_bucket_name()),
#                 ],
#             },
#         }
#     ],
#     job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
#     aws_conn_id="aws_default_christopherkindl",
#     check_interval=int("30"),
#     job_name="extract_load_data",
#     dag=dag
# )
# #
# preprocess_movies = EmrSubmitAndMonitorStepOperator(
#     task_id="preprocess_movies", #change name later
#     steps=[
#         {
#             "Name": "preprocess_movies",
#             "ActionOnFailure": "CONTINUE",
#             "HadoopJarStep": {
#                 "Jar": "command-runner.jar",
#                 "Args": [
#                     "spark-submit",
#                     "--conf",
#                     "deploy-mode=client",
#                     "s3://london-housing-webapp/scripts/sentiment_analysis_test.py",
#                 ],
#             },
#         }
#     ],
#     job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
#     aws_conn_id="aws_default_christopherkindl",
#     check_interval=int("30"),
#     job_name="preprocess_movies", #change name later
#     dag=dag
# )
# # #
# terminate_cluster = PythonOperator(
#     task_id='terminate_cluster',
#     python_callable=terminate_emr,
#     trigger_rule='all_done',
#     dag=dag)
# # #
# trigger_sfn = PythonOperator(
#     task_id='trigger_stepfunctions',
#     python_callable=trigger_step_function,
#     op_kwargs={'stateMachineArn': etlclient.get_stepfunction_arn()},
#     dag=dag)
# # #
# monitor_sfn = PythonOperator(
#     task_id='monitor_stepfunctions',
#     python_callable=monitor_step_function,
#     dag=dag)
#
# # use existing
# #j-1KZF8WQTW74JC
#
#
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
# #
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
# #
# Terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default_christopherkindl",
    dag=dag,
)

start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)
end_data_pipeline = DummyOperator(task_id = 'end_data_pipeline', dag=dag)

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

# test
# construct the DAG by setting the dependencies
start_data_pipeline >> create_emr_cluster >> step_adder >> step_checker >> terminate_emr_cluster >> end_data_pipeline

#create_schema >> get_twitter_data >> save_result_to_postgres_db
# create_cluster >> wait_for_cluster_completion
# wait_for_cluster_completion >> extract_load_data
# extract_load_data >> preprocess_movies >> terminate_cluster
# terminate_cluster >> trigger_sfn >> monitor_sfn
#create_emr_cluster >> step_adder >> step_checker >> terminate_emr_cluster


#create_schema >> get_flat_file_station_information >> create_emr_cluster >> add_steps >> watch_step >> terminate_emr_cluster >> save_result_to_postgres_db

#For Alternative method
# create_schema >> web_scraping_task_dexters >> s3_save_file_func
