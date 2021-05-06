# Data pipeline to process and analyse Twitter data in a distributed fashion using Apache Spark and Airflow in AWS environment

This repository shows the development of a scalable data pipeline in [AWS](https://aws.amazon.com/de/) using parallelisation techniques via [Apache Spark](https://spark.apache.org/) on [Amazon EMR](https://aws.amazon.com/de/emr/) and orchestrating workflows via [Apache Airflow](https://airflow.apache.org/). The data analysis part consists of a simpl esentiment analysis using a rule-based approach and a topic analysis using word frequencies by applying common NLP techniques.  

The datapipeline is used for an existing [web application](https://subway.christopherkindl.com/) which allows enduser to analyse housing prices based on locations of subway stations. More precisely, users see the average housing price of properties that are within a radius of less than 1km of a particular subway station. Therefore, the new data pipeline shown in this repository is used to make the application richer and, thus, incorporate sentiment scoring and topics analysis to give users a better sense of the common mood and an indication of what type of milieu lives in a particular area.
 
The python-based web scraper using `BeautifulSoup` to fetch geo-specific housing prices from property website across London is provided in the [repository](https://github.com/christopherkindl/twitter-data-pipeline-using-airflow-and-apache-spark/tree/main/04_web_scraper) as well but will not be extensively discussed here.


![alt text](https://github.com/christopherkindl/twitter-data-pipeline-using-airflow-and-apache-spark/blob/main/03_images/Architecture.png)


## Prerequisites

1. [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git) to clone the repository
2. [AWS account](https://aws.amazon.com/de/) to run pipeline in cloud environment
3. [Twitter developer account](https://developer.twitter.com/en/apply-for-access) with access to the standard API to fetch tweets
4. Database - In this project, we use a [PostgreSQL](https://aws.amazon.com/de/rds/postgresql/what-is-postgresql/) database. Database related code snippets (e.g. uploading data to db) might differ when using other databases
4. Webhosting and domain with [WordPress](https://wordpress.org/support/article/how-to-install-wordpress/) to run the client application
5. [wpDataTables](https://wpdatatables.com/pricing/) - WordPress plugin to easily access databases and create production-ready tables and charts

&emsp;

## 0. Setup of the cloud environment in AWS

AWS provides [Amazon Managed Workflows for Apache Airflow (MWAA)](https://aws.amazon.com/de/blogs/aws/introducing-amazon-managed-workflows-for-apache-airflow-mwaa/) that makes it very easy to run Apache Airflow on AWS.

1. Go to the [MWAA console](https://console.aws.amazon.com/mwaa/home) and click `create a new environment` to follow the step-by-step configuration guide

2. Select an existing [S3 bucket](https://s3.console.aws.amazon.com/) or create a new one and define the path where the Airflow DAG (the script which executes all tasks you want to run for the data pipeline) should be loaded from. The bucket name must start with `airflow-`

3. Upload `requirements.txt` that contains our python libaries to run the Airflow DAG. AWS will install them via `pip install`. Hint: If your DAG runs on libaries that are not available in pip, you can upload a `plugins.zip` in which you can include your desired libaries as [Python wheels](https://medium.com/swlh/beginners-guide-to-create-python-wheel-7d45f8350a94)

4. Each environment runs in a [Amazon Virtual Private Cloud (VPC)](https://aws.amazon.com/de/vpc/) using private subnets in two [availability zones](https://aws.amazon.com/de/about-aws/global-infrastructure/regions_az/#Availability_Zones). AWS recommends to use a `private network` for the web server access. For simplicity, we select a `public network` which allows to log in over the internet. Lastly, we let MWAA create a new `security group`

5. For the environment class, we select `pw1.small` as it corresponds best to our DAG workload

6. Activate `Airflow task logs` using the default setting. This allows to have logging information which is especially helpful for debugging

7. Keep the `default settings`

8. Create a `new role` or use an existing one and complete the setup by clicking `create new environment`

&emsp;

## 1a. Variables and connections for the MWAA environment

MWAA provides variables to store and retrieve arbitrary content or settings as a simple key-value store within Airflow. They can be created directly from the user interface (Admin > Variables) or bulk uploaded via `JSON` files.

```JSON

{
    "london-housing-webapp": {
        "bucket_name": "london-housing-webapp",
        "key1": "input/subway_station_information.csv", 
        "output_key": "api_output/twitter_results.parquet",
        "db_name": "postgres",
        "consumer_key": "{{TWITTER API KEY}}",
        "consumer_secret": "{{TWITTER API SECRET}}",
        "access_token": "{{TWITTER API ACCESS TOKEN}}",
        "access_token_secret": "{{{TWITTER API ACCESS TOKEN}}"
    }
}

```

A sample [variabes file](https://github.com/christopherkindl/twitter-data-pipeline-using-airflow-and-apache-spark/blob/main/01_airflow/airflow_variables.json) is provided in the repository that contains all variables used in this project.

Airflow also allows to define connection objects. In our case, we need a connection to `AWS` itself (Airflow acts as an external system to AWS) and to our `database` in which the final results will be stored.

&emsp;

## 1b. General settings in the Airflow DAG

Define basic configuration information, such as `schedule_interval` or `start_date in section` `default_args` and `dag` of the DAG. This is also the place where we incorporate our variables and connection objects

```Python

default_args = {
    'start_date': datetime(2021, 3, 8),
    'owner': 'Airflow',
    'filestore_base': '/tmp/airflowtemp/',
    'email_on_failure': True,
    'email_on_retry': False,
    'aws_conn_id': 'aws_default_christopherkindl',
    'bucket_name': Variable.get('london-housing-webapp', deserialize_json=True)['bucket_name'],
    'postgres_conn_id': 'postgres_id_christopherkindl',
    'output_key': Variable.get('london-housing-webapp',deserialize_json=True)['output_key'],
    'db_name': Variable.get('london-housing-webapp', deserialize_json=True)['db_name']
}

dag = DAG('london-housing-webapp',
          description='fetch tweets via API, run sentiment and topic analysis via Spark, save results to PostgreSQL',
          schedule_interval='@weekly',
          catchup=False,
          default_args=default_args,
          max_active_runs=1)

```

&emsp;

## 2. Taks in the Airflow DAG

**Basic architecture**  

A typical Airflow DAG consists of different tasks that either fetch, transform or process data in various ways. Heavy data analysis tasks are not recommended to run within the MWAA environment due to its modest workload capacity. ML tasks are usally called via [Amazon SageMaker](https://aws.amazon.com/de/sagemaker/), whereas complex data analyses can be done in distributed fashion on [Amazon EMR](https://aws.amazon.com/de/emr/). In our case, we run the data analysis on an Amazon EMR cluster using [Apache Spark](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark.html) (via Python API PySpark).

We can either write customized functions (e.g. request data via Twitter API) or can make use of predefined modules which are usually there to trigger external activities (e.g. data analysis in Spark on Amazon EMR).

Example of a customized function which is then assigned to a `PythonOperator` to function as a task:

```Python

# custom function

def create_schema(**kwargs):
    '''
    1. connect to Postgre Database
    2. create schema and tables in which the final data will be stored
    3. execute query
    '''
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



# qualify as task

create_schema = PythonOperator(
    task_id='create_schema',
    provide_context=True,
    python_callable=create_schema,
    op_kwargs=default_args,
    dag=dag,

)


```

The custom function above creates schema and tables directly into the PostgreSQL database in which the final data will be uploaded to. Note how `op_kwargs = default_args` allows to interface with the general configuration information provided. 

&emsp;

**Indicating the order of the tasks** 

Tasks can be executed in sequence or simoutanesly if possible. The order can be indicated with the following example syntax:

```Python

start_data_pipeline >> create_schema >> get_twitter_data >> create_emr_cluster >> step_adder
step_adder >> step_checker >> terminate_emr_cluster >> summarised_data_lineage_spark >> save_result_to_postgres_db
save_result_to_postgres_db >> end_data_pipeline

```
&emsp;

**All tasks of the DAG**  

![alt text](https://github.com/christopherkindl/twitter-data-pipeline-using-airflow-and-apache-spark/blob/main/03_images/airflow_steps.jpg)

See the entire Airflow DAG code of this project [here](https://github.com/christopherkindl/twitter-data-pipeline-using-airflow-and-apache-spark/blob/main/01_airflow/Airflow_DAG.py)

&emsp;

## 3. Run Spark on Amazon EMR

**Permissions**  

Change [IAM policy](https://github.com/christopherkindl/twitter-data-pipeline-using-airflow-and-apache-spark/blob/main/IAM_policy_configuration.json) to the following setting to allow MWAA to interface with Amazon EMR. 

```JSON
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "elasticmapreduce:DescribeStep",
                "elasticmapreduce:AddJobFlowSteps",
                "elasticmapreduce:RunJobFlow"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": "iam:PassRole",
            "Resource": [
                "arn:aws:iam::{{AWS ID}}:role/EMR_DefaultRole",
                "arn:aws:iam::{{AWS ID}}:role/EMR_EC2_DefaultRole"
            ]
        }
    ]
}

```
&emsp;

**Motivation to use Spark for data analysis**

We want to set up an infrastructure that allows big data analysis in a distributed fashion. [Apache Spark](https://www.infoworld.com/article/3236869/what-is-apache-spark-the-big-data-platform-that-crushed-hadoop.html) as our big data framework of choice has the following two main advantages: First, Spark’s in-memory data engine can perform tasks very efficient due to parallelisation logic. Second, Spark’s developer-friendly API reduces much of the grunt work of distributed computing and can be accessed in various languages. In our case, we use [PySpark](https://pypi.org/project/pyspark/) which is a Python API to interface with Spark on a high-level. This means it is suitable for interacting with an existing cluster but does not contain tools to set up a new, standalone cluster.  
The parallelisation logic of a distributed architecture is the main driver to speed up processing and, thus, enable scalability. Using Spark’s DataFrame or Resilient Distributed Dataset (RDD) allows to distribute the data computation across a cluster. 

**Under the hood**

We use Amazon’s big data platform [EMR](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-what-is-emr.html) to run our Spark cluster. A Spark cluster can be characterised by a master node that serves as the central coordinator and worker nodes on which the tasks/jobs are executed (=parallelisation). It requires a distributed storage layer which is [HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html) (Hadoop Distributed File System) in our case. S3 object storage is used as our main data storage and HDFS as our intermediate temporary memory on which the script will access the Twitter data and write the results on it. Temporary means that the processed data to HDFS will disappear after termination of the cluster. The reason to use HDFS is because it is a lot faster than writing the results directly to the S3 bucket.  

**Interaction between Airflow and Amazon EMR**

Every step that will be made on the cluster will be triggered by our Airflow DAG: First, we create the Spark cluster by providing specific configuration details and launch Hadoop for the distributed data storage simultaneously. In terms of the cluster configuration, we use one master node and two worker nodes all running on a m5.xlarge [instance](https://aws.amazon.com/de/ec2/instance-types/) (16 GB RAM, 4 CPU cores) given the relatively small dataset size. Next, we trigger a bootstrap action to install non-standard python libraries ([vaderSentiment](https://pypi.org/project/vaderSentiment/), [NLTK](https://pypi.org/project/nltk/) for NLP pre-processing steps) on which the sentiment and topic analysis script is dependent on. The [file](https://github.com/christopherkindl/twitter-data-pipeline-using-airflow-and-apache-spark/blob/main/02_emr_spark_jobs/python-libraries.sh) is loaded from an S3 bucket and submitted in the form of a `bash` script. 

Airflow offers pre-defined modules to quickly interact with Amazon EMR. The example below shows how an Amazon EMR cluster with Spark (PySpark) and Hadoop application is created using `EmrCreateJobFlowOperator()`.   


```Python

# define configuration setting for EMR cluster

JOB_FLOW_OVERRIDES = {
    "Name": "sentiment_analysis",
    "ReleaseLabel": "emr-5.33.0",
    "LogUri": "s3n://{{ BUCKET_NAME}}/logs/", # define path where logging files should be saved
    "BootstrapActions": [
        {'Name': 'install python libraries',
                'ScriptBootstrapAction': {
                # path where the .sh file to install non-standard python libaries is loaded from
                'Path': 's3://{{ BUCKET_NAME}}/scripts/python-libraries.sh'} 
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
                    "PYSPARK_PYTHON": "/usr/bin/python3"
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
                "InstanceCount": 1, # one master node
            },
            {
                "Name": "Core - 2", 
                "Market": "SPOT", # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2, # two worker nodes
            },
        ],
        "Ec2SubnetId": "{{SUBNET_ID}}",
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False, # this lets us programmatically terminate the cluster
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}


# qualify it as an Airflow task
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES, # 
    aws_conn_id="aws_default_christopherkindl",
    emr_conn_id="emr_default_christopherkindl",
    dag=dag,
)

```

&emsp;

**Submitting Spark jobs**

We can submit our Spark job that contains the python file for the sentiment analysis as well as data movement steps. Here, the configuration `s3-dist-cp` allows us to transfer data within S3, or between HDFS and S3. The python file is loaded from the S3 bucket while the scraped Twitter data is moved from the S3 bucket to the HDFS for the sentiment analysis and vice versa once the analysis is done. The same procedure is applied to run the topic analysis. We add a step sensor that will periodically check if the last step is completed, skipped or terminated. After the step sensor identifies the completion of the sentiment analysis (e.g. moving final data from HDFS to S3 bucket), a final step is added to terminate the cluster. The last step is necessary as AWS operates on a pay-per-use model (EMR is usually billed per second) and leaving unneeded resources running is wasteful anyways.

**Hint:** The [pyspark scripts](https://github.com/christopherkindl/twitter-data-pipeline-using-airflow-and-apache-spark/tree/main/02_emr_spark_jobs) to run the analyses are not discussed in detail here. In-code comments should be sufficient to understand the concept of each analysis. We can start a Spark session and fetch the Twitter data as follows. 

```Python

# code snippet of sentiment_analysis.py

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, help="HDFS input", default="/twitter_results")
    parser.add_argument("--output", type=str, help="HDFS output", default="/output")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("SentimentAnalysis").getOrCreate()
    
    # refers to function defined above which can be found in the complete script
    sentiment_analysis(input_loc=args.input, output_loc=args.output)

```

To read more about Spark sessions and Spark contexts, check this [post](https://sparkbyexamples.com/spark/sparksession-vs-sparkcontext/).

&emsp;

The figure below summarises the tasks to set up the EMR environment and execute jobs in Spark followed by a code snippet that shows how Spark jobs/steps are defined and called in the Airflow DAG

![alt text](https://github.com/christopherkindl/twitter-data-pipeline-using-airflow-and-apache-spark/blob/main/03_images/EMR_Spark.jpg)


**Spark-specific jobs:**

```Python

# define spark jobs that are executed on Amazon EMR

SPARK_STEPS = [
    {
        "Name": "move raw data from S3 to HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://{{BUCKET_NAME}}/api_output/twitter_results.parquet", # refer to the path where the tweets are stored
                "--dest=/twitter_results", # define destination path in HDFS
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
                # refer to the path where the .py script for the data analysis is stored
                "s3://{{BUCKET_NAME}}/scripts/sentiment_analysis.py", 
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
                "--src=/output", # path to store results temporarily in HDFS
                "--dest=s3://{{BUCKET_NAME}}/sentiment/", # final destination in S3 Bucket
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
                # path where .py file for second analysis is stored
                "s3://{{BUCKET_NAME}}/scripts/topic_analysis.py", 
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
                "--dest=s3://{{BUCKET_NAME}}/topics/",
            ],
        },
    },
]

# qualify it as an Airflow task
step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default_christopherkindl",
    steps=SPARK_STEPS,
    dag=dag,
)
```


**Key Airflow modules to interface with Amazon EMR:**
- `EmrCreateJobFlowOperator()`: to create EMR cluster with desired applications on it
- `EmrAddStepsOperator()`: to define jobs 
- `EmrStepSensor()`: to watch steps
- `EmrTerminateJobFlowOperator()`: to terminate EMR cluster

&emsp;


## 4. Launch Airflow DAG

Upload the final Airflow DAG to the corresponding path as explaind in the MWAA environment setup guide. Go to the Airflow user interface and start the DAG by switching the button to `On` (**Hint:** use a date in the past to trigger the DAG immediately). 

Useful housekeeping things to know:
- Log files can be accessed through clicking on the colored status squares which appear in the [Tree view](https://airflow.apache.org/docs/apache-airflow/stable/ui.html) mode
- When spark steps are running, you can watch it in Amazon EMR (**AWS management console** > **EMR** > **Clusters**) directly and see how the steps are executed
- Log files of Spark jobs are not shown in the Airflow generated log files, they have to be enabled when configuring the EMR cluster by providing a S3 path (see the example in readme section **Interaction between Airflow and Amazon EMR**)

A more detailed description can be found here.

&emsp;

## 5. Connect database to web application

For simplification, this documentation does not cover the detailed development process of the website itself. Using the WordPress pluging [wpDataTables](https://wpdatatables.com/pricing/) allows us to easily access any common database (MySQL, PostgreSQL, etc). Apparently, noSQL databases are not supported.  

Once installed, you can connect to a database (**WordPress Website Admin Panel** > **wpDataTables** > **Settings** > **separate DB connection**) and run a query (**WordPress Website Admin Panel** > **wpDataTables** > **Create a Table/Chart**) that is automatically transformed into a table or chat:

![alt image](https://github.com/christopherkindl/twitter-data-pipeline-using-airflow-and-apache-spark/blob/main/03_images/wp_plugin.jpg)  
&emsp;

**Using views to avoid complex queries at client-side**  

To anticipate a better website performance, we avoid writing a complex query at client-side and, thus, create a view within the schema that already has both data sources (housing prices, sentiment data) combined. The topic analysis data has its own query due to its generalised form and is accessed directly since it does not require any transformation steps at the client-side. 

**Hint:** Views can be easily created using a database administration tool, such as [pgAdmin](https://www.pgadmin.org/)  

The figure below summarises the interaction between client-side and the database.  

![alt image](https://github.com/christopherkindl/twitter-data-pipeline-using-airflow-and-apache-spark/blob/main/03_images/web_application.jpg)


