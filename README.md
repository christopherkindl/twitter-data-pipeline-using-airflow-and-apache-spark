# Data pipeline to process and analyse Twitter data in a distributed fashion using Apache Spark and Airflow in AWS environment

This repository shows the development of a scalable data pipeline in AWS using parallesisation techniques via Apache Spark on Amazon EMR and orchestrating workflows via Apache Airflow.

![alt text](https://github.com/christopherkindl/twitter-data-pipeline-using-airflow-and-apache-spark/blob/main/03_images/Architecture.png)


## Prerequisites

1. [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git) to clone the repository
2. [AWS account](https://aws.amazon.com/de/) to run pipeline in cloud environment
3. [Twitter developer account](https://developer.twitter.com/en/apply-for-access) with access to the standard API to fetch tweets
4. Database - in this project, the code to store the data in a database is written specially for a [PostgreSQL](link to doc) database
4. Webhosting with [WordPress](https://wordpress.org/support/article/how-to-install-wordpress/) to run the client application

&emsp;

## 0. Setup of the cloud environment in AWS

AWS provides [Amazon Managed Workflows for Apache Airflow (MWAA)](https://aws.amazon.com/de/blogs/aws/introducing-amazon-managed-workflows-for-apache-airflow-mwaa/) that makes it very easy to run Apache Airflow on AWS.

1. Go to the [MWAA console](https://console.aws.amazon.com/mwaa/home) and create a new environment

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

```
{
    "london-housing-webapp": {
        "bucket_name": "london-housing-webapp",
        "key1": "input/subway_station_information.csv" # where the geo information file for the API request is loaded from
        "output_key": "api_output/twitter_results.parquet", # where to store the scraped tweets
        "db_name": "postgres", # name of db
        "consumer_key": "{{TWITTER API KEY}}",
        "consumer_secret": "{{TWITTER API SECRET}}",
        "access_token": "{{TWITTER API ACCESS TOKEN}}",
        "access_token_secret": "{{{TWITTER API ACCESS TOKEN}}"
    }
}

```

A sample [JSON variabes](link to file) file is provided in the repository that contains all variables used in this project.

Airflow also allows to define connection objects. In our case, we need a connection to `AWS` itself (Airflow acts as an external system to AWS) and to our `database` in which the final results will be stored.

(Code snippt in json format)

## 1b. General settings in the Airflow DAG

Define basic information, such as schedule_interval or start_date in section `default_args` and `dag` of the DAG. This is also the place where we incorporate our variables and connection objects

```
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

