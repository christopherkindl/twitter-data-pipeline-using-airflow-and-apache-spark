# Data pipeline to process and analyse Twitter data in a distributed fashion using Apache Spark and Airflow in AWS environment

This repository shows the development of a scalable data pipeline in AWS using parallesisation techniques via Apache Spark on Amazon EMR and orchestrating workflows via Apache Airflow.

![alt text](https://github.com/christopherkindl/twitter-data-pipeline-using-airflow-and-apache-spark/blob/main/03_images/Architecture.png)

## Prerequisites

1. [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git) to clone the repository
2. [AWS account](https://aws.amazon.com/de/) to run pipeline in cloud environment
3. Webhosting with [WordPress](https://wordpress.org/support/article/how-to-install-wordpress/) to run the client application

## 0. Setting up the cloud environment in AWS

AWS provides [Amazon Managed Workflows for Apache Airflow (MWAA)](https://aws.amazon.com/de/blogs/aws/introducing-amazon-managed-workflows-for-apache-airflow-mwaa/) that makes it very easy to run Apache Airflow on AWS.

Go to the [MWAA console](https://console.aws.amazon.com/mwaa/home) and create a new environment:




Select an existing [S3](https://s3.console.aws.amazon.com/) bucket or create a new one and define the path where the Airflow DAG (the script which executes all tasks you want to run for the data pipeline) should be loaded from. The bucket name must start with `airflow-`

