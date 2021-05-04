# Data pipeline to process and analyse Twitter data in a distributed fashion using Apache Spark and Airflow in AWS environment

This repository shows the development of a scalable data pipeline in AWS using parallesisation techniques via Apache Spark on Amazon EMR and orchestrating workflows via Apache Airflow.

![alt text](https://github.com/christopherkindl/twitter-data-pipeline-using-airflow-and-apache-spark/blob/main/03_images/Architecture.png)

## Prerequisites

1. [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git) to clone the repository
2. [AWS account](https://aws.amazon.com/de/) to run pipeline in cloud environment
3. Webhosting with [WordPress](https://wordpress.org/support/article/how-to-install-wordpress/) to run the client application

## 0. Setting up the cloud environment in AWS

AWS provides [Amazon Managed Workflows for Apache Airflow (MWAA)](https://aws.amazon.com/de/blogs/aws/introducing-amazon-managed-workflows-for-apache-airflow-mwaa/) that makes it very easy to run Apache Airflow on AWS.

1. Go to the [MWAA console](https://console.aws.amazon.com/mwaa/home) and create a new environment:

2. Select an existing [S3 bucket](https://s3.console.aws.amazon.com/) or create a new one and define the path where the Airflow DAG (the script which executes all tasks you want to run for the data pipeline) should be loaded from. The bucket name must start with `airflow-`

3. Upload `requirements.txt` that contains our python libaries to run the Airflow DAG. AWS will install them via `pip install`. Hint: If your DAG runs on libaries that are not available in pip, you can upload a `plugins.zip` in which you can include your desired libaries as [Python wheels](https://medium.com/swlh/beginners-guide-to-create-python-wheel-7d45f8350a94).

Each environment runs in a [Amazon Virtual Private Cloud (VPC)](https://aws.amazon.com/de/vpc/) using private subnets in two [availability zones](https://aws.amazon.com/de/about-aws/global-infrastructure/regions_az/#Availability_Zones). AWS recommends to use a `private network` for the web server access. For simplicity, we select a `public network` which allows to log in over the internet. Lastly, we let MWAA create a new security group.
