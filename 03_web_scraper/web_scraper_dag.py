from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook

import pandas as pd
import numpy as np
import time
from bs4 import BeautifulSoup
import requests
import re
from re import sub
from decimal import Decimal
import io

from datetime import datetime
from datetime import timedelta
import logging


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
# 2. Create custom functions
# =============================================================================


# Webscraping Zoopla
def web_scraping(**kwargs):


    # Convert price string into a numerical value
    def to_num(price):
        value = Decimal(sub(r'[^\d.]', '', price))
        return float(value)

    def is_dropped(money):
        for i in range(len(money)):
            if(money[i] != '£' and money[i] != ',' and (not money[i].isdigit())):
                return True
        return False

    #set up the the scraper
    url = 'https://www.zoopla.co.uk/for-sale/property/london/?page_size=25&q=london&radius=0&results_sort=newest_listings&pn='

    map = {}
    id = 0

    #set max_pages to 2 for test purposes
    max_pages = 300

    # time.sleep(10)
    start = time.time()

    for p in range(max_pages):
        cur_url = url + str(p + 1)

        print("Scraping page: %d" % (p + 1))
        #print(cur_url)
        html_text = requests.get(cur_url).text
        soup = BeautifulSoup(html_text, 'lxml')

        ads = soup.find_all('div', class_ = 'css-wfndrn-StyledContent e2uk8e18')
        page_nav = soup.find_all('a', class_ = 'css-slm4qd-StyledPaginationLink eaoxhri5')

        if(len(page_nav) == 0):
            print("max page number: %d" % (p))
            # end = time.time()
            # print(end - start)
            break

        for k in range(len(ads)):
            ad = ads[k]

            #find link and ID ('identifier' in the link acts as a unique id for the ad)
            link = ad.find('a', class_ = 'e2uk8e4 css-gl9725-StyledLink-Link-FullCardLink e33dvwd0')

            #find section for address
            address = ad.find('p', class_ = 'css-wfe1rf-Text eczcs4p0').text

            #find price information
            price = ad.find('p', class_ = 'css-18tfumg-Text eczcs4p0').text

            # if the price is valid or not, if not we do not consider this ad
            if(is_dropped(price)): continue

            #find public transport information
            subway_section = ad.find('div', class_ = 'css-braguw-TransportWrapper e2uk8e28')
            subway_information = subway_section.find_all('p', class_ = 'css-wfe1rf-Text eczcs4p0')

            #skip ads that only contain information of train station
            outlier = subway_section.find('span', class_ = 'e1uy4ban0 css-10ibqwe-StyledIcon-Icon e15462ye0')
            if(outlier['data-testid'] == 'national_rail_station'): continue

            #find section for bedroom, bathroom and living room information (room numbers)
            feature_section = ad.find('div', class_ = 'css-58bgfg-WrapperFeatures e2uk8e15')

            #find all information available for room numbers
            category = feature_section.find_all('div', class_ = 'ejjz7ko0 css-l6ka86-Wrapper-IconAndText e3e3fzo1')

            #assign id
            ad_id = link['href'] #returns url snippet with identifier from the url
            ad_id= ad_id.split("?")[0] #split by '?' ans '/' and apply index to retain only identifier number
            ad_id= ad_id.split("/")[3]

            if(ad_id in map): continue
            map[ad_id] = {}

            #assign link
            link = 'https://www.zoopla.co.uk/' + link['href']
            map[ad_id]["link"] = link

            #assign address
            map[ad_id]["address"] = address

            #assign bedroom nr
            try:
                map[ad_id]["room_nr"] = category[0].text
            except IndexError:
            #insert None value if index is not found
                map[ad_id]["room_nr"] = 'None'
                #print("Feature not listed")

            #assign bathroom nr
            try:
                map[ad_id]["bath_nr"] = category[1].text
            except IndexError:
            #insert None value if index is not found
                map[ad_id]["bath_nr"] = 'None'
                #print("Feature not listed")

            #assign living room nr
            try:
                map[ad_id]["living_nr"] = category[2].text
            except IndexError:
            #insert None value if index is not found
                map[ad_id]["living_nr"] = 'None'
                #print("Feature not listed")

            #assign price
            map[ad_id]["price"] = to_num(price)

            #assign subway station and distance to it
            s = subway_information[0].text
            x = s.split(' miles ')
            if len(x) == 1: continue
            map[ad_id]["distance"] = float(x[0])
            map[ad_id]["subway_station"] = x[1]

    print("Scraping task finished")

    #transform to dict to list
    result = []
    cur_row = 0
    for cur_id in map.keys():
        link = map[cur_id]["link"]
        cur_price = map[cur_id]["price"]
        cur_bedroom = map[cur_id]["room_nr"]
        cur_bathroom = map[cur_id]["bath_nr"]
        cur_living = map[cur_id]["living_nr"]
        cur_address = map[cur_id]["address"]
        cur_distance = map[cur_id]["distance"]
        cur_subway_station = map[cur_id]["subway_station"]
        result.append([])
        result[cur_row].append(str(cur_id))
        result[cur_row].append(str(link))
        result[cur_row].append(str(cur_price))
        result[cur_row].append(str(cur_bedroom))
        result[cur_row].append(str(cur_bathroom))
        result[cur_row].append(str(cur_living))
        result[cur_row].append(str(cur_address))
        result[cur_row].append(str(cur_distance))
        result[cur_row].append(str(cur_subway_station))
        cur_row += 1

    #transform to dataframe
    df = pd.DataFrame(result, columns = ["ad_id", "link", "price", "bedrooms", "bathrooms", "living_rooms", "address", "distance", "subway_station"])
    df

    #Adjusting "None values to be NaN for df
    df = df.replace(r'None', np.NaN)
    # df = df.where(pd.notnull(df), None)
    log.info("Scraping succesful")


    #Establishing S3 connection
    s3 = S3Hook(kwargs['aws_conn_id'])
    bucket_name = kwargs['bucket_name']

    #creating timestamp

    # from datetime import datetime
    #
    # now = datetime.now() # current date and time

    # date_time = now.strftime("%Y_%m_%d_%HH_%Mm")
    # print("date and time:",date_time)

    #name of the file
    key = Variable.get("london-housing-webapp", deserialize_json=True)['key2']

    # Prepare the file to send to s3
    csv_buffer_zoopla = io.StringIO()
    #Ensuring the CSV files treats "NAN" as null values
    zoopla_csv=df.to_csv(csv_buffer_zoopla, index=False)

    # Save the pandas dataframe as a csv to s3
    s3 = s3.get_resource_type('s3')

    # Get the data type object from pandas dataframe, key and connection object to s3 bucket
    data = csv_buffer_zoopla.getvalue()

    print("Saving CSV file")
    object = s3.Object(bucket_name, key)

    # Write the file to S3 bucket in specific path defined in key
    object.put(Body=data)

    log.info('Finished saving the scraped data to s3')

    return

# Saving file to postgreSQL database
def save_result_to_postgres_db(**kwargs):


    #Establishing connection to S3 bucket
    bucket_name = kwargs['bucket_name']
    key = Variable.get("london-housing-webapp", deserialize_json=True)['key2']
    s3 = S3Hook(kwargs['aws_conn_id'])
    log.info("Established connection to S3 bucket")


    # Get the task instance
    task_instance = kwargs['ti']
    print(task_instance)


    # Read the content of the key from the bucket
    csv_bytes_zoopla = s3.read_key(key, bucket_name)
    # Read the CSV
    clean_zoopla = pd.read_csv(io.StringIO(csv_bytes_zoopla ))#, encoding='utf-8')

    log.info('passing Zoopla data from S3 bucket')

    # Connect to the PostgreSQL database
    pg_hook = PostgresHook(postgres_conn_id=kwargs["postgres_conn_id"], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    log.info('Initialised connection')

    #Required code for clearing an error related to int64
    import numpy
    from psycopg2.extensions import register_adapter, AsIs
    def addapt_numpy_float64(numpy_float64):
        return AsIs(numpy_float64)
    def addapt_numpy_int64(numpy_int64):
        return AsIs(numpy_int64)
    register_adapter(numpy.float64, addapt_numpy_float64)
    register_adapter(numpy.int64, addapt_numpy_int64)

    log.info('Loading row by row into database')
    # #Removing NaN values and converting to NULL:

    clean_zoopla = clean_zoopla.where(pd.notnull(clean_zoopla), None)

    s = """INSERT INTO schema_housing.zoopla( ad_id, link, price, bedrooms, bathrooms, living_rooms, address, distance, subway_station) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    for index in range(len(clean_zoopla)):
        obj = []

        obj.append([clean_zoopla.ad_id[index],
                   clean_zoopla.link[index],
                   clean_zoopla.price[index],
                   clean_zoopla.bedrooms[index],
                   clean_zoopla.bathrooms[index],
                   clean_zoopla.living_rooms[index],
                   clean_zoopla.address[index],
                   clean_zoopla.distance[index],
                   clean_zoopla.subway_station[index]])

        cursor.executemany(s, obj)
        conn.commit()

    log.info('Finished saving the scraped data to postgres database')

# =============================================================================
# 3. Create Operators
# =============================================================================

web_scraping = PythonOperator(
    task_id='web_scraping',
    provide_context=True,
    python_callable=web_scraping,
    op_kwargs=default_args,
    dag=dag,

)

# =============================================================================
# 4. Indicating the order of the dags
# =============================================================================


web_scraping >> save_result_to_postgres_db
