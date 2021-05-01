# import pyspark modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lower, col, regexp_replace, concat_ws, collect_list
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from nltk.stem.snowball import SnowballStemmer
from pyspark.sql.types import StructType, ArrayType, StringType

# standard modules
import re
from datetime import datetime
import argparse



def topic_analysis(input_loc, output_loc):

    # read input
    df_raw = spark.read.option("header", True).parquet(input_loc)
    #df_raw = spark.read.option("header", True).parquet(input_loc, compression='gzip')

    # lowercase text and remove special characters
    df_raw_lw = df_raw.select("date", "station", (lower(regexp_replace('tweets', "(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", "")).alias('tweets')))
    df_raw_rm = df_raw_lw.select((regexp_replace('tweets',' +', ' ').alias('tweets')), "date", "station")


    # tokenize text
    tokenizer = Tokenizer(inputCol="tweets", outputCol="tweets_token")
    df_tokens = tokenizer.transform(df_raw_rm).select("tweets_token", "date", "station")

    # remove stop words
    remover = StopWordsRemover(inputCol="tweets_token", outputCol="tweets_sw_removed")#, stopWords=stopwordList)
    df_sw = remover.transform(df_tokens).select("tweets_sw_removed", "date", "station")

    # stemming
    stemmer = SnowballStemmer(language='english')
    stemmer_udf = udf(lambda tokens: [stemmer.stem(token) for token in tokens], ArrayType(StringType()))
    df_stemmed = df_sw.withColumn("tweets", stemmer_udf("tweets_sw_removed")).select("tweets", "date", "station")

    # create string from list of strings
    join_udf = udf(lambda x: ",".join(x))
    df_clean = df_stemmed.withColumn("tweets", join_udf(col("tweets")))

    # group by station and join tweets together
    df_tmp = df_clean.select('tweets', 'station').groupby('station').agg(concat_ws(" ", collect_list("tweets")).alias("tweets"))

    # set up bag of words computation
    bow = df_tmp.rdd\
        .filter(lambda x: x.tweets)\
        .map( lambda x: x.tweets.replace(',',' ').replace('.',' ').replace('-',' '))\
        .flatMap(lambda x: x.split())\
        .map(lambda x: (x, 1))

    # run bag of words by reduceByKey
    bow_tmp = bow.reduceByKey(lambda x,y:x+y)

    # show top 5 words
    bow_sorted = bow_tmp.takeOrdered(10,lambda a: -a[1])

    # convert list of topics into single string
    topics = ""

    for index in range(len(bow_sorted)):
        if bow_sorted[index][0] != 'london':
            topics += bow_sorted[index][0]
            topics += ", "

    # create dataframe and add current date
    columns = ["date","topics"]
    curr_date = datetime.now().strftime('%Y-%m-%d')
    data = [(curr_date, topics)]
    df_final = spark.createDataFrame(data).toDF(*columns)

    # output as csv file
    df_final.write.mode("overwrite").csv(output_loc)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, help="HDFS input", default="/twitter_results")
    parser.add_argument("--output", type=str, help="HDFS output", default="/output")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("TopicAnalysis").getOrCreate()
    topic_analysis(input_loc=args.input, output_loc=args.output)
