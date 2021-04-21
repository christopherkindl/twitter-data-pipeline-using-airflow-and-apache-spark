# pyspark
import argparse

from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import array_contains

# sentiment analysis
from textblob import TextBlob
from pyspark.sql.functions import udf




# function to run sentiment analysis
def sentiment_analysis(input_loc, output_loc):
    # function to set up sentiment analysis logic
    def apply_blob(sentence):
        temp = TextBlob(sentence).sentiment[0]
        if temp == 0.0:
            return 0.0 # Neutral
        elif temp >= 0.0:
            return 1.0 # Positive
        else:
            return 2.0 # Negative

    # read input
    df_raw = spark.read.option('header', True).csv(input_loc)

    # assign sentiment function as user defined function
    sentiment = udf(apply_blob)

    # apply sentiment function to all tweets
    df_clean = df_raw.withColumn('sentiment', sentiment(df_raw['tweets']))

    # save output as csv
    df_clean.write.csv(output_loc)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=str,
                        help='HDFS input', default='/twitter')
    parser.add_argument('--output', type=str,
                        help='HDFS output', default='/output')
    args = parser.parse_args()
    spark = SparkSession.builder.appName(
        'Sentiment Analysis').getOrCreate()
    sentiment_analysis(input_loc=args.input, output_loc=args.output)
