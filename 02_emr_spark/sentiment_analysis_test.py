# PySpark modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

import parser


# sentiment modules
# from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
# analyzer = SentimentIntensityAnalyzer()



# define function to get compounded sentiment score
# def apply_vader(sentence):
#     """
#     1. calculates positivity, negativity and neutrality score of sentence.
#     2. returns compounded sentiment score which represents the total of all subscores.
#     """
#     vs = analyzer.polarity_scores(sentence)
#     return float(round(vs.get('compound'), 2))


def sentiment_analysis(input_loc, output_loc):

    # read input
    df_raw = spark.read.option("header", True).parquet(input_loc)
    #df_raw = spark.read.option("header", True).parquet(input_loc, compression='gzip')

    # assign sentiment function as an user defined function
    # sentiment = udf(apply_vader)
    #
    # # perform sentiment analysis
    # df_clean = df_raw.withColumn('sentiment', sentiment(df_raw['tweets']))

    # output as parquet file
    #df_clean.write.mode("overwrite").parquet(output_loc)

    # output as csv file
    df_raw.write.mode("overwrite").csv(output_loc)

if __name__ == "__main__":
    #parser = argparse.ArgumentParser()
    #parser.add_argument("--input", type=str, help="HDFS input", default="/twitter")
    #parser.add_argument("--output", type=str, help="HDFS output", default="/output")
    input = "s3://london-housing-webapp/api_output/df_2.parquet"
    output = "s3://london-housing-webapp/00-wed/"
    #args = parser.parse_args()
    spark = SparkSession.builder.appName("SentimentAnalysis").getOrCreate()
    sentiment_analysis(input_loc=input, output_loc=output)
    #sentiment_analysis(input_loc=args.input, output_loc=args.output)
