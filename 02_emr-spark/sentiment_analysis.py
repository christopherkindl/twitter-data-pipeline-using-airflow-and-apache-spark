# PySpark modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
#from textblob import TextBlob

# sentiment modules
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
analyzer = SentimentIntensityAnalyzer()



if __name__ == "__main__":
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--input', type=str,
    #                     help='HDFS input', default='/twitter')
    # parser.add_argument('--output', type=str,
    #                     help='HDFS output', default='/output')
    # args = parser.parse_args()

    # start spark session
    spark = SparkSession.builder.appName('SentimentAnalysis').getOrCreate()

    # define function to get compounded sentiment score
    def apply_vader(sentence):
        """
        calculates positivity, negativity and neutrality score of sentence
        returns compounded sentiment score which represents the total of all subscores
        """
        vs = analyzer.polarity_scores(sentence)
        return round(vs.get('compound'), 2)

    # assign sentiment function as an user defined function
    sentiment = udf(apply_vader)

    # read csv file as spark df
    df_raw = spark.read.option('header', True).csv('s3://london-housing-webapp/twitter_output.csv')

    # apply sentiment function to all tweets
    df_clean = df_raw.withColumn('sentiment', sentiment(df_raw['tweets']))

    # save final output as csv
    df_clean.write.csv('s3://london-housing-webapp/twitter_output_dryrun_5.csv')

    spark.stop()

    #sentiment_analysis(input_loc=args.input, output_loc=args.output)
