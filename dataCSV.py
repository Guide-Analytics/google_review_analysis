##

# Review Analysis 2.0 - Google
# 'dataCSV'
# Gide Inc. 2019

##

from pyspark.sql import SparkSession, SQLContext
from loggingInfo import init_logging
from pyspark.sql.functions import regexp_replace, udf, collect_list
from pyspark.sql.types import StringType
from punc_removal import PunctuationRemoval
from pyspark import SparkContext, SparkConf

import random
import time
sc = SparkContext(appName="Spam Test", master="local[*]",
                  conf=SparkConf().set('spark.ui.port',
                                       random.randrange(4000, 5000)))

# Initiate Spark Session
spark = SparkSession.builder.appName('Google Review Report 2.0')\
    .master("local[2]")\
    .config('spark.ui.port', random.randrange(4000, 5000)).getOrCreate()


# load_dataset_and_set_views:
# Purpose: reading the data from csv through SPARK
# Input; [String] pathR, pathA
# Output: [Spark DataFrame] rev_data, auth_data
def load_dataset_and_set_views(pathR="GOOGLE_REVIEWS.csv.gz", pathA="REVIEWS_AUTHORS.csv.gz"):

    """
    :param pathR: default csv file: GOOGLE_REVIEWS.csv
    :param pathA: default csv file: REVIEWS_AUTHORS.csv
    :return: spark, reviews DataFrame, authors DataFrame
    """

    logger = init_logging()

    logger.info('Start READ Google Reviews CSV')

    # Try not to touch these commands.
    # These are normal Spark functions and it is the quickest way to handle Spark data
    while True:
        try:
            rev_data_raw = spark.read.csv(pathR, mode="PERMISSIVE", header='true', sep=',', inferSchema=True,
                                          multiLine=True, quote='"', escape='"')
            rev_data_raw = rev_data_raw.withColumn('Review Text', regexp_replace('Review Text', '"', ''))
            logger.info('Reading Reviews CSV')

            auth_data_raw = spark.read.csv(pathA, mode="PERMISSIVE", header='true', sep=',', inferSchema=True,
                                           multiLine=True, quote='"', escape='"')
            auth_data_raw = auth_data_raw.withColumn('Review Text', regexp_replace('Review Text', '"', ''))
            auth_data_raw = auth_data_raw.withColumn("Business Name", regexp_replace("Business Name", '"', ''))
            logger.info('Reading Author CSV')

            break
        except:
            logger.warning('Reviews/Authors CSV do not exist. Ensure GOOGLE_REVIEWS.csv & REVIEWS_AUTHORS.csv exist in directory.')
            logger.warning('Trying again in 10 seconds')
            time.sleep(10)

    # Setting up Database/DataFrame header names
    logger.info('Setting Up DB Headers')
    rev_data = rev_data_raw.toDF("GOOGLE_REVIEWS ID", "Business Rating", "Business Reviews", "Source URL",
                                 "Business Name", "Author Name", "Local Guide", "Review Text", "Review Rating",
                                 "Review Date", "Author URL", "Like", "Review Photo", "Scraped Time")

    auth_data = auth_data_raw.toDF("GOOGLE_REVIEWS ID", "Note", "Level", "Source URL", "Source Business Name",
                                   "Business Name", "Business Addresses", "Review Text", "Author", "Review Rating",
                                   "Review Date", "Reviewer URL", "Scraped Time", "Like", "Review Photo")

    # Creating Temp views (for extracting and testing purposes)
    logger.info('Creating Temp Views')
    rev_data.createOrReplaceTempView("rev_data")
    auth_data.createOrReplaceTempView("auth_data")

    return spark, rev_data, auth_data


def spam_ham_words():

    spam = sc.textFile('spam_texts/spam.txt')
    ham = sc.textFile("spam_texts/ham.txt")

    spam_words = spam.map(lambda email: email.split())
    ham_words = ham.map(lambda email: email.split())

    return spam_words, ham_words


def review_count(prod_data, auth_data):

    remove_punc = PunctuationRemoval()
    udf_punc = udf(remove_punc.remove_nuke, StringType())
    remove_auth_data = auth_data.withColumn('Review Text', udf_punc('Review Text'))
    remove_prod_data = prod_data.withColumn('Review Text', udf_punc('Review Text'))

    all_author_reviews = remove_auth_data.agg(collect_list('Review Text')). \
        rdd.flatMap(lambda x: x).flatMap(lambda x: x)
    all_product_reviews = remove_prod_data.agg(collect_list('Review Text')). \
        rdd.flatMap(lambda x: x).flatMap(lambda x: x)

    return all_author_reviews, all_product_reviews
