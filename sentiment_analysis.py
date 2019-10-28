##

# Review Analysis 2.0 - Google
# 'sentiment_analysis'
# Gide Inc. 2019

# Hutto, C.J. & Gilbert, E.E. (2014). VADER: A Parsimonious Rule-based Model for Sentiment Analysis of
# Social Media Text.
# Eighth International Conference on Weblogs and Social Media (ICWSM-14). Ann Arbor, MI, June 2014.

##

import ssl
import textblob
from textblob import TextBlob

# textblob.download_corpora

from pyspark.sql.functions import split, udf
from pyspark.sql.types import StringType, IntegerType

from loggingInfo import init_logging
logger = init_logging()


def sentiment_detection(sentence):

    """
    :purpose detects sentiment (negative or positive mood) for each sentence
    :param sentence:
    :return:
    """

    try:
        logger.info("Analyzing Sentence Sentiment")

        if sentence != None:
            sentiment = TextBlob(sentence)
            ss = float(sentiment.sentiment.polarity)
            return ss
        else:
            return 0.0

    except AttributeError as error:
        logger.error("Issue Analyzing Sentence Sentiment")


def sentiment_analysis(data, col="Review Text"):

    """
    :purpose: construct sentiment analysis information in database
    :param data:
    :param col:
    :return: new_data
    """

    try:
        logger.info('Sentiment Analysis Updating')
        udfSentAnal= udf(sentiment_detection, StringType())
        new_data = data.withColumn("Sentiment Score", udfSentAnal(col))
        logger.info("Sentiment Analysis Finished")

    except ValueError as error:
        logger.error('Sentiment Analysis Error Detected:' + str(error))
        new_data = data.withColumn("Sentiment Score", "ERROR")

    return new_data


def sentiment_kw_analysis(data, col="KWSentence"):

    """
    :purpose: construct sentiment with the keywords and sentences containing keywords in database
    :param data:
    :param col:
    :return:
    """

    try:
        logger.info('KW Sentiment Analysis Updating')
        udfSentAnal= udf(sentiment_detection, StringType())
        new_data = data.withColumn("KW Sentiment Score", udfSentAnal(col))
        logger.info("KW Sentiment Analysis Finished")

    except ValueError as error:
        logger.error('KW Sentiment Analysis Error Detected:' + str(error))
        new_data = data.withColumn("KW Sentiment Score", "ERROR")

    return new_data
