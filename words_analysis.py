##

# Review Analysis 2.0 - Google
# 'words_analysis'
# Gide Inc. 2019

##

from pyspark.sql.functions import split, udf
from pyspark.sql.types import StringType, IntegerType, FloatType
from loggingInfo import init_logging

import re

logger = init_logging()


def word_count(word):

    """
    :purpose: counting the number of words in sentences
    :param word:
    :return: int 0 or int length
    """

    try:
        length = len(re.findall(r"[a-z]+(?:'[a-z]+)?", word.lower()))
        return length
    except:
        return 0


def high_quality_words(word):

    """
    :purpose: counting high quality words in sentences
    :param word:
    :return: int count
    """

    count = 0
    with open('quality_words.txt', 'r') as f:

        hquality_words = [line.split(", ") for line in f][0]

    try:
        lstOfWords = re.findall(r"[a-z]+(?:'[a-z]+)?", word.lower())
    except:
        lstOfWords = []

    for hquality_word in hquality_words:

        if hquality_word in lstOfWords:
            count += 1

    return count


def low_quality_words(word):

    """
    :purpose: counting low quality words in sentences
    :param word:
    :return:
    """

    count = 0
    with open('low_quality_words.txt', 'r') as f:

        lquality_words = [line.split(", ") for line in f][0]

    try:
        lstOfWords = re.findall(r"[a-z]+(?:'[a-z]+)?", word.lower())
    except:
        lstOfWords = []

    for lquality_word in lquality_words:

        if lquality_word in lstOfWords:
            count += 1

    return count


def word_count_analysis(data, col="Review Text"):

    """
    :purpose: construct word count information in database
    :param data:
    :param col:
    :return: dataframe new_data
    """

    try:
        logger.info('Word Count Analysis Updating')
        udfWordCount = udf(word_count, StringType())
        new_data = data.withColumn("word_count", udfWordCount(col))
        logger.info('Word Count Analysis Finished')
    except:
        logger.error('Word Count Error Detected')
        new_data = data.withColumn("word_count", "ERROR")

    return new_data


def high_quality_words_analysis(data, col="Review Text"):

    """
    :purpose: construct high quality word count info in database
    :param data:
    :param col:
    :return: dataframe new_data
    """

    try:
        logger.info('High Quality Word Count Updating')
        udfHighWordCount = udf(high_quality_words, StringType())
        new_data = data.withColumn("high_quality_word_count", udfHighWordCount(col))
        logger.info("High Quality Word Count Finished")

    except ValueError as error:
        logger.error('High Quality Word Count Error Detected:' + str(error))
        new_data = data.withColumn("high_quality_word_count", "ERROR")

    return new_data


def low_quality_words_analysis(data, col="Review Text"):

    """
    :purpose: construct low quality word count info in database
    :param data:
    :param col:
    :return: dataframe new_data
    """

    try:
        logger.info('Low Quality Word Count Updating')
        udfLowWordCount = udf(low_quality_words, StringType())
        new_data = data.withColumn("Low Quality Word Count", udfLowWordCount(col))
        logger.info("Low Quality Word Count Finished")

    except ValueError as error:
        logger.error('Low Quality Word Count Error Detected:' + str(error))
        new_data = data.withColumn("Low Quality Word Count", "ERROR")

    return new_data
