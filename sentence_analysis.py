##

# Review Analysis 2.0 - Google
# 'sentence_analysis'
# Gide Inc. 2019

# Hutto, C.J. & Gilbert, E.E. (2014). VADER: A Parsimonious Rule-based Model for Sentiment Analysis of
# Social Media Text.
# Eighth International Conference on Weblogs and Social Media (ICWSM-14). Ann Arbor, MI, June 2014.

##

from pyspark.sql.functions import split, udf, explode, col
from pyspark.sql.types import StringType, IntegerType, MapType, StructType, ArrayType, StructField
from loggingInfo import init_logging
from nltk import sent_tokenize, word_tokenize
from itertools import product
import re

logger = init_logging()


def _word_detection(quality_words, sentenceLst):  # underscore helper function

    """
    :purpose if in each sentence list, if it detects keywords --> produce the words detected
    :param quality_words:
    :param sentenceLst:
    :return:
    """

    def find_whole_word(w):
        return re.compile(r'\b({0})\b'.format(w), flags=re.IGNORECASE).search

    sent_extracted = []

    for word, sentences in product(quality_words, sentenceLst):
        if find_whole_word(word)(sentences): # 'in' also can contain substring of words (may need to change this function)
            sent_extracted.append((word, sentences))

    return sent_extracted


def kw_sentence_detection(sentences):
    """
    :purpose construction of keywords and sentences containing the keywords
    :param sentences:
    :return:
    """

    with open('quality_words.txt', 'r') as f:

        high_quality_words = [line.split(", ") for line in f][0]

    try:
        lst_of_sentences = sent_tokenize(sentences)
    except:
        lst_of_sentences = [" "]

    kw_sent_extracted = _word_detection(high_quality_words, lst_of_sentences)

    return kw_sent_extracted


def sentence_analysis(data):
    """
    :purpose: For each keyword and keyword sentences, we must populate (explode) them into several rows to clearly
    see what author and what review are using such keywords/sentences
    :param data: [data] DataFrame (Spark) - initial DataFrame from reading GOOGLE_REVIEWS.csv
    :return: [new_sent_data] DataFrame (Spark)
    """

    logger.info('Analyzing Keywords and Sentences in Review Text')
    udf_sent_extracted = udf(kw_sentence_detection, ArrayType(StructType([StructField("Keywords", StringType()),
                                                                          StructField("KWSentence", StringType())])))

    new_sent_data = data.withColumn("Keywords", explode(udf_sent_extracted("Review Text")))

    new_sent_data = new_sent_data.select("GOOGLE_REVIEWS ID", "Keywords.Keywords", "Keywords.KWSentence",
                                         "Business Rating",
                                         "Business Reviews", "Source URL", "Business Name", "Author Name",
                                         "Local Guide", "Review Text", "Review Rating", "Author URL", "Review Date")

    return new_sent_data
