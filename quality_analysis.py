"""
#################################################
@product: Gide Product Analysis
@filename: Quality Word Usage Analysis (BETA version for Google Analysis)

@author: Michael Brock Li
@date: December 16th 2018
##################################################
"""

"""
Please note that Quality Word Analysis is not ready to produce most accurate results.
The simply functionality so far provided is divide the number of corpus word used in quality_words.txt
over the corpus used in the entire reviews
"""



import pandas as pd
import numpy as np
import scipy.stats as ss
import ast
import re
import itertools
from nltk.tokenize import word_tokenize
from pyspark.sql.functions import udf, lit
from pyspark.sql.types import StringType, IntegerType, FloatType
from punc_removal import PunctuationRemoval
from loggingInfo import init_logging


def modelInitiate(review, corpus_count, review_count):

    logger = init_logging()

    """

    :param review:
    :param all_reviews:
    :return:
    """

    """
    @purpose: Extracting one author review information and all author information
    to build and construct a model analysis between one author and all authors
    and analyze the author's use of quality words

    Note: I won't be surprised if the algorithm analysis is not clear you below or
    it is super confusing because it is confusing!

    """
    #logger.info('Analyzing Quality Words')
    #weight = float(corpus_count) / float(review_count)
    #print("Result: " +str(corpus_count)+ "||"+ str(weight))
    CAP, CARP, weight = product_characteristics(review, corpus_count, review_count)
    # CMA = confusionMatrixConst(CAP, CARP)  ## Confusion matrix (relationship model matrix of authors and their reviews)
    # V_author = cramersStatsAlgo(CMA)  ## Calculation Cramer's V to determine the relationship of the words and the reviews
    logger.info('Quality Words...'+str(weight)+"|"+str(review))

    return weight


def confusionMatrixConst(data1, data2):
    """

    :param data1:
    :param data2:
    :return:
    """
    """
    @purpose: Confusion Matrix construction

    @inputs: data1 [list], data2 [list] (from modelInititate.py)
    @outputs: confusion_matrix [pandas array]
    """

    cm_author = pd.Series(data1, name='Authors')
    #print(cm_author)
    cm_relative = pd.Series(data2, name='General')
    #print(cm_relative)

    confusion_matrix = pd.crosstab(cm_author, cm_relative)
    print(confusion_matrix)

    return confusion_matrix


def cramersStatsAlgo(confusion_matrix):
    """

    :param confusion_matrix:
    :return:
    """
    """
    Cramer's V analysis algorithm using the confusion matrix inputs
    and calculate the relationships between the words used for each author and
    the word use for all the author; using a chi-squared distribution and cramer's V
    to determine the probability of the word's relationship - the cramer's V value is a measure
    of the relationship

    @inputs: confusion_matrix [pandas array]
    @outputs: V [float]

    """

    chi2 = ss.chi2_contingency(confusion_matrix)[0]
    n = confusion_matrix.sum().sum()
    phi2 = chi2 / n
    r, k = confusion_matrix.shape

    with np.errstate(divide='ignore'):
        phi2corr = max(0, phi2 - ((k - 1) * (r - 1)) / (n - 1))
        rcorr = r - ((r - 1) ** 2) / (n - 1)
        kcorr = k - ((k - 1) ** 2) / (n - 1)
        try:
            V = np.sqrt(phi2corr / min((kcorr - 1), (rcorr - 1)))
        except:
            V = 0
    return V


def product_characteristics(review, corpus_count, review_count):

    """

    :param review:
    :param all_author_reviews:
    :return:
    """

    """
    @purpose:

    @outputs: CAP [list], CARP [list], CAAP [list], CGP [list], CT [list], authorIDInfo [list],
    all_corpus [list]

    """

    with open('quality_words.txt', 'r') as f:
        corpus_words = [line.split(", ") for line in f][0]

    def find_whole_word(w):
        return re.compile(r'\b({0})\b'.format(w), flags=re.IGNORECASE).search

    author_corpus = []
    countAuthorPercentage = []  ## One review for one author
    countAuthorReviewsPercentage = []  ## All reviews for one author

    # countGeneralPercentage = []  ## All reviews for all authors (reviews percentages (how much it changed)

    for word in corpus_words: # , a_review in product(
        if find_whole_word(word)(review):
            author_corpus.append(word)


    #print("Author Corpus" +str(author_corpus))

    word_tokens = word_tokenize(review)
    indiv_reviews = word_tokens

    """
    One individual author comparision with corpus and review
    """
    featureWordCount = float(len(author_corpus)) ## Individual Author Corpus count
    totalWordCount = float(len(indiv_reviews))  ## Individual Author Review Count
    author_corpus = []
    """
    One individual author percentage
    """
    try:
        relative_percentage = float("{0:.3f}".format(featureWordCount / totalWordCount))
    except:
        relative_percentage = 0.0

    try:
        author_all_percentage = float("{0:.3f}".format(corpus_count / review_count))
    except:
        author_all_percentage = 0.0

    #print("Relative Pe" +str(relative_percentage))
    #print("Author Percentage" +str(author_all_percentage))
    countAuthorPercentage.append(relative_percentage)
    countAuthorReviewsPercentage.append(author_all_percentage)

    CAP = countAuthorPercentage
    CARP = countAuthorReviewsPercentage
    #print("CAP" +str(CAP))
    #print("CARP" +str(CARP))
    try:
        weight = float(relative_percentage/author_all_percentage)
    except ZeroDivisionError:
        weight = 0.0
    print(weight)
    return CAP, CARP, weight


def quality_analysis_author(data, author_all_percent, col='Review Text'): # corpus_count, review_count

    """
    :param all_reviews:
    :purpose: construct quality score for Review Text and determine the weight of quality word used
    :param data:
    :param col:
    :return:
    """

    remove_punc = PunctuationRemoval()
    udf_punc = udf(remove_punc.remove_nuke, StringType())
    data = data.withColumn('Review Text', udf_punc('Review Text'))

    try:
        # logger.info('KW Sentiment Analysis Updating')
        udfQualAnal = udf(modelInitiate, StringType())
        new_data = data.withColumn("Quality Word Percentage", data['high_quality_word_count'] / data['word_count'])
                                                         ## / lit(author_all_percent)))
        # logger.info("KW Sentiment Analysis Finished")

    except ValueError as error:
        # logger.error('KW Sentiment Analysis Error Detected:' + str(error))
        new_data = data.withColumn("Quality Word Score", "ERROR")

    return new_data


def quality_analysis_product(data, product_all_percent, col='Review Text'): # corpus_count, review_count

    """
    :param all_reviews:
    :purpose: construct quality score for Review Text and determine the weight of quality word used
    :param data:
    :param col:
    :return:
    """

    remove_punc = PunctuationRemoval()
    udf_punc = udf(remove_punc.remove_nuke, StringType())
    data = data.withColumn('Review Text', udf_punc('Review Text'))

    try:
        # logger.info('KW Sentiment Analysis Updating')
        udfQualAnal = udf(modelInitiate, StringType())
        new_data = data.withColumn("Quality Word Score", ((data['high_quality_word_count'] / data['word_count'])
                                                          / lit(product_all_percent)))
        # logger.info("KW Sentiment Analysis Finished")

    except ValueError as error:
        # logger.error('KW Sentiment Analysis Error Detected:' + str(error))
        new_data = data.withColumn("Quality Word Score", "ERROR")

    return new_data


def all_author_review_count(all_reviews):
    with open('quality_words.txt', 'r') as f:
        corpus_words = [line.split(", ") for line in f][0]

    def find_whole_word(w):
        return re.compile(r'\b({0})\b'.format(w), flags=re.IGNORECASE).search

    def remove_punc(x):
        punc = '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
        lowercased_str = x.lower()
        for ch in punc:
            lowercased_str = lowercased_str.replace(ch, '')
        return lowercased_str

    new_list_corpus_count = all_reviews.map(lambda x: [i if find_whole_word(i)(x) else '' for i in corpus_words]).\
        flatMap(lambda x: x).filter(lambda x: x != '').count()

    new_list_reviews_count = all_reviews.map(remove_punc).\
        map(lambda x: word_tokenize(x)).flatMap(lambda x: x).count()

    percentage = float(new_list_corpus_count) / float(new_list_reviews_count)
    return percentage
