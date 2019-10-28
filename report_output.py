##

# Review Analysis 2.0 - Google
# 'report_output'
# Gide Inc. 2019

##

from words_analysis import *
from dataCSV import *
from sentiment_analysis import *
from sentence_analysis import *
from loggingInfo import init_logging
from quality_analysis import *
from spam_analysis import *
# from punc_removal import PunctuationRemoval
# from pyspark.sql.functions import udf, collect_list
# from pyspark.sql.types import StringType
from spam_analysis import spam_train, spam_detection

spark, rev_data, auth_data = load_dataset_and_set_views()
rev_list, auth_list = review_count(rev_data, auth_data)
prod_data = rev_data
logger = init_logging()

"""
Spam Analysis Inputs
"""
training_data, test_data, tf = spam_train()
training_data.cache()
test_data.cache()
model = spam_detection(training_data, test_data)

"""
Quality Analysis Inputs
"""
author_all_percentage = all_author_review_count(auth_list)


def sentiment_info():
    """
    :purpose extract sentiment information including: word count, high word count, low word count, and sentiment
    :return new_rev_data: DataFrame for Sentiment Review Table (Sentiment Info on the reviews for one site)
    """

    new_rev_data = word_count_analysis(rev_data)
    new_rev_data = high_quality_words_analysis(new_rev_data)
    new_rev_data = low_quality_words_analysis(new_rev_data)
    new_rev_data = sentiment_analysis(new_rev_data)
    new_rev_data = spam_analysis(new_rev_data, model=model, tf=tf)

    return new_rev_data


def author_info():
    """
    :purpose extract author information - word count, sentiment, low word count, high word count, quality weight
    :return new_auth_data: DataFrame for Author Table (all author reviews)
    """

    """
    temp percentage (for future debugging purposes)
    """

    """
    """
    new_auth_data = word_count_analysis(auth_data)
    new_auth_data = sentiment_analysis(new_auth_data)
    new_auth_data = high_quality_words_analysis(new_auth_data)
    new_auth_data = low_quality_words_analysis(new_auth_data)
    new_auth_data = quality_analysis_author(new_auth_data, author_all_percentage)  # corpus_count, token_count)
    new_auth_data = spam_analysis(new_auth_data, model=model, tf=tf)

    return new_auth_data


def product_info():
    """
    :purpose extract product information - keyword and keyword in sentence extraction, sentiment, word count, low word
    count
    :return new_prod_data: DataFrame for Product Review Table (Product Info including keywords for one site)
    """
    product_all_percentage = all_author_review_count(rev_list)

    new_prod_data = prod_data.select("GOOGLE_REVIEWS ID", "Business Rating", "Business Reviews", "Source URL",
                                     "Business Name", "Author Name", "Local Guide", "Review Text", "Review Rating",
                                     "Author URL", "Review Date")

    new_prod_data = sentence_analysis(new_prod_data)
    new_prod_data = sentiment_kw_analysis(new_prod_data)
    new_prod_data = sentiment_analysis(new_prod_data)
    new_prod_data = high_quality_words_analysis(new_prod_data)
    new_prod_data = word_count_analysis(new_prod_data, col="KWSentence")
    new_prod_data = low_quality_words_analysis(new_prod_data, col="KWSentence")
    new_prod_data = quality_analysis_product(new_prod_data, product_all_percentage)  # corpus_count, token_count)

    return new_prod_data


def initiate():
    """
    :purpose writes CSV files for each functionalities above
    :return None: returns None - generates three folders containing csv analysis
    """

    with open('GoogleReviewAnalysisLog.log', 'w'):
        pass

    try:
        logger.info("Creating Sentiment Info CSV")
        sent_df = sentiment_info()
        sent_df.write.format('csv') \
            .mode('overwrite').option("header", "true").save("sentiment_info")
    except:
        logger.error("Error In Creating Sentiment Info CSV")

    try:
        logger.info("Creating Product Info CSV")
        prod_df = product_info()
        prod_df.write.format('csv') \
            .mode('overwrite').option("header", "true").save("product_info")
    except:
        logger.error("Error In Creating Product Info CSV")

    try:
        logger.info("Creating Author Info CSV")
        auth_df = author_info()
        auth_df.write.format('csv') \
            .mode('overwrite').option("header", "true").save("author_info")
    except:
        logger.info("Error In Creating Author Info CSV")


initiate()
