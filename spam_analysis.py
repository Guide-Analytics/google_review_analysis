from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from loggingInfo import init_logging
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.regression import LabeledPoint
from dataCSV import spam_ham_words
from pyspark.mllib.classification import LogisticRegressionWithSGD, LogisticRegressionWithLBFGS, SVMWithSGD, NaiveBayes
logger = init_logging()


def spam_train():

    spam_words, ham_words = spam_ham_words()
    tf = HashingTF(numFeatures=200)
    spam_features = tf.transform(spam_words)
    ham_features = tf.transform(ham_words)

    spam_samples = spam_features.map(lambda features: LabeledPoint(1, features))
    ham_samples = ham_features.map(lambda features: LabeledPoint(0, features))

    samples = spam_samples.union(ham_samples)
    [training_data, test_data] = samples.randomSplit([0.8, 0.2])
    training_data.cache()
    test_data.cache()

    return training_data, test_data, tf


def spam_detection(training_data, test_data):

    def score(model):
        predictions = model.predict(test_data.map(lambda x: x.features))
        labels_and_preds = test_data.map(lambda x: x.label).zip(predictions)
        accuracy = labels_and_preds.filter(lambda x: x[0] == x[1]).count() / float(test_data.count())
        return accuracy

    algo = NaiveBayes()
    model = algo.train(training_data)
    print(str(score(model)))

    return model


def spam_predict(review, model, tf):
    """

    :param review:
    :param model:
    :param tf:
    :return: float 0.0 or 1.0
    """

    spam = tf.transform(str(review).split(" "))

    return str(model.predict(spam))


def spam_score(model, tf):
    return udf(lambda l: spam_predict(l, model, tf))


def spam_analysis(data, model, tf):

    """
    :purpose: detect any spam reviews
    :param data:
    :return: dataframe new_data
    """

    try:
        logger.info('Spam Updating')
        new_data = data.withColumn('spam_detector', spam_score(model, tf)(col('Review Text')))
        logger.info("Spam Labelling Complete")

    except ValueError as error:
        logger.error('Spam Labelling Error Detected:' + str(error))
        new_data = data.withColumn("spam_detector", "ERROR")

    return new_data
