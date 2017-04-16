#!/usr/bin/python
from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, RegexTokenizer, NGram, Word2Vec, HashingTF, IDF
from pyspark.ml.classification import NaiveBayes, NaiveBayesModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.linalg import Vectors, VectorUDT

from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType


# todo load data from trainer first, remove stop words, stemming, vectorize

if __name__ == '__main__':
    """
    The data is a CSV with emoticons removed. Data file format has 6 fields:
    0 - the polarity of the tweet (0 = negative, 2 = neutral, 4 = positive)
    1 - the id of the tweet (2087)
    2 - the date of the tweet (Sat May 16 23:58:44 UTC 2009)
    3 - the query (lyx). If there is no query, then this value is NO_QUERY.
    4 - the user that tweeted (robotickilldozr)
    5 - the text of the tweet (Lyx is cool)
    """
    master = "local[*]"
    if len(sys.argv) == 2:
        master = sys.argv[1]
    sc = SparkContext(master, "Twitter_Sentiment_Analysis")
    spark = SparkSession.builder.appName("Twitter_Sentiment_Analysis").getOrCreate()

    rdd = sc.textFile("/training_data/training.1600000.processed.noemoticon.csv")
    rdd_pos = sc.textFile("/training_data/positive-words.txt")
    rdd_neg = sc.textFile("/training_data/negative-words.txt")

    def parse_col(line):
        line = line.replace("\"", '')
        txt = line.split(",")
        if len(txt) < 2: return [""]
        j = " ".join(txt[5:])
        res = txt[:5]
        res.append(j)

        return res

    cols = rdd.map(parse_col).filter(lambda line: len(line) > 2)
    text = cols.map(lambda line: (float(line[0]), line[5]))

    def rm_junks(line):
        val = line[0]
        txt = line[1]
        words = txt.split(" ")
        keep = []
        for w in words:
            if not (w.startswith("@") or w.startswith("#")):
                w = w.replace('.', '')
                w = w.replace(',', '')
                w = w.replace(';', '')
                w = w.replace('\'t', ' not')
                keep.append(w)
            elif "http" in w or "www." in w:
                w = "URL"
                keep.append(w)
        ntxt = " ".join(keep)
        nline = (val, ntxt)
        return nline

    text2 = text.map(rm_junks).collect()

    rawLabelTweetDataFrame = spark.createDataFrame(text2, ["label", "tweets"])

    regexTokenizer = RegexTokenizer(inputCol="tweets", outputCol="words", pattern="\\W")
    tokenized = regexTokenizer.transform(rawLabelTweetDataFrame)

    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    filteredDataFrame = remover.transform(tokenized).select("label", "filtered")

    uningram = NGram(n=2, inputCol="filtered", outputCol="ngrams")
    uningramDataFrame = uningram.transform(filteredDataFrame)
    uningramDataFrame.select("label", "ngrams").show(truncate=False)

    uningramData = uningramDataFrame.select("label", "ngrams")

    #hashingTF = HashingTF(inputCol="ngrams", outputCol="rawFeatures", numFeatures=20)
	
    featurizedData = hashingTF.transform(uningramData)
    # alternatively, CountVectorizer can also be used to get term frequency vectors

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)

    rescaledData.select("label", "features").show()

    splits = rescaledData.randomSplit([0.7, 0.3], 1234)
    train = splits[0]
    test = splits[1]

    # create the trainer and set its parameters
    nb = NaiveBayes(smoothing=1.0, modelType="multinomial")

    # train the model
    model = nb.fit(train)

    # select example rows to display.
    predictions = model.transform(test)
    predictions.show()

    # compute accuracy on the test set
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                                  metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print("Test set accuracy = " + str(accuracy))














