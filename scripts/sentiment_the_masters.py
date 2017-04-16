#!/usr/bin python
from __future__ import print_function
import sys
import json
import sys
from pyspark import SparkContext
from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, RegexTokenizer, NGram, Word2Vec, HashingTF, IDF
from pyspark.ml.classification import NaiveBayes, NaiveBayesModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType

import matplotlib.pyplot as plt
import pandas
import panda

if __name__ == '__main__':
    base = "hdfs://localhost:9000/twitter_data/"
    master = "local[*]"
    # sc = SparkContext(master, "Twitter_Sentiment_Analysis")
    rdd = spark.sparkContext.wholeTextFiles(base)

    def extract_time_and_tweet(line):
        # print(line[len(base):])
        print(line[0])
        a_flume = line[1].split('\n')
        for tweet in a_flume:
            if len(tweet) > 3:
                data = json.loads(tweet)
                return 0.0, data['text'], data["timestamp_ms"]

    text2 = rdd.map(extract_time_and_tweet)

    rawLabelTweetDataFrame = spark.createDataFrame(text2, ["label", "tweets", "time_stamp_ms"])

    regexTokenizer = RegexTokenizer(inputCol="tweets", outputCol="words", pattern="\\W")
    tokenized = regexTokenizer.transform(rawLabelTweetDataFrame)

    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    filteredDataFrame = remover.transform(tokenized).select("label", "filtered", "time_stamp_ms")

    ngram = NGram(n=1, inputCol="filtered", outputCol="ngrams")
    ngramDataFrame = ngram.transform(filteredDataFrame)
    ngramDataFrame.show()

    ngramData = ngramDataFrame.select("label", "ngrams", "time_stamp_ms")
    hashingTF = HashingTF(inputCol="ngrams", outputCol="rawFeatures", numFeatures=3000)
    featurizedData = hashingTF.transform(ngramData)
    # alternatively, CountVectorizer can also be used to get term frequency vectors
    featurizedData.show()

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)
    rescaledData.show()

    model = NaiveBayesModel.load("hdfs://localhost:9000/model_naive_bayes")
    predictions = model.transform(rescaledData)
    predictions.show()

    res = predictions.toPandas()
    print(predictions.count())

    plt.plot(res.time_stamp_ms, res.prediction)
    plt.show()

