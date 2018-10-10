import os
import time
from pyspark import SparkContext, SparkConf

conf = SparkConf()
sc = SparkContext(conf=conf)

current_dir = os.path.dirname(os.path.realpath(__file__))


def uncache_data():
    file_rdd = sc.textFile("file://{}/cnsa_news.txt".format(current_dir))
    first_start = time.time()
    file_rdd.count()
    first_end = time.time()
    print("uncache first:{}".format(first_end-first_start))

    second_start = time.time()
    file_rdd.count()
    second_end = time.time()
    print("uncache second:{}".format(second_end - second_start))


def cache_data():
    file_rdd = sc.textFile("file://{}/cnsa_news.txt".format(current_dir)).cache()
    first_start = time.time()
    file_rdd.collect()
    first_end = time.time()
    print("cache first:{}".format(first_end - first_start))

    second_start = time.time()
    file_rdd.count()
    second_end = time.time()
    print("cache second:{}".format(second_end - second_start))


# uncache_data()
cache_data()
sc.stop()
