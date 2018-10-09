import os
from pyspark import SparkConf, SparkContext
from operator import add

conf = SparkConf()
sc = SparkContext(conf=conf)

current_dir = os.path.dirname(os.path.realpath(__file__))


def format_read_from_text():
    """
    cnsa_news.txt 是我在新闻网截取的一段英文新闻
    从文件中读取单词  格式化后将其转换为RDD为数据集
    :return:
    """
    file_rdd = sc.textFile("{}/cnsa_news.txt".format(current_dir))
    format_rdd = file_rdd.flatMap(lambda s: s.strip(".").split(" ")).map(lambda x: (x, 1))
    return format_rdd


def count_word_by_sort():
    """
    统计单词出现频率  频率从大到小排序,并将其保存到文件
    :return:
    """
    format_rdd = format_read_from_text()
    result_rdd = format_rdd.reduceByKey(add).map(
        lambda x: (x[1], x[0])
    ).sortByKey(False).map(lambda x: (x[1], x[0]))
    print(result_rdd.collect())
    if not os.path.exists("result"):
        result_rdd.saveAsTextFile("result")


count_word_by_sort()
sc.stop()
