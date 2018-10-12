# coding:utf-8
from pyspark import (
    SparkContext, SparkConf
)
from pyspark.streaming import StreamingContext
from operator import add

if __name__ == "__main__":
    """
    一个基于spark_streaming的简单wordcount，服务端nc -lk监听9999端口，
    在terminal输入word，streaming每隔5秒输出统计的wordcount
    """
    conf = SparkConf()
    sc = SparkContext(conf=conf, appName="PythonStreamingNetworkWordCount")
    ssc = StreamingContext(sc, 5)

    lines = ssc.socketTextStream("localhost", 9999)
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(add)

    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
