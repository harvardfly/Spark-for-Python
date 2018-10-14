# coding: utf-8
import sys
import os

pre_current_dir = os.path.dirname(os.getcwd())
sys.path.append(pre_current_dir)
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from settings import ES_CONF

current_dir = os.path.dirname(os.path.realpath(__file__))

spark = SparkSession.builder.appName("weather_result").getOrCreate()


def get_health_level(value):
    """
    PM2.5对应健康级别
    :param value:
    :return:
    """
    if 0 <= value <= 50:
        return "Very Good"
    elif 50 < value <= 100:
        return "Good"
    elif 100 < value <= 150:
        return "Unhealthy for Sensi"
    elif value <= 200:
        return "Unhealthy"
    elif 200 < value <= 300:
        return "Very Unhealthy"
    elif 300 < value <= 500:
        return "Hazardous"
    elif value > 500:
        return "Extreme danger"
    else:
        return None


def get_weather_result():
    """
    获取Spark SQL分析后的数据
    :return:
    """
    # load所需字段的数据到DF
    df_2017 = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("file://{}/data/Beijing2017_PM25.csv".format(current_dir)) \
        .select("Year", "Month", "Day", "Hour", "Value", "QC Name")

    # 查看Schema
    df_2017.printSchema()

    # 创建对应级别的
    level_function_udf = udf(get_health_level, StringType())

    # 新建列healthy_level 并healthy_level分组
    group_2017 = df_2017.withColumn(
        "healthy_level", level_function_udf(df_2017['Value'])
    ).groupBy("healthy_level").count()

    # 新建列days和percentage 并计算它们对应的值
    result_2017 = group_2017.select("healthy_level", "count") \
        .withColumn("days", group_2017['count'] / 24) \
        .withColumn("percentage", group_2017['count'] / df_2017.count())
    result_2017.show()


def write_result_es():
    """
    将SparkSQL计算结果写入到ES
    :return:
    """
    result_2017 = get_weather_result()
    result_2017.selectExpr("Grade as grade", "count", "precent") \
        .write.format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "{}".format(ES_CONF['ELASTIC_HOST'])) \
        .mode("overwrite") \
        .save("{}/pm_value".format(ES_CONF['WEATHER_INDEX_NAME']))

write_result_es()
spark.stop()
