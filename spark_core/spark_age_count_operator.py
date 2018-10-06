import os
import time
import random
from shortuuid import uuid
from pyspark import SparkContext, SparkConf
from operator import add

conf = SparkConf()
sc = SparkContext(conf=conf)


def time_func(func):
    """
    程序运行时间装饰器
    :param func:
    :return:
    """

    def funcer(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print("{}:{}".format(func.__name__, end_time - start_time))
        return result

    return funcer


def gen_random_data(text_name):
    """
    生成随机的数据供spark运行
    :param text_name:
    :return:
    """
    with open(text_name, "w+") as f:
        for i in range(100):
            shot_id = uuid()
            age = random.randint(1, 99)
            f.write(shot_id)
            f.write("\t")
            f.write(str(age))
            f.write("\n")


def format_data_rdd():
    """
    格式化数据集为所需的rdd
    :return:
    """
    file_path = "name_age.txt"
    if not os.path.exists(file_path):
        try:
            gen_random_data(file_path)
        except Exception as e:
            print(e)

    text_rdd = sc.textFile(file_path)
    format_rdd_data = text_rdd.map(lambda s: s.split("\t")[1])

    return format_rdd_data


def operator_rdd():
    """
    对rdd数据集执行求和、求平均值操作
    :return:
    """
    data_rdd = format_data_rdd()
    counts = data_rdd.count()
    sum_age = data_rdd.map(lambda x: int(x)).reduce(add)

    print("avg age is:{}".format(sum_age / counts))


operator_rdd()
sc.stop()
