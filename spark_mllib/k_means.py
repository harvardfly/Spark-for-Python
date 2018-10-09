import os
from pyspark import (
    SparkConf, SparkContext
)
from pyspark.mllib.clustering import (
    KMeans, KMeansModel
)
from numpy import array
from math import sqrt

conf = SparkConf()
sc = SparkContext(conf=conf)

current_dir = os.path.dirname(os.path.realpath(__file__))


def kmeans():
    """
    使用mllib对Spark安装包mllib的测试数据集做K-means聚类,由于train方法：
        Training points as an `RDD` of `Vector` or convertible
    所以需对数据集格式化：
        初始数据集 --> ['0.0 0.0 0.0', '0.1 0.1 0.1', '0.2 0.2 0.2']
        格式化后数据集 --> [array([0., 0., 0.]), array([0.1, 0.1, 0.1]), array([0.2, 0.2, 0.2])]
    :return:
    """
    data_rdd = sc.textFile('{}/mllib/kmeans_data.txt'.format(current_dir))
    parsed_data_rdd = data_rdd.map(lambda line: array([float(x) for x in line.split(' ')]))

    # 建立聚类模型
    clusters = KMeans.train(parsed_data_rdd, 2, maxIterations=10, initializationMode="random")

    # Evaluate clustering by computing Within Set Sum of Squared Errors
    def error(point):
        center = clusters.centers[clusters.predict(point)]
        return sqrt(sum([x ** 2 for x in (point - center)]))

    WSSSE = parsed_data_rdd.map(lambda point: error(point)).reduce(lambda x, y: x + y)
    print("Within Set Sum of Squared Error = " + str(WSSSE))

    # 保存 训练好的模型
    model_path = "{}/kmeans_model".format(current_dir)
    if not os.path.exists(model_path):
        clusters.save(sc, model_path)

    trained_model = KMeansModel.load(
        sc, "{}/kmeans_model".format(current_dir)
    )
    return trained_model


kmeans()
sc.stop()
