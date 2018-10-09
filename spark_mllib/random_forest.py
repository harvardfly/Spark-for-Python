import os
from pyspark import (
    SparkConf, SparkContext
)
from pyspark.mllib.tree import (
    RandomForest, RandomForestModel
)

from pyspark.mllib.util import MLUtils

conf = SparkConf()
sc = SparkContext(conf=conf)

current_dir = os.path.dirname(os.path.realpath(__file__))


def random_forest():
    """
    使用mllib对Spark安装包mllib的测试数据集做随机森林测试
    80%数据作为训练数据  20%数据作为测试数据
    :return:
    """
    data_rdd = MLUtils.loadLibSVMFile(
        sc, '{}/mllib/sample_libsvm_data.txt'.format(current_dir)
    )

    train_data_rdd, test_data_rdd = data_rdd.randomSplit([0.8, 0.2])
    model = RandomForest.trainClassifier(
        train_data_rdd, numClasses=2, categoricalFeaturesInfo={}, numTrees=3
    )

    # 根据测试集的features预测laber值为0还是1
    predict_rdd = model.predict(test_data_rdd.map(lambda x: x.features))

    # 测试集实际的laber值
    labels_rdd = test_data_rdd.map(lambda lp: lp.label).zip(predict_rdd)

    # 测试样本中预测值与实际值不符的百分比(错误率)
    print(labels_rdd.filter(lambda x: x[0] != x[1]).count())
    test_err = labels_rdd.filter(lambda x: x[0] != x[1]).count() / float(test_data_rdd.count())
    print("test error rate:{}".format(test_err))

    # 保存 训练好的模型
    model_path = "{}/my_random_forest_model".format(current_dir)
    if not os.path.exists(model_path):
        model.save(sc, model_path)

    trained_model = RandomForestModel.load(
        sc, "{}/my_random_forest_model".format(current_dir)
    )
    print(trained_model.toDebugString())
    return trained_model


random_forest()
sc.stop()
