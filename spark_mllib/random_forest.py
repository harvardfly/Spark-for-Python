from pyspark import (
    SparkConf, SparkContext
)
from pyspark.mllib.tree import (
    RandomForest, RandomForestModel
)

from pyspark.mllib.util import MLUtils

conf = SparkConf()
sc = SparkContext(conf=conf)

data_rdd = MLUtils.loadLibSVMFile(sc, 'mllib/sample_libsvm_data.txt')
train_data_rdd, test_data_rdd = data_rdd.randomSplit([0.8, 0.2])
model = RandomForest.trainClassifier(
    train_data_rdd, numClasses=2, categoricalFeaturesInfo={}, numTrees=3
)

# 根据测试集的features预测laber值为0还是1
predict_rdd = model.predict(test_data_rdd.map(lambda x: x.features))

# 测试集实际的laber值
labels_rdd = test_data_rdd.map(lambda lp: lp.label).zip(predict_rdd)
print(labels_rdd.collect())
# print(test_data_rdd.map(lambda lp: lp.features).collect())
# testErr = labels_rdd.filter(lambda v, p: v != p).count() / float(test_data_rdd.count())
# print('Test Error = ' + str(testErr))
# print('Learned classification forest model:')
# print(model.toDebugString())
#
# # Save and load model
# # model.save(sc,"target/tmp/myRandomForestClassificationModel")
# # sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModel")

sc.stop()
