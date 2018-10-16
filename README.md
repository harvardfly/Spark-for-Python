# 项目说明
基于PySpark的统计分析,主要分为以下模块：
```angular2html
1.spark_core:spark的基本操作，统计、wordcount、TopN等,数据主要来自英文新闻网站和自己随机构造的数据
2.spark_mllib:针对spark mllib里面机器学习算法的使用做了demo及说明，数据集来自spark2.3.2安装包里面mllib自带的数据集
3.spark_sql: 
    (1)spark_sql对本地数据库的数据统计;数据主要为教育数据，对试题、试卷、知识点做统计分析
    (2)对空气质量指数PM2.5的分析，使用ElasticSearch存取数据
4.spark_streaming: 监听本地9999端口,streaming统计分析；streaming与Kafka结合起来处理分析；
5.其它模块持续更新中...
```
  
# 项目使用
```
cd Spark-for-Python  进入到项目
pip install -r requirements.txt  安装所需的pip包
cp settings.py.example settings.py  修改配置文件

将mysql-connector放到spark的jars目录
cd spark_sql
cp mysql-connector-java-8.0.11.jar /home/ubuntu/spark-2.3.2-bin-hadoop2.7/jars

Spark SQL连接ElasticSearch
cp elasticsearch-spark-20_2.11-6.4.1.jar /home/ubuntu/spark-2.3.2-bin-hadoop2.7/jars

如果提示：ClassNotFoundException  Failed to find data source: org.elasticsearch.spark.sql.，则表示spark没有发现jar包，此时需重新编译pyspark：
    (1) cd /opt/spark-2.3.2-bin-hadoop2.7/python
    (2) python3 setup.py sdist
    (3) pip install dist/*.tar.gz
    
如果提示：Multiple ES-Hadoop versions detected in the classpath; please use only one ，则表示ES-Hadoop有多余的(既有elasticsearch-hadoop，又有elasticsearch-spark)：
    此时删除多余的jar包，重新编译pyspark 即可
    

创建虚拟环境：virtualenv -p python3 env_py3_spark
安装pyspark包：
    1.cd /home/ubuntu/spark-2.3.2-bin-hadoop2.7/python
    2.python3 setup.py sdist
    3.pip3 install dist/*.tar.gz
```

# 版本控制
```angular2html
1.Spark版本为2.3.2 
2.Python版本为3.5.2
3.mysql-connector-java-8.0.11.jar
4.ElasticSearch 6.4.1  Kinaba 6.4.1
5.elasticsearch-spark-20_2.11-6.4.1.jar
```
