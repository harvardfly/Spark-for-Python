# coding: utf-8
from pyspark.sql import SparkSession
from settings import MYSQL_CONF
from pyspark import SparkConf


class SparkSql(object):
    conf = SparkConf()

    def __init__(self):
        self.spark = None
        self.connector = None

        self.init_spark_confer()
        self.init_mysql_connector()

    def init_spark_confer(self):
        """
        初始化spark配置
        :return:
        """
        self.spark = SparkSession.builder \
            .config(conf=self.conf) \
            .getOrCreate()

    def init_mysql_connector(self):
        """
        spark连接mysql
        :return:
        """
        connector_url = 'jdbc:mysql://{0}:{1}/{2}'.format(
            MYSQL_CONF['host'],
            MYSQL_CONF['port'],
            MYSQL_CONF['db']
        )

        self.connector = self.spark.read \
            .format("jdbc") \
            .option("url", connector_url) \
            .option("user", MYSQL_CONF['user']) \
            .option("password", MYSQL_CONF['password'])

    def load_table_dataframe(self, table_name):
        """
        读取数据库表的DATAFRAME
        :param table_name:
        :return:
        """
        table_dataframe = self.connector.option('dbtable', table_name).load()
        print(table_dataframe)
        return table_dataframe

    def __del__(self):
        """
        对象任务完成后关闭链接
        :return:
        """
        print('关闭spark链接')
        self.spark.stop()
