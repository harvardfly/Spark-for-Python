# coding: utf-8
from pyspark.sql import SparkSession
from settings import MYSQL_CONF
from abc import abstractmethod


class SparkSql(object):
    def __init__(self):
        self.spark = None
        self.connector_url = None

        self.init_spark_confer()
        self.init_mysql_connector()

    def init_spark_confer(self):
        """
        初始化spark配置
        :return:
        """
        self.spark = SparkSession.builder \
            .appName("spark_sql") \
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

        self.df = self.spark.read \
            .format("jdbc") \
            .option("url", connector_url) \
            .option("user", MYSQL_CONF['user']) \
            .option("password", MYSQL_CONF['password'])

    def load_table_dataframe (self, table_name):
        """
        读取数据库表的DATAFRAME
        :param table_name:
        :return:
        """
        table_dataframe = self.df.option('dbtable', table_name).load()
        return table_dataframe
