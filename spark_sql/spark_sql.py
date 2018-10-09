# coding: utf-8
from pyspark.sql import SparkSession
from settings import MYSQL_CONF


class SparkBase(object):
    """
    SPARK 基类
    """

    def init_jdbc(self):
        raise NotImplementedError


class SparkSql(SparkBase):

    def __init__(self):
        self.spark = None
        self.jdbc = None

        self.init_spark()
        self.init_jdbc()

    def init_spark(self):
        """
        初始化spark
        :return:
        """
        self.spark = SparkSession.builder \
            .master(self.SPARK_MASTER) \
            .appName(self.APP_NAME) \
            .getOrCreate()

    def init_jdbc(self):
        """
        初始化jdbc
        :return:
        """
        jdbc_url = 'jdbc:mysql://{0}:{1}/{2}'.format(
            MYSQL_CONF['host'],
            MYSQL_CONF['port'],
            MYSQL_CONF['db']
        )

        self.jdbc = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("user", MYSQL_CONF['user']) \
            .option("password", MYSQL_CONF['password'])

    def load_table_df(self, table_name):
        """
        读取数据库表的DATAFRAME
        :param table_name:
        :return: DATAFRAME
        """
        jdbc_df = self.jdbc.option('dbtable', table_name).load()
        return jdbc_df
