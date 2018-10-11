# coding: utf-8
import sys
import os

pre_current_dir = os.path.dirname(os.getcwd())
sys.path.append(pre_current_dir)
from spark_sql.spark_sql_base import SparkSql


class SparkQuestion(object):
    """
    DEMO:
    s_question = SparkQuestion()
    diff_df = s_question.get_question_diff_distri(faculty=3, subject=1)
    print(diff_df.toJSON().collect())
    """

    def __init__(self):
        self.spark_sql = SparkSql()
        self.question_df = self.spark_sql.load_table_dataframe('question')
        self.question_cog_map_df = self.spark_sql.load_table_dataframe(
            'question_cognition_map'
        )
        self.q_choice_df = self.spark_sql.load_table_dataframe('question_choice')

    def get_question_diff(self, faculty=None, subject=None):
        """
        得到某学科和学段下面试题的困难度分布 按试题数从大到小排序
        :param faculty:
        :param subject:
        :return:
        """

        filter_str = "faculty = {0} and subject = {1}".format(
            faculty, subject
        )

        # 统计排序
        res_df = self.question_df.filter(filter_str).groupBy(
            "diff"
        ).count().sort("count", ascending=False)

        return res_df


s_question = SparkQuestion()
diff_df = s_question.get_question_diff(faculty=3, subject=1)
print(diff_df.toJSON().collect())
