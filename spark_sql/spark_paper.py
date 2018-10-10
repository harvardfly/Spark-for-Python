# coding: utf-8
from pyspark.sql.functions import broadcast
from spark_sql.spark_sql_base import SparkSql


class SparkPaper(object):
    """
    DEMO:
    s_paper = SparkPaper()
    paper_id = "0009f836-bd30-11e7-97d7-005056b23776"
    paper_info = s_paper.get_paper_info(paper_id=paper_id)
    print(paper_info.toJSON().collect())
    """
    def __init__(self):
        self.spark_sql = SparkSql()

    def get_paper_info(self, paper_id=None):
        # if not paper_id:
        #     raise ResourceError('缺少paper_id')

        # 读取表的dataframe
        sub_q_df = self.spark_sql.load_table_dataframe('paper_subtype_question')
        q_map_df = self.spark_sql.load_table_dataframe('question_cognition_map')

        df = broadcast(sub_q_df).filter(
            sub_q_df.paper_id == paper_id
        ).join(
            q_map_df, on=[
                sub_q_df.question_id == q_map_df.question_id
            ], how='left'
        ).select(q_map_df.cognition_map_num)

        # 统计排序
        res_df = df.groupBy(
            "cognition_map_num"
        ).count().sort('count', ascending=False)

        return res_df